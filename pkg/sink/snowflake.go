package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Stefen-Taime/mako/pkg/pipeline"

	// Snowflake driver registers itself as "snowflake" for database/sql.
	_ "github.com/snowflakedb/gosnowflake"
)

// ═══════════════════════════════════════════
// Snowflake Sink (gosnowflake)
// ═══════════════════════════════════════════

// SnowflakeSink writes events to Snowflake using the gosnowflake driver.
// Events are stored as VARIANT (JSON) in the target table.
//
// Connection is configured via:
//   - config.dsn  (full DSN string)
//   - SNOWFLAKE_DSN env var
//   - Individual fields: account, user, password, warehouse, role
//
// Target table should have at minimum a VARIANT column for event data.
// Recommended schema:
//
//	CREATE TABLE <db>.<schema>.<table> (
//	    event_data    VARIANT NOT NULL,
//	    event_key     VARCHAR,
//	    event_topic   VARCHAR,
//	    event_offset  NUMBER,
//	    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
//	);
type SnowflakeSink struct {
	database  string
	schema    string
	table     string
	warehouse string
	config    map[string]any
	dsn       string
	db        *sql.DB
	mu        sync.Mutex
}

// NewSnowflakeSink creates a Snowflake sink.
func NewSnowflakeSink(database, schema, table string, config map[string]any) *SnowflakeSink {
	dsn := ""
	warehouse := ""

	if config != nil {
		if d, ok := config["dsn"].(string); ok {
			dsn = d
		}
		if w, ok := config["warehouse"].(string); ok {
			warehouse = w
		}
	}

	if dsn == "" {
		dsn = os.Getenv("SNOWFLAKE_DSN")
	}

	// Build DSN from individual config/env vars if not provided
	if dsn == "" {
		account := sfEnvOrConfig(config, "account", "SNOWFLAKE_ACCOUNT", "")
		user := sfEnvOrConfig(config, "user", "SNOWFLAKE_USER", "")
		password := sfEnvOrConfig(config, "password", "SNOWFLAKE_PASSWORD", "")
		if warehouse == "" {
			warehouse = sfEnvOrConfig(config, "warehouse", "SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
		}
		role := sfEnvOrConfig(config, "role", "SNOWFLAKE_ROLE", "")

		if account != "" && user != "" {
			dsn = fmt.Sprintf("%s:%s@%s/%s/%s",
				user, password, account, database, schema)

			params := []string{}
			if warehouse != "" {
				params = append(params, "warehouse="+warehouse)
			}
			if role != "" {
				params = append(params, "role="+role)
			}
			// gosnowflake driver-level timeouts — without these the driver
			// uses very short defaults that cause context deadline exceeded
			// even when the Go context has plenty of time left.
			params = append(params, "loginTimeout=30")
			params = append(params, "requestTimeout=60")
			params = append(params, "clientTimeout=300")

			dsn += "?" + strings.Join(params, "&")
		}
	}

	return &SnowflakeSink{
		database:  database,
		schema:    schema,
		table:     table,
		warehouse: warehouse,
		config:    config,
		dsn:       dsn,
	}
}

func sfEnvOrConfig(cfg map[string]any, key, envKey, defaultVal string) string {
	if cfg != nil {
		if v, ok := cfg[key].(string); ok && v != "" {
			return v
		}
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return defaultVal
}

// Open connects to Snowflake.
func (s *SnowflakeSink) Open(ctx context.Context) error {
	if s.dsn == "" {
		return fmt.Errorf("snowflake: no DSN configured (set SNOWFLAKE_DSN or provide account/user/password in config)")
	}

	db, err := sql.Open("snowflake", s.dsn)
	if err != nil {
		return fmt.Errorf("snowflake open: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	// Use context.Background() so the DB connection pool is not tied to the
	// pipeline context. When the pipeline ctx is cancelled (e.g. file source
	// EOF + auto-terminate), a ctx-derived context would invalidate the
	// pool and cause Write() to fail with context deadline exceeded.
	// Graceful shutdown is handled by Close() calling db.Close().
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()

	if err := db.PingContext(initCtx); err != nil {
		db.Close()
		return fmt.Errorf("snowflake ping: %w", err)
	}

	// NOTE: warehouse is set via the DSN query parameter (?warehouse=X).
	// A redundant USE WAREHOUSE command here can corrupt the driver's
	// internal connection state and cause subsequent queries to fail
	// with context deadline exceeded.

	// Auto-create the target table if it does not exist.
	qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)
	createDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		event_data    VARIANT NOT NULL,
		event_key     VARCHAR,
		event_topic   VARCHAR,
		event_offset  NUMBER,
		loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
	)`, qualifiedTable)
	if _, err := db.ExecContext(initCtx, createDDL); err != nil {
		db.Close()
		return fmt.Errorf("snowflake create table: %w", err)
	}

	fmt.Fprintf(os.Stderr, "[snowflake] connected to %s.%s.%s\n", s.database, s.schema, s.table)
	s.db = db
	return nil
}

// Write inserts events into Snowflake using batch INSERT with PARSE_JSON.
// Snowflake uses ? as parameter placeholder (not $N like PostgreSQL).
// Large batches are split into chunks of 100 to avoid driver timeouts on
// long-running transactions.
func (s *SnowflakeSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)
	query := fmt.Sprintf(
		"INSERT INTO %s (event_data, event_key, event_topic, event_offset) SELECT PARSE_JSON(?), ?, ?, ?",
		qualifiedTable,
	)

	// Split into chunks to avoid driver timeout on large batches.
	const chunkSize = 100
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		if err := s.writeChunk(query, events[i:end]); err != nil {
			return err
		}
	}

	return nil
}

// writeChunk inserts a single chunk of events inside its own transaction.
func (s *SnowflakeSink) writeChunk(query string, events []*pipeline.Event) error {
	// Use context.Background() so writes are not tied to the pipeline context.
	// The pipeline ctx may already be cancelled (file source EOF + auto-terminate)
	// by the time the final flush happens, which would corrupt the connection pool.
	writeCtx, writeCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer writeCancel()

	tx, err := s.db.BeginTx(writeCtx, nil)
	if err != nil {
		return fmt.Errorf("snowflake begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(writeCtx, query)
	if err != nil {
		return fmt.Errorf("snowflake prepare: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		_, err = stmt.ExecContext(writeCtx, string(data), string(event.Key), event.Topic, event.Offset)
		if err != nil {
			return fmt.Errorf("snowflake insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("snowflake commit: %w", err)
	}

	return nil
}

// Flush is a no-op (writes are synchronous).
func (s *SnowflakeSink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the database connection.
func (s *SnowflakeSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *SnowflakeSink) Name() string {
	return fmt.Sprintf("snowflake:%s.%s.%s", s.database, s.schema, s.table)
}
