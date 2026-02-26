package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

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
			if len(params) > 0 {
				dsn += "?" + strings.Join(params, "&")
			}
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

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("snowflake ping: %w", err)
	}

	// Set warehouse context if specified
	if s.warehouse != "" {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("USE WAREHOUSE %s", s.warehouse)); err != nil {
			// Non-fatal: warehouse might be set in the DSN
			fmt.Fprintf(os.Stderr, "[snowflake] warning: USE WAREHOUSE failed: %v\n", err)
		}
	}

	s.db = db
	return nil
}

// Write inserts events into Snowflake using batch INSERT with PARSE_JSON.
func (s *SnowflakeSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	qualifiedTable := fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)

	// Build multi-row INSERT
	// INSERT INTO db.schema.table (event_data, event_key, event_topic, event_offset)
	// SELECT PARSE_JSON($1), $2, $3, $4
	// UNION ALL SELECT PARSE_JSON($5), $6, $7, $8 ...
	var sb strings.Builder
	args := make([]any, 0, len(events)*4)

	sb.WriteString(fmt.Sprintf("INSERT INTO %s (event_data, event_key, event_topic, event_offset) ", qualifiedTable))

	for i, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		argBase := i * 4
		if i > 0 {
			sb.WriteString(" UNION ALL ")
		}
		sb.WriteString(fmt.Sprintf("SELECT PARSE_JSON($%d), $%d, $%d, $%d",
			argBase+1, argBase+2, argBase+3, argBase+4))

		args = append(args, string(data), string(event.Key), event.Topic, event.Offset)
	}

	_, err := s.db.ExecContext(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("snowflake insert: %w", err)
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
