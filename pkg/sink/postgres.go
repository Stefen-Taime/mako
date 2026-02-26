package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════
// PostgreSQL Sink (pgx)
// ═══════════════════════════════════════════

// PostgresSink writes events to PostgreSQL using pgx connection pool.
// Events are inserted as JSONB into the target table.
type PostgresSink struct {
	dsn    string
	schema string
	table  string
	config map[string]any
	pool   *pgxpool.Pool
	buffer []*pipeline.Event
	mu     sync.Mutex
}

// NewPostgresSink creates a PostgreSQL sink.
// The DSN can be provided via config["dsn"], or via the POSTGRES_DSN
// environment variable, or constructed from individual fields.
func NewPostgresSink(database, schema, table string, cfg map[string]any) *PostgresSink {
	dsn := ""
	if cfg != nil {
		if d, ok := cfg["dsn"].(string); ok {
			dsn = d
		}
	}
	if dsn == "" {
		dsn = os.Getenv("POSTGRES_DSN")
	}
	if dsn == "" {
		// Build DSN from individual config or env vars
		host := envOrConfig(cfg, "host", "POSTGRES_HOST", "localhost")
		port := envOrConfig(cfg, "port", "POSTGRES_PORT", "5432")
		user := envOrConfig(cfg, "user", "POSTGRES_USER", "mako")
		pass := envOrConfig(cfg, "password", "POSTGRES_PASSWORD", "mako")
		db := database
		if db == "" {
			db = envOrConfig(cfg, "database", "POSTGRES_DB", "mako")
		}
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			user, pass, host, port, db)
	}

	if schema == "" {
		schema = "public"
	}

	return &PostgresSink{
		dsn:    dsn,
		schema: schema,
		table:  table,
		config: cfg,
	}
}

// envOrConfig returns value from config map, then env var, then default.
func envOrConfig(cfg map[string]any, key, envKey, defaultVal string) string {
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

// Open establishes the connection pool to PostgreSQL.
func (s *PostgresSink) Open(ctx context.Context) error {
	poolCfg, err := pgxpool.ParseConfig(s.dsn)
	if err != nil {
		return fmt.Errorf("postgres parse dsn: %w", err)
	}

	poolCfg.MinConns = 2
	poolCfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres ping: %w", err)
	}

	s.pool = pool
	return nil
}

// Write inserts events into PostgreSQL.
// Each event is stored as a JSONB row with columns:
//   - event_key: the Kafka key (if any)
//   - event_data: the full event as JSONB
//   - event_timestamp: the event timestamp
//   - topic: source topic
//   - partition: source partition
//   - offset: source offset
//
// The table must exist. Use the docker/postgres/init schema or create:
//
//	CREATE TABLE <schema>.<table> (
//	    id BIGSERIAL PRIMARY KEY,
//	    event_key TEXT,
//	    event_data JSONB NOT NULL,
//	    event_timestamp TIMESTAMPTZ DEFAULT NOW(),
//	    topic TEXT,
//	    partition INTEGER,
//	    "offset" BIGINT,
//	    created_at TIMESTAMPTZ DEFAULT NOW()
//	);
func (s *PostgresSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	qualifiedTable := fmt.Sprintf("%s.%s", s.schema, s.table)

	// Use COPY for bulk insert (much faster than individual INSERTs)
	rows := make([][]any, 0, len(events))
	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		rows = append(rows, []any{
			string(event.Key),       // event_key
			string(data),            // event_data (jsonb)
			event.Timestamp,         // event_timestamp
			event.Topic,             // topic
			int(event.Partition),    // partition
			event.Offset,            // offset
		})
	}

	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{s.schema, s.table},
		[]string{"event_key", "event_data", "event_timestamp", "topic", "partition", "offset"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		// Fallback to batch INSERT if COPY fails (e.g., table schema mismatch)
		return s.batchInsert(ctx, qualifiedTable, events)
	}

	return nil
}

// batchInsert is a fallback using INSERT ... VALUES for compatibility.
func (s *PostgresSink) batchInsert(ctx context.Context, qualifiedTable string, events []*pipeline.Event) error {
	batch := &pgx.Batch{}
	query := fmt.Sprintf(
		`INSERT INTO %s (event_key, event_data, event_timestamp, topic, partition, "offset")
		 VALUES ($1, $2::jsonb, $3, $4, $5, $6)`,
		qualifiedTable,
	)

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		batch.Queue(query,
			string(event.Key),
			string(data),
			event.Timestamp,
			event.Topic,
			int(event.Partition),
			event.Offset,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("postgres batch insert: %w", err)
		}
	}

	return nil
}

// Flush is a no-op for PostgreSQL (writes are synchronous).
func (s *PostgresSink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the connection pool.
func (s *PostgresSink) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *PostgresSink) Name() string {
	return fmt.Sprintf("postgres:%s.%s", s.schema, s.table)
}
