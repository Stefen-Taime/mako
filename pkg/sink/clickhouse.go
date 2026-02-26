package sink

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// ClickHouse Sink (clickhouse-go v2)
// ═══════════════════════════════════════════

// ClickHouseSink writes events to ClickHouse using the official native protocol driver.
// Events are inserted as JSON strings into the target table using batch inserts.
//
// YAML example:
//
//	sink:
//	  type: clickhouse
//	  database: analytics
//	  table: pipeline_events
//	  config:
//	    host: localhost
//	    port: "9000"
//	    user: default
//	    password: ""
//	    # Or use a full DSN:
//	    # dsn: clickhouse://default:@localhost:9000/analytics
//
// Target table schema (create before running):
//
//	CREATE TABLE analytics.pipeline_events (
//	    event_date    Date DEFAULT toDate(event_ts),
//	    event_ts      DateTime64(3) DEFAULT now64(),
//	    event_key     String,
//	    event_data    String,    -- JSON string
//	    topic         String,
//	    partition_id  UInt32,
//	    offset_id     UInt64
//	) ENGINE = MergeTree()
//	ORDER BY (event_date, event_ts)
//	PARTITION BY toYYYYMM(event_date);
type ClickHouseSink struct {
	dsn      string
	database string
	table    string
	config   map[string]any
	conn     clickhouse.Conn
	mu       sync.Mutex
}

// NewClickHouseSink creates a ClickHouse sink.
// The DSN can be provided via config["dsn"], the CLICKHOUSE_DSN env var,
// or constructed from individual fields (host, port, user, password).
func NewClickHouseSink(database, table string, cfg map[string]any) *ClickHouseSink {
	dsn := ""
	if cfg != nil {
		if d, ok := cfg["dsn"].(string); ok {
			dsn = d
		}
	}
	if dsn == "" {
		dsn = os.Getenv("CLICKHOUSE_DSN")
	}

	return &ClickHouseSink{
		dsn:      dsn,
		database: database,
		table:    table,
		config:   cfg,
	}
}

// Open establishes the connection to ClickHouse.
func (s *ClickHouseSink) Open(ctx context.Context) error {
	var opts *clickhouse.Options
	var err error

	if s.dsn != "" {
		opts, err = clickhouse.ParseDSN(s.dsn)
		if err != nil {
			return fmt.Errorf("clickhouse parse dsn: %w", err)
		}
	} else {
		// Build options from individual config / env vars
		host := envOrConfig(s.config, "host", "CLICKHOUSE_HOST", "localhost")
		port := envOrConfig(s.config, "port", "CLICKHOUSE_PORT", "9000")
		user := envOrConfig(s.config, "user", "CLICKHOUSE_USER", "default")
		pass := envOrConfig(s.config, "password", "CLICKHOUSE_PASSWORD", "")
		db := s.database
		if db == "" {
			db = envOrConfig(s.config, "database", "CLICKHOUSE_DB", "default")
		}

		secure := envOrConfig(s.config, "secure", "CLICKHOUSE_SECURE", "false")
		isSecure, _ := strconv.ParseBool(secure)

		addr := fmt.Sprintf("%s:%s", host, port)

		opts = &clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: db,
				Username: user,
				Password: pass,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 10 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
		}

		if isSecure {
			opts.TLS = &tls.Config{
				InsecureSkipVerify: false,
			}
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}

	// Verify connectivity
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	s.conn = conn
	return nil
}

// Write inserts events into ClickHouse using a batch insert.
// Each event is stored as a JSON string with associated metadata.
func (s *ClickHouseSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	qualifiedTable := s.table
	if s.database != "" {
		qualifiedTable = s.database + "." + s.table
	}

	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO %s (event_ts, event_key, event_data, topic, partition_id, offset_id)",
		qualifiedTable,
	))
	if err != nil {
		return fmt.Errorf("clickhouse prepare batch: %w", err)
	}

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("clickhouse marshal event: %w", err)
		}

		ts := event.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}

		if err := batch.Append(
			ts,
			string(event.Key),
			string(data),
			event.Topic,
			uint32(event.Partition),
			uint64(event.Offset),
		); err != nil {
			return fmt.Errorf("clickhouse append: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse send batch: %w", err)
	}

	return nil
}

// Flush is a no-op for ClickHouse (batch writes are synchronous).
func (s *ClickHouseSink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the ClickHouse connection.
func (s *ClickHouseSink) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *ClickHouseSink) Name() string {
	return fmt.Sprintf("clickhouse:%s.%s", s.database, s.table)
}
