// Package sink implements output adapters for Iguazu pipelines.
//
// Each sink implements the pipeline.Sink interface.
// DoorDash: Snowpipe → Snowflake as primary sink.
// Iguazu supports multiple sinks per pipeline.
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// Stdout Sink (for debugging / development)
// ═══════════════════════════════════════════

type StdoutSink struct {
	writer io.Writer
	count  int64
	mu     sync.Mutex
}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{writer: os.Stdout}
}

func (s *StdoutSink) Open(ctx context.Context) error   { return nil }
func (s *StdoutSink) Close() error                      { return nil }
func (s *StdoutSink) Flush(ctx context.Context) error   { return nil }
func (s *StdoutSink) Name() string                      { return "stdout" }

func (s *StdoutSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return err
		}
		fmt.Fprintf(s.writer, "[%s] %s\n", time.Now().Format(time.RFC3339), string(data))
		s.count++
	}
	return nil
}

// ═══════════════════════════════════════════
// File Sink (for local testing)
// ═══════════════════════════════════════════

type FileSink struct {
	path   string
	format string // json|jsonl
	file   *os.File
	mu     sync.Mutex
}

func NewFileSink(path, format string) *FileSink {
	if format == "" {
		format = "jsonl"
	}
	return &FileSink{path: path, format: format}
}

func (s *FileSink) Open(ctx context.Context) error {
	f, err := os.Create(s.path)
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

func (s *FileSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return err
		}
		fmt.Fprintf(s.file, "%s\n", data)
	}
	return nil
}

func (s *FileSink) Flush(ctx context.Context) error {
	if s.file != nil {
		return s.file.Sync()
	}
	return nil
}

func (s *FileSink) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

func (s *FileSink) Name() string { return "file:" + s.path }

// ═══════════════════════════════════════════
// Snowflake Sink (Snowpipe integration)
// ═══════════════════════════════════════════

// SnowflakeSink writes events to Snowflake via Snowpipe or INSERT.
// DoorDash pattern: "Snowpipe for loading into Snowflake, reducing
// latency from 1 day to minutes."
type SnowflakeSink struct {
	database string
	schema   string
	table    string
	config   map[string]any
	buffer   []*pipeline.Event
	mu       sync.Mutex
}

func NewSnowflakeSink(database, schema, table string, config map[string]any) *SnowflakeSink {
	return &SnowflakeSink{
		database: database,
		schema:   schema,
		table:    table,
		config:   config,
	}
}

func (s *SnowflakeSink) Open(ctx context.Context) error {
	// In production: establish Snowflake connection via gosnowflake driver
	// or configure Snowpipe REST API endpoint
	return nil
}

func (s *SnowflakeSink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buffer = append(s.buffer, events...)
	// In production: batch INSERT or stage to S3/GCS then trigger Snowpipe
	return nil
}

func (s *SnowflakeSink) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// In production: execute the actual write
	s.buffer = s.buffer[:0]
	return nil
}

func (s *SnowflakeSink) Close() error { return nil }
func (s *SnowflakeSink) Name() string {
	return fmt.Sprintf("snowflake:%s.%s.%s", s.database, s.schema, s.table)
}

// ═══════════════════════════════════════════
// BigQuery Sink
// ═══════════════════════════════════════════

type BigQuerySink struct {
	project string
	dataset string
	table   string
	config  map[string]any
}

func NewBigQuerySink(project, dataset, table string, config map[string]any) *BigQuerySink {
	return &BigQuerySink{project: project, dataset: dataset, table: table, config: config}
}

func (s *BigQuerySink) Open(ctx context.Context) error              { return nil }
func (s *BigQuerySink) Write(ctx context.Context, events []*pipeline.Event) error { return nil }
func (s *BigQuerySink) Flush(ctx context.Context) error             { return nil }
func (s *BigQuerySink) Close() error                                { return nil }
func (s *BigQuerySink) Name() string {
	return fmt.Sprintf("bigquery:%s.%s.%s", s.project, s.dataset, s.table)
}

// ═══════════════════════════════════════════
// Builder — construct sinks from YAML specs
// ═══════════════════════════════════════════

// BuildFromSpec creates the appropriate sink from a YAML spec.
func BuildFromSpec(spec v1.Sink) (pipeline.Sink, error) {
	switch spec.Type {
	case v1.SinkStdout:
		return NewStdoutSink(), nil
	case v1.SinkSnowflake:
		return NewSnowflakeSink(spec.Database, spec.Schema, spec.Table, spec.Config), nil
	case v1.SinkBigQuery:
		project, _ := spec.Config["project"].(string)
		return NewBigQuerySink(project, spec.Schema, spec.Table, spec.Config), nil
	case v1.SinkKafka:
		brokers, _ := spec.Config["brokers"].(string)
		return NewKafkaSink(brokers, spec.Topic), nil
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", spec.Type)
	}
}

// NewKafkaSink creates a Kafka sink stub.
func NewKafkaSink(brokers, topic string) pipeline.Sink {
	return &kafkaSinkAdapter{brokers: brokers, topic: topic}
}

type kafkaSinkAdapter struct {
	brokers string
	topic   string
}

func (s *kafkaSinkAdapter) Open(ctx context.Context) error              { return nil }
func (s *kafkaSinkAdapter) Write(ctx context.Context, events []*pipeline.Event) error { return nil }
func (s *kafkaSinkAdapter) Flush(ctx context.Context) error             { return nil }
func (s *kafkaSinkAdapter) Close() error                                { return nil }
func (s *kafkaSinkAdapter) Name() string                                { return "kafka:" + s.topic }
