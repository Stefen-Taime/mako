package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
)

// ═══════════════════════════════════════════
// BigQuery Sink (cloud.google.com/go/bigquery)
// ═══════════════════════════════════════════

// BigQuerySink writes events to BigQuery using the Go client library.
// Events are inserted as JSON rows using the streaming inserter.
//
// Authentication: uses Application Default Credentials (ADC).
// Set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`.
//
// Configuration via YAML:
//
//	sink:
//	  type: bigquery
//	  schema: my_dataset
//	  table: my_table
//	  config:
//	    project: my-gcp-project
type BigQuerySink struct {
	project string
	dataset string
	table   string
	config  map[string]any

	client   *bigquery.Client
	inserter *bigquery.Inserter
	mu       sync.Mutex
}

// NewBigQuerySink creates a BigQuery sink.
func NewBigQuerySink(project, dataset, table string, config map[string]any) *BigQuerySink {
	if project == "" && config != nil {
		if p, ok := config["project"].(string); ok {
			project = p
		}
	}
	if project == "" {
		project = os.Getenv("GCP_PROJECT")
	}
	if project == "" {
		project = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}

	return &BigQuerySink{
		project: project,
		dataset: dataset,
		table:   table,
		config:  config,
	}
}

// bqEvent implements bigquery.ValueSaver for streaming inserts.
type bqEvent struct {
	InsertID string
	Data     map[string]any
	Topic    string
	Offset   int64
	LoadedAt time.Time
}

// Save implements bigquery.ValueSaver.
func (e *bqEvent) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = make(map[string]bigquery.Value, len(e.Data)+3)

	// Flatten event data into top-level columns
	for k, v := range e.Data {
		row[k] = v
	}

	// Add metadata columns
	row["_mako_topic"] = e.Topic
	row["_mako_offset"] = e.Offset
	row["_mako_loaded_at"] = e.LoadedAt

	return row, e.InsertID, nil
}

// Open creates the BigQuery client and configures the inserter.
func (s *BigQuerySink) Open(ctx context.Context) error {
	if s.project == "" {
		return fmt.Errorf("bigquery: project not configured (set GCP_PROJECT env or config.project)")
	}
	if s.dataset == "" {
		return fmt.Errorf("bigquery: dataset (schema) not configured")
	}
	if s.table == "" {
		return fmt.Errorf("bigquery: table not configured")
	}

	client, err := bigquery.NewClient(ctx, s.project)
	if err != nil {
		return fmt.Errorf("bigquery client: %w", err)
	}

	s.client = client

	// Configure the inserter
	tableRef := client.Dataset(s.dataset).Table(s.table)
	s.inserter = tableRef.Inserter()

	// Skip invalid rows and allow unknown values for flexibility
	s.inserter.SkipInvalidRows = false
	s.inserter.IgnoreUnknownValues = true

	return nil
}

// Write streams events to BigQuery using the streaming inserter.
func (s *BigQuerySink) Write(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	rows := make([]*bqEvent, 0, len(events))
	for _, event := range events {
		insertID := fmt.Sprintf("%s-%d-%d", event.Topic, event.Partition, event.Offset)

		rows = append(rows, &bqEvent{
			InsertID: insertID,
			Data:     event.Value,
			Topic:    event.Topic,
			Offset:   event.Offset,
			LoadedAt: time.Now(),
		})
	}

	if err := s.inserter.Put(ctx, rows); err != nil {
		// Check for partial insert errors
		if multiErr, ok := err.(bigquery.PutMultiError); ok {
			var errMsgs []string
			for _, rowErr := range multiErr {
				for _, e := range rowErr.Errors {
					errMsgs = append(errMsgs, e.Error())
				}
			}
			return fmt.Errorf("bigquery partial insert: %s", joinMax(errMsgs, 3))
		}
		return fmt.Errorf("bigquery insert: %w", err)
	}

	return nil
}

// joinMax joins up to maxN strings with "; ".
func joinMax(msgs []string, maxN int) string {
	if len(msgs) <= maxN {
		result := ""
		for i, m := range msgs {
			if i > 0 {
				result += "; "
			}
			result += m
		}
		return result
	}
	result := ""
	for i := 0; i < maxN; i++ {
		if i > 0 {
			result += "; "
		}
		result += msgs[i]
	}
	return fmt.Sprintf("%s (and %d more)", result, len(msgs)-maxN)
}

// Flush is a no-op (streaming inserts are immediate).
func (s *BigQuerySink) Flush(ctx context.Context) error {
	return nil
}

// Close shuts down the BigQuery client.
func (s *BigQuerySink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// Name returns the sink identifier.
func (s *BigQuerySink) Name() string {
	return fmt.Sprintf("bigquery:%s.%s.%s", s.project, s.dataset, s.table)
}

// bqEventJSON is an alternative ValueSaver that stores the entire event
// as a JSON string in a single column. Useful for schema-flexible tables.
type bqEventJSON struct {
	InsertID  string
	EventJSON string
	EventKey  string
	Topic     string
	Offset    int64
	LoadedAt  time.Time
}

// Save implements bigquery.ValueSaver for JSON-column tables.
func (e *bqEventJSON) Save() (row map[string]bigquery.Value, insertID string, err error) {
	row = map[string]bigquery.Value{
		"event_data":   e.EventJSON,
		"event_key":    e.EventKey,
		"event_topic":  e.Topic,
		"event_offset": e.Offset,
		"loaded_at":    e.LoadedAt,
	}
	return row, e.InsertID, nil
}

// WriteJSON inserts events as JSON strings (for tables with a STRING event_data column).
func (s *BigQuerySink) WriteJSON(ctx context.Context, events []*pipeline.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	rows := make([]*bqEventJSON, 0, len(events))
	for _, event := range events {
		data, err := json.Marshal(event.Value)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}

		insertID := fmt.Sprintf("%s-%d-%d", event.Topic, event.Partition, event.Offset)
		rows = append(rows, &bqEventJSON{
			InsertID:  insertID,
			EventJSON: string(data),
			EventKey:  string(event.Key),
			Topic:     event.Topic,
			Offset:    event.Offset,
			LoadedAt:  time.Now(),
		})
	}

	if err := s.inserter.Put(ctx, rows); err != nil {
		return fmt.Errorf("bigquery json insert: %w", err)
	}

	return nil
}
