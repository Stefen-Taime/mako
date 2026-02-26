// Package v1 defines the Mako Pipeline Specification.
//
// This is the declarative YAML DSL that users write to define
// real-time data pipelines.
//
// Example:
//
//	pipeline:
//	  name: order-events
//	  source:
//	    type: kafka
//	    topic: events.orders
//	  transforms:
//	    - name: pii_mask
//	      type: hash_fields
//	      fields: [email, phone]
//	  sink:
//	    type: snowflake
//	    database: ANALYTICS
package v1

import "time"

// PipelineSpec is the top-level specification for a Mako pipeline.
// One YAML file = one pipeline = one isolated processing unit.
// Each event type gets its own worker, so an anomaly on one event
// doesn't block all others.
type PipelineSpec struct {
	APIVersion string   `yaml:"apiVersion" json:"apiVersion"` // mako/v1
	Kind       string   `yaml:"kind" json:"kind"`             // Pipeline
	Pipeline   Pipeline `yaml:"pipeline" json:"pipeline"`
}

// Pipeline defines the complete data pipeline configuration.
type Pipeline struct {
	Name        string            `yaml:"name" json:"name"`
	Description string            `yaml:"description,omitempty" json:"description,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Owner       string            `yaml:"owner,omitempty" json:"owner,omitempty"`
	Source      Source            `yaml:"source" json:"source"`
	Transforms  []Transform       `yaml:"transforms,omitempty" json:"transforms,omitempty"`
	Sink        Sink              `yaml:"sink" json:"sink"`
	Sinks       []Sink            `yaml:"sinks,omitempty" json:"sinks,omitempty"` // Multi-sink support
	Schema      *SchemaSpec       `yaml:"schema,omitempty" json:"schema,omitempty"`
	Isolation   IsolationSpec     `yaml:"isolation,omitempty" json:"isolation,omitempty"`
	Monitoring  *MonitoringSpec   `yaml:"monitoring,omitempty" json:"monitoring,omitempty"`
	Resources   *ResourceSpec     `yaml:"resources,omitempty" json:"resources,omitempty"`
}

// ═══════════════════════════════════════════
// Source — where events come from
// ═══════════════════════════════════════════

// Source defines where the pipeline reads events from.
// Kafka is the default event bus. Every event flows through
// Kafka before any processing.
type Source struct {
	Type   SourceType        `yaml:"type" json:"type"`
	Config map[string]any    `yaml:"config,omitempty" json:"config,omitempty"`
	Labels map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`

	// Kafka-specific (most common)
	Topic         string `yaml:"topic,omitempty" json:"topic,omitempty"`
	ConsumerGroup string `yaml:"consumerGroup,omitempty" json:"consumerGroup,omitempty"`
	Brokers       string `yaml:"brokers,omitempty" json:"brokers,omitempty"`
	StartOffset   string `yaml:"startOffset,omitempty" json:"startOffset,omitempty"` // earliest|latest

	// Schema enforcement (protobuf, avro, json)
	Schema string `yaml:"schema,omitempty" json:"schema,omitempty"` // protobuf://path, avro://path, json://path
}

type SourceType string

const (
	SourceKafka    SourceType = "kafka"
	SourceHTTP     SourceType = "http"
	SourceFile     SourceType = "file"
	SourcePostgres SourceType = "postgres_cdc"
)

// ═══════════════════════════════════════════
// Transform — processing steps
// ═══════════════════════════════════════════

// Transform defines a single processing step in the pipeline.
// Declarative YAML DSL for data transformation, reducing
// iteration cycles from weeks to hours.
type Transform struct {
	Name   string        `yaml:"name" json:"name"`
	Type   TransformType `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config,omitempty" json:"config,omitempty"`

	// Type-specific fields (sugar for common transforms)
	Fields    []string `yaml:"fields,omitempty" json:"fields,omitempty"`       // hash_fields, drop_fields, rename_fields
	Query     string   `yaml:"query,omitempty" json:"query,omitempty"`         // sql transform
	Condition string   `yaml:"condition,omitempty" json:"condition,omitempty"` // filter
	Mapping   map[string]string `yaml:"mapping,omitempty" json:"mapping,omitempty"` // rename, cast
	Window    *WindowSpec       `yaml:"window,omitempty" json:"window,omitempty"`   // aggregate
}

type TransformType string

const (
	// Data quality / governance
	TransformHashFields TransformType = "hash_fields"   // PII hashing (SHA-256 + salt)
	TransformMaskFields TransformType = "mask_fields"    // Partial masking (****1234)
	TransformDropFields TransformType = "drop_fields"    // Remove columns
	TransformFilter     TransformType = "filter"         // Row-level filter (SQL WHERE)

	// Enrichment
	TransformSQL        TransformType = "sql"            // Arbitrary SQL transform
	TransformLookup     TransformType = "lookup"         // Join with reference table
	TransformRename     TransformType = "rename_fields"  // Rename columns
	TransformCast       TransformType = "cast_fields"    // Type casting
	TransformFlatten    TransformType = "flatten"        // Flatten nested structs
	TransformDefault    TransformType = "default_values" // Fill nulls

	// Aggregation
	TransformAggregate  TransformType = "aggregate"      // Window aggregations
	TransformDedupe     TransformType = "deduplicate"    // Deduplication by key

	// Custom
	TransformPlugin     TransformType = "plugin"         // User-supplied WASM/Go plugin
)

// WindowSpec defines windowing for aggregations.
type WindowSpec struct {
	Type     string `yaml:"type" json:"type"`         // tumbling|sliding|session
	Size     string `yaml:"size" json:"size"`         // 5m, 1h, 1d
	Slide    string `yaml:"slide,omitempty" json:"slide,omitempty"` // for sliding windows
	GroupBy  []string `yaml:"groupBy,omitempty" json:"groupBy,omitempty"`
	Function string `yaml:"function" json:"function"` // count|sum|avg|min|max|percentile
	Field    string `yaml:"field" json:"field"`
	Output   string `yaml:"output" json:"output"`     // output field name
}

// ═══════════════════════════════════════════
// Sink — where processed events go
// ═══════════════════════════════════════════

// Sink defines the destination for processed events.
// Mako supports multiple sinks per pipeline.
type Sink struct {
	Name   string            `yaml:"name,omitempty" json:"name,omitempty"`
	Type   SinkType          `yaml:"type" json:"type"`
	Config map[string]any    `yaml:"config,omitempty" json:"config,omitempty"`
	Labels map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`

	// Warehouse-specific
	Database string `yaml:"database,omitempty" json:"database,omitempty"`
	Schema   string `yaml:"schema,omitempty" json:"schema,omitempty"`
	Table    string `yaml:"table,omitempty" json:"table,omitempty"`

	// Kafka sink (for event routing)
	Topic string `yaml:"topic,omitempty" json:"topic,omitempty"`

	// Object storage
	Bucket string `yaml:"bucket,omitempty" json:"bucket,omitempty"`
	Prefix string `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	Format string `yaml:"format,omitempty" json:"format,omitempty"` // parquet|json|avro

	// Batching
	Batch *BatchSpec `yaml:"batch,omitempty" json:"batch,omitempty"`
}

type SinkType string

const (
	SinkSnowflake  SinkType = "snowflake"
	SinkBigQuery   SinkType = "bigquery"
	SinkPostgres   SinkType = "postgres"
	SinkKafka      SinkType = "kafka"
	SinkS3         SinkType = "s3"
	SinkGCS        SinkType = "gcs"
	SinkClickHouse SinkType = "clickhouse"
	SinkStdout     SinkType = "stdout" // For debugging
)

type BatchSpec struct {
	Size     int    `yaml:"size,omitempty" json:"size,omitempty"`         // records per batch
	Interval string `yaml:"interval,omitempty" json:"interval,omitempty"` // flush interval
	MaxBytes int    `yaml:"maxBytes,omitempty" json:"maxBytes,omitempty"`
}

// ═══════════════════════════════════════════
// Schema enforcement
// ═══════════════════════════════════════════

// SchemaSpec defines schema enforcement rules.
type SchemaSpec struct {
	Registry      string `yaml:"registry,omitempty" json:"registry,omitempty"`
	Subject       string `yaml:"subject,omitempty" json:"subject,omitempty"`
	Compatibility string `yaml:"compatibility,omitempty" json:"compatibility,omitempty"` // BACKWARD|FORWARD|FULL
	Enforce       bool   `yaml:"enforce,omitempty" json:"enforce,omitempty"`
	OnFailure     string `yaml:"onFailure,omitempty" json:"onFailure,omitempty"` // reject|dlq|log
	DLQTopic      string `yaml:"dlqTopic,omitempty" json:"dlqTopic,omitempty"`
}

// ═══════════════════════════════════════════
// Isolation
// ═══════════════════════════════════════════

// IsolationSpec controls fault isolation.
// Each pipeline runs independently for maximum resilience.
type IsolationSpec struct {
	Strategy string `yaml:"strategy,omitempty" json:"strategy,omitempty"` // per_event_type|shared|dedicated
	MaxRetries int  `yaml:"maxRetries,omitempty" json:"maxRetries,omitempty"`
	BackoffMs  int  `yaml:"backoffMs,omitempty" json:"backoffMs,omitempty"`
	DLQEnabled bool `yaml:"dlqEnabled,omitempty" json:"dlqEnabled,omitempty"`
}

// ═══════════════════════════════════════════
// Monitoring
// ═══════════════════════════════════════════

// MonitoringSpec defines observability for the pipeline.
type MonitoringSpec struct {
	FreshnessSLA  string            `yaml:"freshnessSLA,omitempty" json:"freshnessSLA,omitempty"` // 5m, 1h
	AlertChannel  string            `yaml:"alertChannel,omitempty" json:"alertChannel,omitempty"` // Slack channel
	Metrics       *MetricsSpec      `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	Alerts        []AlertSpec       `yaml:"alerts,omitempty" json:"alerts,omitempty"`
	Labels        map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
}

type MetricsSpec struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Endpoint string `yaml:"endpoint,omitempty" json:"endpoint,omitempty"` // Prometheus endpoint
	Port     int    `yaml:"port,omitempty" json:"port,omitempty"`
}

type AlertSpec struct {
	Name      string `yaml:"name" json:"name"`
	Type      string `yaml:"type" json:"type"`     // freshness|volume|error_rate|latency
	Threshold string `yaml:"threshold" json:"threshold"`
	Severity  string `yaml:"severity" json:"severity"` // info|warning|critical
	Channel   string `yaml:"channel,omitempty" json:"channel,omitempty"`
}

// ═══════════════════════════════════════════
// Resources
// ═══════════════════════════════════════════

// ResourceSpec defines compute resources for the pipeline.
type ResourceSpec struct {
	Replicas  int    `yaml:"replicas,omitempty" json:"replicas,omitempty"`
	CPU       string `yaml:"cpu,omitempty" json:"cpu,omitempty"`
	Memory    string `yaml:"memory,omitempty" json:"memory,omitempty"`
	Autoscale *AutoscaleSpec `yaml:"autoscale,omitempty" json:"autoscale,omitempty"`
}

type AutoscaleSpec struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	MinReplicas int    `yaml:"minReplicas,omitempty" json:"minReplicas,omitempty"`
	MaxReplicas int    `yaml:"maxReplicas,omitempty" json:"maxReplicas,omitempty"`
	TargetLag   string `yaml:"targetLag,omitempty" json:"targetLag,omitempty"` // consumer lag threshold
}

// ═══════════════════════════════════════════
// Status (runtime, not user-defined)
// ═══════════════════════════════════════════

type PipelineStatus struct {
	Name         string         `json:"name"`
	State        PipelineState  `json:"state"`
	Source       SourceStatus   `json:"source"`
	Sink         SinkStatus     `json:"sink"`
	Throughput   ThroughputInfo `json:"throughput"`
	LastEvent    *time.Time     `json:"lastEvent,omitempty"`
	StartedAt    *time.Time     `json:"startedAt,omitempty"`
	Errors       int64          `json:"errors"`
	EventsIn     int64          `json:"eventsIn"`
	EventsOut    int64          `json:"eventsOut"`
}

type PipelineState string

const (
	StateRunning  PipelineState = "running"
	StateStopped  PipelineState = "stopped"
	StateFailed   PipelineState = "failed"
	StateDegraded PipelineState = "degraded"
	StateStarting PipelineState = "starting"
)

type SourceStatus struct {
	Connected    bool  `json:"connected"`
	Lag          int64 `json:"lag"`
	Partitions   int   `json:"partitions"`
}

type SinkStatus struct {
	Connected    bool   `json:"connected"`
	LastFlush    *time.Time `json:"lastFlush,omitempty"`
	PendingRows  int64  `json:"pendingRows"`
}

type ThroughputInfo struct {
	EventsPerSec float64 `json:"eventsPerSec"`
	BytesPerSec  float64 `json:"bytesPerSec"`
}
