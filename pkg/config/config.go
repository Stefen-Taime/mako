// Package config handles loading, parsing, and validating
// Mako pipeline YAML specifications.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"gopkg.in/yaml.v3"
)

// Load reads and parses a pipeline YAML file.
func Load(path string) (*v1.PipelineSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return Parse(data)
}

// Parse parses raw YAML bytes into a PipelineSpec.
func Parse(data []byte) (*v1.PipelineSpec, error) {
	var spec v1.PipelineSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	// Set defaults
	applyDefaults(&spec)

	return &spec, nil
}

// SpecResult pairs a parsed spec with its file path.
type SpecResult struct {
	Spec *v1.PipelineSpec
	Path string
}

// LoadAll loads all pipeline YAML files from a directory.
func LoadAll(dir string) ([]*v1.PipelineSpec, error) {
	var specs []*v1.PipelineSpec

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := filepath.Ext(e.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		spec, err := Load(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", e.Name(), err)
		}
		specs = append(specs, spec)
	}

	return specs, nil
}

// ═══════════════════════════════════════════
// Validation
// ═══════════════════════════════════════════

// ValidationError represents a single validation issue.
type ValidationError struct {
	Field   string
	Message string
	Severity string // error|warning
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("[%s] %s: %s", e.Severity, e.Field, e.Message)
}

// ValidationResult contains all validation issues.
type ValidationResult struct {
	Errors   []ValidationError
	Warnings []ValidationError
}

func (r *ValidationResult) IsValid() bool {
	return len(r.Errors) == 0
}

func (r *ValidationResult) addError(field, msg string) {
	r.Errors = append(r.Errors, ValidationError{Field: field, Message: msg, Severity: "error"})
}

func (r *ValidationResult) addWarning(field, msg string) {
	r.Warnings = append(r.Warnings, ValidationError{Field: field, Message: msg, Severity: "warning"})
}

// Validate checks a PipelineSpec for correctness.
func Validate(spec *v1.PipelineSpec) *ValidationResult {
	result := &ValidationResult{}

	// API version
	if spec.APIVersion != "" && spec.APIVersion != "mako/v1" {
		result.addError("apiVersion", fmt.Sprintf("unsupported version %q, expected mako/v1", spec.APIVersion))
	}

	// Pipeline name
	p := spec.Pipeline
	if p.Name == "" {
		result.addError("pipeline.name", "required")
	} else if !isValidName(p.Name) {
		result.addError("pipeline.name", "must be lowercase alphanumeric with hyphens (e.g., order-events)")
	}

	// Source
	validateSource(result, &p.Source)

	// Transforms
	for i, t := range p.Transforms {
		validateTransform(result, &t, i)
	}

	// Sink(s)
	if p.Sink.Type == "" && len(p.Sinks) == 0 {
		result.addError("pipeline.sink", "at least one sink is required")
	}
	if p.Sink.Type != "" {
		validateSink(result, &p.Sink, "pipeline.sink")
	}
	for i, s := range p.Sinks {
		validateSink(result, &s, fmt.Sprintf("pipeline.sinks[%d]", i))
	}

	// Schema
	if p.Schema != nil {
		validateSchema(result, p.Schema)
	}

	// Monitoring
	if p.Monitoring != nil {
		validateMonitoring(result, p.Monitoring)
	}

	// Warnings for best practices
	if p.Owner == "" {
		result.addWarning("pipeline.owner", "recommended for governance (who owns this pipeline?)")
	}
	if p.Monitoring == nil {
		result.addWarning("pipeline.monitoring", "recommended: add freshnessSLA and alertChannel")
	}
	if p.Schema == nil {
		result.addWarning("pipeline.schema", "recommended: enable schema enforcement")
	}

	return result
}

func validateSource(r *ValidationResult, s *v1.Source) {
	validTypes := map[v1.SourceType]bool{
		v1.SourceKafka: true, v1.SourceHTTP: true,
		v1.SourceFile: true, v1.SourcePostgres: true,
		v1.SourceDuckDB: true,
	}

	if s.Type == "" {
		r.addError("source.type", "required")
	} else if !validTypes[s.Type] {
		r.addError("source.type", fmt.Sprintf("unsupported type %q", s.Type))
	}

	if s.Type == v1.SourceKafka && s.Topic == "" {
		r.addError("source.topic", "required for kafka source")
	}
}

func validateTransform(r *ValidationResult, t *v1.Transform, idx int) {
	prefix := fmt.Sprintf("transforms[%d]", idx)

	if t.Name == "" {
		r.addError(prefix+".name", "required")
	}
	if t.Type == "" {
		r.addError(prefix+".type", "required")
	}

	// Type-specific validation
	switch t.Type {
	case v1.TransformHashFields, v1.TransformMaskFields, v1.TransformDropFields:
		if len(t.Fields) == 0 {
			r.addError(prefix+".fields", "required for "+string(t.Type))
		}
	case v1.TransformSQL:
		if t.Query == "" {
			r.addError(prefix+".query", "required for sql transform")
		}
	case v1.TransformFilter:
		if t.Condition == "" {
			r.addError(prefix+".condition", "required for filter transform")
		}
	case v1.TransformRename:
		if len(t.Mapping) == 0 {
			r.addError(prefix+".mapping", "required for rename_fields transform")
		}
	case v1.TransformAggregate:
		if t.Window == nil {
			r.addError(prefix+".window", "required for aggregate transform")
		} else {
			validateWindow(r, t.Window, prefix+".window")
		}
	}
}

func validateWindow(r *ValidationResult, w *v1.WindowSpec, prefix string) {
	validTypes := map[string]bool{"tumbling": true, "sliding": true, "session": true}
	if !validTypes[w.Type] {
		r.addError(prefix+".type", fmt.Sprintf("must be tumbling|sliding|session, got %q", w.Type))
	}
	if w.Size == "" {
		r.addError(prefix+".size", "required")
	} else if _, err := ParseDuration(w.Size); err != nil {
		r.addError(prefix+".size", fmt.Sprintf("invalid duration: %s", err))
	}
	if w.Type == "sliding" && w.Slide == "" {
		r.addError(prefix+".slide", "required for sliding window")
	}
	if w.Function == "" {
		r.addError(prefix+".function", "required")
	}
}

func validateSink(r *ValidationResult, s *v1.Sink, prefix string) {
	validTypes := map[v1.SinkType]bool{
		v1.SinkSnowflake: true, v1.SinkBigQuery: true, v1.SinkPostgres: true,
		v1.SinkKafka: true, v1.SinkS3: true, v1.SinkGCS: true,
		v1.SinkClickHouse: true, v1.SinkDuckDB: true, v1.SinkStdout: true,
	}

	if !validTypes[s.Type] {
		r.addError(prefix+".type", fmt.Sprintf("unsupported type %q", s.Type))
	}

	switch s.Type {
	case v1.SinkSnowflake, v1.SinkBigQuery, v1.SinkPostgres, v1.SinkClickHouse, v1.SinkDuckDB:
		if s.Table == "" {
			r.addError(prefix+".table", "required for warehouse sink")
		}
	case v1.SinkKafka:
		if s.Topic == "" {
			r.addError(prefix+".topic", "required for kafka sink")
		}
	case v1.SinkS3, v1.SinkGCS:
		if s.Bucket == "" {
			r.addError(prefix+".bucket", "required for object storage sink")
		}
	}
}

func validateSchema(r *ValidationResult, s *v1.SchemaSpec) {
	if s.Enforce && s.Registry == "" {
		r.addError("schema.registry", "required when enforce is true")
	}
	validCompat := map[string]bool{
		"": true, "BACKWARD": true, "FORWARD": true, "FULL": true, "NONE": true,
	}
	if !validCompat[s.Compatibility] {
		r.addError("schema.compatibility", "must be BACKWARD|FORWARD|FULL|NONE")
	}
}

func validateMonitoring(r *ValidationResult, m *v1.MonitoringSpec) {
	if m.FreshnessSLA != "" {
		if _, err := ParseDuration(m.FreshnessSLA); err != nil {
			r.addError("monitoring.freshnessSLA", fmt.Sprintf("invalid duration: %s", err))
		}
	}

	// Slack alerting validation
	webhookURL := os.ExpandEnv(m.SlackWebhookURL)
	if webhookURL == "" {
		webhookURL = os.Getenv("SLACK_WEBHOOK_URL")
	}

	if m.AlertChannel != "" && webhookURL == "" {
		r.addWarning("monitoring.slackWebhookURL",
			"alertChannel is set but no webhook URL configured (set slackWebhookURL or SLACK_WEBHOOK_URL)")
	}

	if m.SlackWebhookURL != "" {
		resolved := os.ExpandEnv(m.SlackWebhookURL)
		if resolved != "" && !strings.HasPrefix(resolved, "https://") {
			r.addError("monitoring.slackWebhookURL",
				"must start with https:// (e.g., https://hooks.slack.com/services/...)")
		}
	}
}

// ═══════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════

func applyDefaults(spec *v1.PipelineSpec) {
	if spec.APIVersion == "" {
		spec.APIVersion = "mako/v1"
	}
	if spec.Kind == "" {
		spec.Kind = "Pipeline"
	}

	p := &spec.Pipeline

	// Source defaults
	if p.Source.Type == v1.SourceKafka {
		if p.Source.ConsumerGroup == "" {
			p.Source.ConsumerGroup = "mako-" + p.Name
		}
		if p.Source.StartOffset == "" {
			p.Source.StartOffset = "latest"
		}
	}

	// Isolation defaults
	if p.Isolation.Strategy == "" {
		p.Isolation.Strategy = "per_event_type"
	}
	if p.Isolation.MaxRetries == 0 {
		p.Isolation.MaxRetries = 3
	}
	if p.Isolation.BackoffMs == 0 {
		p.Isolation.BackoffMs = 1000
	}

	// Sink batch defaults
	allSinks := append([]v1.Sink{p.Sink}, p.Sinks...)
	for i := range allSinks {
		if allSinks[i].Batch == nil {
			allSinks[i].Batch = &v1.BatchSpec{
				Size:     1000,
				Interval: "10s",
			}
		}
	}
}

func isValidName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return name[0] != '-' && name[len(name)-1] != '-'
}

// ParseDuration parses durations like "5m", "1h", "30s", "1d".
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		var days int
		if _, err := fmt.Sscanf(s, "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid days: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
