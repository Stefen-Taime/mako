package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/pkg/codegen"
	"github.com/Stefen-Taime/mako/pkg/config"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/sink"
	"github.com/Stefen-Taime/mako/pkg/transform"
)

// ═══════════════════════════════════════════
// Config / Validation Tests
// ═══════════════════════════════════════════

func TestParseSimplePipeline(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: test-events
  source:
    type: kafka
    topic: events.test
  sink:
    type: stdout
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Name != "test-events" {
		t.Errorf("expected name 'test-events', got %q", spec.Pipeline.Name)
	}
	if spec.Pipeline.Source.Type != v1.SourceKafka {
		t.Errorf("expected source type kafka, got %q", spec.Pipeline.Source.Type)
	}
	if spec.Pipeline.Source.ConsumerGroup != "mako-test-events" {
		t.Errorf("expected auto consumer group, got %q", spec.Pipeline.Source.ConsumerGroup)
	}
}

func TestValidateMinimalPipeline(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if !result.IsValid() {
		t.Errorf("minimal pipeline should be valid, got errors: %v", result.Errors)
	}
}

func TestValidateRejectsEmptyName(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if result.IsValid() {
		t.Error("should reject empty pipeline name")
	}
}

func TestValidateRejectsBadName(t *testing.T) {
	cases := []string{"My Pipeline", "UPPER", "has_underscore", "-leading", "trailing-"}
	for _, name := range cases {
		spec := &v1.PipelineSpec{
			Pipeline: v1.Pipeline{
				Name:   name,
				Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
				Sink:   v1.Sink{Type: v1.SinkStdout},
			},
		}
		result := config.Validate(spec)
		if result.IsValid() {
			t.Errorf("should reject name %q", name)
		}
	}
}

func TestValidateAcceptsGoodNames(t *testing.T) {
	cases := []string{"order-events", "payment-v2", "a", "test123"}
	for _, name := range cases {
		spec := &v1.PipelineSpec{
			Pipeline: v1.Pipeline{
				Name:   name,
				Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
				Sink:   v1.Sink{Type: v1.SinkStdout},
			},
		}
		result := config.Validate(spec)
		if !result.IsValid() {
			t.Errorf("should accept name %q, got errors: %v", name, result.Errors)
		}
	}
}

func TestValidateKafkaSourceRequiresTopic(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if result.IsValid() {
		t.Error("kafka source without topic should be invalid")
	}
}

func TestValidateTransformHashRequiresFields(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Transforms: []v1.Transform{
				{Name: "hash", Type: v1.TransformHashFields},
			},
			Sink: v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if result.IsValid() {
		t.Error("hash_fields without fields should be invalid")
	}
}

func TestValidateWarnsOnMissingOwner(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "t"},
			Sink:   v1.Sink{Type: v1.SinkStdout},
		},
	}

	result := config.Validate(spec)
	if len(result.Warnings) == 0 {
		t.Error("should warn about missing owner")
	}
}

func TestLoadExampleSimple(t *testing.T) {
	spec, err := config.Load("examples/simple/pipeline.yaml")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if spec.Pipeline.Name != "order-events" {
		t.Errorf("expected 'order-events', got %q", spec.Pipeline.Name)
	}
	result := config.Validate(spec)
	if !result.IsValid() {
		t.Errorf("simple example should be valid: %v", result.Errors)
	}
}

func TestLoadExampleAdvanced(t *testing.T) {
	spec, err := config.Load("examples/advanced/pipeline.yaml")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if spec.Pipeline.Name != "payment-features" {
		t.Errorf("expected 'payment-features', got %q", spec.Pipeline.Name)
	}
	if len(spec.Pipeline.Transforms) != 8 {
		t.Errorf("expected 8 transforms, got %d", len(spec.Pipeline.Transforms))
	}
	if len(spec.Pipeline.Sinks) != 2 {
		t.Errorf("expected 2 additional sinks, got %d", len(spec.Pipeline.Sinks))
	}
}

// ═══════════════════════════════════════════
// Transform Tests
// ═══════════════════════════════════════════

func TestHashFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email", "phone"}},
	}
	chain, err := transform.NewChain(specs, transform.WithPIISalt("test-salt"))
	if err != nil {
		t.Fatalf("build chain: %v", err)
	}

	event := map[string]any{
		"email":  "john@example.com",
		"phone":  "+15551234567",
		"amount": 99.99,
	}

	result, err := chain.Apply(event)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Email should be hashed (not original)
	if result["email"] == "john@example.com" {
		t.Error("email should be hashed")
	}
	// Phone should be hashed
	if result["phone"] == "+15551234567" {
		t.Error("phone should be hashed")
	}
	// Amount should be unchanged
	if result["amount"] != 99.99 {
		t.Errorf("amount should be unchanged, got %v", result["amount"])
	}
	// PII processed flag
	if result["_pii_processed"] != true {
		t.Error("should set _pii_processed flag")
	}
	// Hash should be deterministic
	result2, _ := chain.Apply(map[string]any{"email": "john@example.com", "phone": "+15551234567", "amount": 99.99})
	if result["email"] != result2["email"] {
		t.Error("hash should be deterministic")
	}
}

func TestMaskFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "mask", Type: v1.TransformMaskFields, Fields: []string{"email", "card"}},
	}
	chain, _ := transform.NewChain(specs)

	event := map[string]any{
		"email": "john@example.com",
		"card":  "4111222233334444",
		"name":  "John",
	}

	result, _ := chain.Apply(event)

	email, _ := result["email"].(string)
	if !strings.HasPrefix(email, "j") || !strings.Contains(email, "***") {
		t.Errorf("email mask incorrect: %s", email)
	}

	card, _ := result["card"].(string)
	if !strings.HasSuffix(card, "4444") || !strings.Contains(card, "****") {
		t.Errorf("card mask incorrect: %s", card)
	}

	if result["name"] != "John" {
		t.Error("non-masked fields should be unchanged")
	}
}

func TestDropFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"internal_id", "debug"}},
	}
	chain, _ := transform.NewChain(specs)

	event := map[string]any{
		"email":       "john@example.com",
		"internal_id": "abc123",
		"debug":       true,
	}

	result, _ := chain.Apply(event)

	if _, exists := result["internal_id"]; exists {
		t.Error("internal_id should be dropped")
	}
	if _, exists := result["debug"]; exists {
		t.Error("debug should be dropped")
	}
	if result["email"] != "john@example.com" {
		t.Error("email should remain")
	}
}

func TestFilterTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed"},
	}
	chain, _ := transform.NewChain(specs)

	// Should pass
	result, _ := chain.Apply(map[string]any{"status": "completed", "amount": 100})
	if result == nil {
		t.Error("completed event should pass filter")
	}

	// Should be filtered out
	result, _ = chain.Apply(map[string]any{"status": "test", "amount": 1})
	if result != nil {
		t.Error("test event should be filtered out")
	}
}

func TestFilterTransformAND(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed AND environment = production"},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"status": "completed", "environment": "production"})
	if result == nil {
		t.Error("should pass both conditions")
	}

	result, _ = chain.Apply(map[string]any{"status": "completed", "environment": "staging"})
	if result != nil {
		t.Error("should fail second condition")
	}
}

func TestFilterTransformOR(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed OR status = pending"},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"status": "completed"})
	if result == nil {
		t.Error("completed should pass")
	}

	result, _ = chain.Apply(map[string]any{"status": "pending"})
	if result == nil {
		t.Error("pending should pass")
	}

	result, _ = chain.Apply(map[string]any{"status": "failed"})
	if result != nil {
		t.Error("failed should not pass")
	}
}

func TestFilterIsNotNull(t *testing.T) {
	specs := []v1.Transform{
		{Name: "filter", Type: v1.TransformFilter, Condition: "email IS NOT NULL"},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"email": "test@test.com"})
	if result == nil {
		t.Error("non-null email should pass")
	}

	result, _ = chain.Apply(map[string]any{"email": nil})
	if result != nil {
		t.Error("null email should be filtered")
	}

	result, _ = chain.Apply(map[string]any{"name": "test"})
	if result != nil {
		t.Error("missing email should be filtered")
	}
}

func TestRenameFieldsTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "rename", Type: v1.TransformRename, Mapping: map[string]string{"amt": "amount", "ts": "timestamp"}},
	}
	chain, _ := transform.NewChain(specs)

	result, _ := chain.Apply(map[string]any{"amt": 100.0, "ts": "2024-01-01", "name": "test"})

	if _, exists := result["amt"]; exists {
		t.Error("old field 'amt' should be removed")
	}
	if result["amount"] != 100.0 {
		t.Error("'amount' should have the value")
	}
	if result["name"] != "test" {
		t.Error("unrenamed fields should remain")
	}
}

func TestTransformChaining(t *testing.T) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email"}},
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"internal_id"}},
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed"},
	}
	chain, _ := transform.NewChain(specs)

	// Event that passes all transforms
	event := map[string]any{
		"email":       "john@example.com",
		"internal_id": "abc",
		"status":      "completed",
		"amount":      100,
	}

	result, _ := chain.Apply(event)
	if result == nil {
		t.Fatal("event should pass all transforms")
	}

	// Email should be hashed
	if result["email"] == "john@example.com" {
		t.Error("email should be hashed")
	}
	// internal_id should be dropped
	if _, exists := result["internal_id"]; exists {
		t.Error("internal_id should be dropped")
	}

	// Event that gets filtered
	event2 := map[string]any{"email": "test@test.com", "status": "test"}
	result2, _ := chain.Apply(event2)
	if result2 != nil {
		t.Error("test event should be filtered out")
	}
}

func TestEmptyChain(t *testing.T) {
	chain, _ := transform.NewChain(nil)
	event := map[string]any{"key": "value"}
	result, _ := chain.Apply(event)
	if result["key"] != "value" {
		t.Error("empty chain should pass through")
	}
}

func TestDeduplicateTransform(t *testing.T) {
	specs := []v1.Transform{
		{Name: "dedupe", Type: v1.TransformDedupe, Fields: []string{"event_id"}},
	}
	chain, _ := transform.NewChain(specs)

	// First time: pass
	result, _ := chain.Apply(map[string]any{"event_id": "e1", "data": "first"})
	if result == nil {
		t.Error("first occurrence should pass")
	}

	// Duplicate: filter
	result, _ = chain.Apply(map[string]any{"event_id": "e1", "data": "duplicate"})
	if result != nil {
		t.Error("duplicate should be filtered")
	}

	// Different ID: pass
	result, _ = chain.Apply(map[string]any{"event_id": "e2", "data": "second"})
	if result == nil {
		t.Error("different ID should pass")
	}
}

// ═══════════════════════════════════════════
// Codegen Tests
// ═══════════════════════════════════════════

func TestGenerateK8s(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "test-pipeline",
			Owner:  "test-team",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "events.test", ConsumerGroup: "mako-test"},
			Sink:   v1.Sink{Type: v1.SinkSnowflake, Table: "TEST_EVENTS"},
		},
	}

	k8s, err := codegen.GenerateK8s(spec, "registry.example.com")
	if err != nil {
		t.Fatalf("generate k8s: %v", err)
	}

	checks := []string{
		"test-pipeline",
		"registry.example.com",
		"mako.io/pipeline",
		"app.kubernetes.io/managed-by: mako",
		"events.test",
		"prometheus.io/scrape",
	}

	for _, check := range checks {
		if !strings.Contains(k8s, check) {
			t.Errorf("K8s manifest should contain %q", check)
		}
	}
}

func TestGenerateTerraform(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "order-events",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "events.orders"},
			Sink: v1.Sink{
				Type:     v1.SinkSnowflake,
				Database: "ANALYTICS",
				Schema:   "RAW",
				Table:    "ORDER_EVENTS",
			},
			Isolation: v1.IsolationSpec{DLQEnabled: true},
		},
	}

	tf, err := codegen.GenerateTerraform(spec)
	if err != nil {
		t.Fatalf("generate terraform: %v", err)
	}

	checks := []string{
		"kafka_topic",
		"events.orders",
		"snowflake_table",
		"ANALYTICS",
		"ORDER_EVENTS",
		"snowflake_pipe",
		"dlq",
	}

	for _, check := range checks {
		if !strings.Contains(tf, check) {
			t.Errorf("Terraform should contain %q", check)
		}
	}
}

func TestGenerateTerraformBigQuery(t *testing.T) {
	spec := &v1.PipelineSpec{
		Pipeline: v1.Pipeline{
			Name:   "bq-events",
			Source: v1.Source{Type: v1.SourceKafka, Topic: "events.bq"},
			Sink: v1.Sink{
				Type:   v1.SinkBigQuery,
				Schema: "raw_events",
				Table:  "events",
			},
		},
	}

	tf, err := codegen.GenerateTerraform(spec)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	if !strings.Contains(tf, "google_bigquery_table") {
		t.Error("should contain BigQuery table resource")
	}
}

// ═══════════════════════════════════════════
// Integration: dry-run with fixture data
// ═══════════════════════════════════════════

func TestDryRunWithFixtures(t *testing.T) {
	data, err := os.ReadFile("test/fixtures/events.jsonl")
	if err != nil {
		t.Skip("fixtures not found")
	}

	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email", "phone"}},
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"credit_card_number"}},
		{Name: "filter", Type: v1.TransformFilter, Condition: "status != test"},
	}

	chain, err := transform.NewChain(specs, transform.WithPIISalt("fixture-salt"))
	if err != nil {
		t.Fatalf("build chain: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	passed, filtered := 0, 0

	for _, line := range lines {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("parse fixture: %v", err)
		}

		result, err := chain.Apply(event)
		if err != nil {
			t.Fatalf("transform error: %v", err)
		}

		if result == nil {
			filtered++
			continue
		}
		passed++

		// Verify PII is hashed
		if result["email"] == event["email"] {
			t.Error("email should be hashed in output")
		}
		// Verify credit_card is dropped
		if _, exists := result["credit_card_number"]; exists {
			t.Error("credit_card_number should be dropped")
		}
	}

	if passed == 0 {
		t.Error("expected some events to pass")
	}
	if filtered == 0 {
		t.Error("expected some events to be filtered")
	}

	t.Logf("Fixtures: %d total → %d passed, %d filtered", len(lines), passed, filtered)
}

// ═══════════════════════════════════════════
// Benchmark
// ═══════════════════════════════════════════

func BenchmarkTransformChain(b *testing.B) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email", "phone"}},
		{Name: "drop", Type: v1.TransformDropFields, Fields: []string{"internal_id"}},
		{Name: "filter", Type: v1.TransformFilter, Condition: "status = completed"},
	}
	chain, _ := transform.NewChain(specs)

	event := map[string]any{
		"email":       "john@example.com",
		"phone":       "+15551234567",
		"internal_id": "abc",
		"status":      "completed",
		"amount":      99.99,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chain.Apply(event)
	}
}

func BenchmarkHashField(b *testing.B) {
	specs := []v1.Transform{
		{Name: "hash", Type: v1.TransformHashFields, Fields: []string{"email"}},
	}
	chain, _ := transform.NewChain(specs)
	event := map[string]any{"email": "john@example.com"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chain.Apply(event)
	}
}

// ═══════════════════════════════════════════
// Postgres Flatten Tests
// ═══════════════════════════════════════════

func TestPgColumnTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "TEXT"},
		{"float64", 42.5, "NUMERIC"},
		{"int", 42, "NUMERIC"},
		{"bool_true", true, "BOOLEAN"},
		{"bool_false", false, "BOOLEAN"},
		{"nested_object", map[string]any{"key": "val"}, "JSONB"},
		{"array", []any{1, 2, 3}, "JSONB"},
		{"nil", nil, "TEXT"},
		{"timestamp_rfc3339", "2024-01-15T10:30:00Z", "TIMESTAMPTZ"},
		{"timestamp_space", "2024-01-15 10:30:00+00", "TIMESTAMPTZ"},
		{"not_timestamp", "hello world", "TEXT"},
		{"empty_string", "", "TEXT"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sink.PgColumnType(tc.value)
			if got != tc.expected {
				t.Errorf("PgColumnType(%v) = %q, want %q", tc.value, got, tc.expected)
			}
		})
	}
}

func TestIsTimestampLike(t *testing.T) {
	positives := []string{
		"2024-01-15T10:30:00Z",
		"2024-01-15T10:30:00.123Z",
		"2024-01-15 10:30:00",
		"2024-01-15T10:30:00+05:30",
		"2023-12-31T23:59:59.999999Z",
	}
	for _, s := range positives {
		if !sink.IsTimestampLike(s) {
			t.Errorf("IsTimestampLike(%q) should be true", s)
		}
	}

	negatives := []string{
		"hello",
		"2024-01-15",
		"10:30:00",
		"",
		"not-a-timestamp",
		"2024/01/15 10:30:00",
	}
	for _, s := range negatives {
		if sink.IsTimestampLike(s) {
			t.Errorf("IsTimestampLike(%q) should be false", s)
		}
	}
}

func TestPostgresFlattenConfigParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: flatten-test
  source:
    type: kafka
    topic: events.users
  sink:
    type: postgres
    database: mako
    schema: public
    table: users
    flatten: true
    config:
      host: localhost
      port: "5432"
      user: mako
      password: mako
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Sink.Type != v1.SinkPostgres {
		t.Errorf("expected sink type postgres, got %q", spec.Pipeline.Sink.Type)
	}
	if !spec.Pipeline.Sink.Flatten {
		t.Error("expected flatten to be true")
	}
	if spec.Pipeline.Sink.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Pipeline.Sink.Table)
	}
}

func TestPostgresFlattenNewSinkSignature(t *testing.T) {
	// Verify NewPostgresSink accepts the flatten parameter and stores it.
	cfg := map[string]any{
		"host":     "localhost",
		"port":     "5432",
		"user":     "test",
		"password": "test",
	}

	s := sink.NewPostgresSink("testdb", "public", "users", true, cfg)
	if s == nil {
		t.Fatal("NewPostgresSink returned nil")
	}
	if s.Name() != "postgres:public.users" {
		t.Errorf("unexpected name: %s", s.Name())
	}

	// Non-flatten mode
	s2 := sink.NewPostgresSink("testdb", "public", "events", false, cfg)
	if s2 == nil {
		t.Fatal("NewPostgresSink returned nil for non-flatten")
	}
}

func TestPostgresFlattenBuildFromSpec(t *testing.T) {
	// Verify BuildFromSpec passes flatten flag to postgres sink.
	spec := v1.Sink{
		Type:     v1.SinkPostgres,
		Database: "mako",
		Schema:   "public",
		Table:    "users",
		Flatten:  true,
		Config: map[string]any{
			"host":     "localhost",
			"port":     "5432",
			"user":     "mako",
			"password": "mako",
		},
	}

	s, err := sink.BuildFromSpec(spec)
	if err != nil {
		t.Fatalf("BuildFromSpec failed: %v", err)
	}
	if s == nil {
		t.Fatal("BuildFromSpec returned nil")
	}
	if s.Name() != "postgres:public.users" {
		t.Errorf("unexpected sink name: %s", s.Name())
	}
}

func TestPgColumnTypeMixedEvent(t *testing.T) {
	// Simulate a realistic event and verify type inference for each field.
	event := map[string]any{
		"user_id":    "usr-001",
		"email":      "test@example.com",
		"age":        float64(30),
		"active":     true,
		"created_at": "2024-06-15T10:00:00Z",
		"address":    map[string]any{"city": "Paris", "zip": "75001"},
		"tags":       []any{"admin", "user"},
		"score":      float64(95.5),
	}

	expected := map[string]string{
		"user_id":    "TEXT",
		"email":      "TEXT",
		"age":        "NUMERIC",
		"active":     "BOOLEAN",
		"created_at": "TIMESTAMPTZ",
		"address":    "JSONB",
		"tags":       "JSONB",
		"score":      "NUMERIC",
	}

	for field, expectedType := range expected {
		got := sink.PgColumnType(event[field])
		if got != expectedType {
			t.Errorf("field %q: PgColumnType(%v) = %q, want %q",
				field, event[field], got, expectedType)
		}
	}
}

// ═══════════════════════════════════════════
// BigQuery Flatten Tests
// ═══════════════════════════════════════════

func TestBqColumnTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "STRING"},
		{"float64", 42.5, "FLOAT64"},
		{"int", 42, "INT64"},
		{"bool_true", true, "BOOLEAN"},
		{"bool_false", false, "BOOLEAN"},
		{"nested_object", map[string]any{"key": "val"}, "JSON"},
		{"array", []any{1, 2, 3}, "JSON"},
		{"nil", nil, "STRING"},
		{"timestamp_rfc3339", "2024-01-15T10:30:00Z", "TIMESTAMP"},
		{"timestamp_space", "2024-01-15 10:30:00+00", "TIMESTAMP"},
		{"not_timestamp", "hello world", "STRING"},
		{"empty_string", "", "STRING"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sink.BqColumnType(tc.value)
			if got != tc.expected {
				t.Errorf("BqColumnType(%v) = %q, want %q", tc.value, got, tc.expected)
			}
		})
	}
}

func TestBigQueryFlattenConfigParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: bq-flatten-test
  source:
    type: kafka
    topic: events.users
  sink:
    type: bigquery
    schema: raw_events
    table: users
    flatten: true
    config:
      project: my-gcp-project
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Sink.Type != v1.SinkBigQuery {
		t.Errorf("expected sink type bigquery, got %q", spec.Pipeline.Sink.Type)
	}
	if !spec.Pipeline.Sink.Flatten {
		t.Error("expected flatten to be true")
	}
	if spec.Pipeline.Sink.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Pipeline.Sink.Table)
	}
}

func TestBigQueryFlattenNewSinkSignature(t *testing.T) {
	cfg := map[string]any{
		"project": "my-project",
	}

	s := sink.NewBigQuerySink("my-project", "raw_events", "users", true, cfg)
	if s == nil {
		t.Fatal("NewBigQuerySink returned nil")
	}
	if s.Name() != "bigquery:my-project.raw_events.users" {
		t.Errorf("unexpected name: %s", s.Name())
	}

	// Non-flatten mode
	s2 := sink.NewBigQuerySink("my-project", "raw_events", "events", false, cfg)
	if s2 == nil {
		t.Fatal("NewBigQuerySink returned nil for non-flatten")
	}
}

func TestBigQueryFlattenBuildFromSpec(t *testing.T) {
	spec := v1.Sink{
		Type:    v1.SinkBigQuery,
		Schema:  "raw_events",
		Table:   "users",
		Flatten: true,
		Config: map[string]any{
			"project": "my-gcp-project",
		},
	}

	s, err := sink.BuildFromSpec(spec)
	if err != nil {
		t.Fatalf("BuildFromSpec failed: %v", err)
	}
	if s == nil {
		t.Fatal("BuildFromSpec returned nil")
	}
	if s.Name() != "bigquery:my-gcp-project.raw_events.users" {
		t.Errorf("unexpected sink name: %s", s.Name())
	}
}

func TestBqColumnTypeMixedEvent(t *testing.T) {
	event := map[string]any{
		"user_id":    "usr-001",
		"email":      "test@example.com",
		"age":        float64(30),
		"active":     true,
		"created_at": "2024-06-15T10:00:00Z",
		"address":    map[string]any{"city": "Paris", "zip": "75001"},
		"tags":       []any{"admin", "user"},
		"score":      float64(95.5),
	}

	expected := map[string]string{
		"user_id":    "STRING",
		"email":      "STRING",
		"age":        "FLOAT64",
		"active":     "BOOLEAN",
		"created_at": "TIMESTAMP",
		"address":    "JSON",
		"tags":       "JSON",
		"score":      "FLOAT64",
	}

	for field, expectedType := range expected {
		got := sink.BqColumnType(event[field])
		if got != expectedType {
			t.Errorf("field %q: BqColumnType(%v) = %q, want %q",
				field, event[field], got, expectedType)
		}
	}
}

// ═══════════════════════════════════════════
// ClickHouse Flatten Tests
// ═══════════════════════════════════════════

func TestChColumnTypeInference(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		expected string
	}{
		{"string", "hello", "String"},
		{"float64", 42.5, "Float64"},
		{"int", 42, "Int64"},
		{"bool_true", true, "Bool"},
		{"bool_false", false, "Bool"},
		{"nested_object", map[string]any{"key": "val"}, "String"},
		{"array", []any{1, 2, 3}, "String"},
		{"nil", nil, "String"},
		{"timestamp_rfc3339", "2024-01-15T10:30:00Z", "DateTime64(3)"},
		{"timestamp_space", "2024-01-15 10:30:00+00", "DateTime64(3)"},
		{"not_timestamp", "hello world", "String"},
		{"empty_string", "", "String"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sink.ChColumnType(tc.value)
			if got != tc.expected {
				t.Errorf("ChColumnType(%v) = %q, want %q", tc.value, got, tc.expected)
			}
		})
	}
}

func TestClickHouseFlattenConfigParsing(t *testing.T) {
	yaml := `
apiVersion: mako/v1
kind: Pipeline
pipeline:
  name: ch-flatten-test
  source:
    type: kafka
    topic: events.users
  sink:
    type: clickhouse
    database: analytics
    table: users
    flatten: true
    config:
      host: localhost
      port: "9000"
      user: default
      password: ""
`
	spec, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if spec.Pipeline.Sink.Type != v1.SinkClickHouse {
		t.Errorf("expected sink type clickhouse, got %q", spec.Pipeline.Sink.Type)
	}
	if !spec.Pipeline.Sink.Flatten {
		t.Error("expected flatten to be true")
	}
	if spec.Pipeline.Sink.Table != "users" {
		t.Errorf("expected table 'users', got %q", spec.Pipeline.Sink.Table)
	}
	if spec.Pipeline.Sink.Database != "analytics" {
		t.Errorf("expected database 'analytics', got %q", spec.Pipeline.Sink.Database)
	}
}

func TestClickHouseFlattenNewSinkSignature(t *testing.T) {
	cfg := map[string]any{
		"host":     "localhost",
		"port":     "9000",
		"user":     "default",
		"password": "",
	}

	s := sink.NewClickHouseSink("analytics", "users", true, cfg)
	if s == nil {
		t.Fatal("NewClickHouseSink returned nil")
	}
	if s.Name() != "clickhouse:analytics.users" {
		t.Errorf("unexpected name: %s", s.Name())
	}

	// Non-flatten mode
	s2 := sink.NewClickHouseSink("analytics", "events", false, cfg)
	if s2 == nil {
		t.Fatal("NewClickHouseSink returned nil for non-flatten")
	}
}

func TestClickHouseFlattenBuildFromSpec(t *testing.T) {
	spec := v1.Sink{
		Type:     v1.SinkClickHouse,
		Database: "analytics",
		Table:    "users",
		Flatten:  true,
		Config: map[string]any{
			"host":     "localhost",
			"port":     "9000",
			"user":     "default",
			"password": "",
		},
	}

	s, err := sink.BuildFromSpec(spec)
	if err != nil {
		t.Fatalf("BuildFromSpec failed: %v", err)
	}
	if s == nil {
		t.Fatal("BuildFromSpec returned nil")
	}
	if s.Name() != "clickhouse:analytics.users" {
		t.Errorf("unexpected sink name: %s", s.Name())
	}
}

func TestChColumnTypeMixedEvent(t *testing.T) {
	event := map[string]any{
		"user_id":    "usr-001",
		"email":      "test@example.com",
		"age":        float64(30),
		"active":     true,
		"created_at": "2024-06-15T10:00:00Z",
		"address":    map[string]any{"city": "Paris", "zip": "75001"},
		"tags":       []any{"admin", "user"},
		"score":      float64(95.5),
	}

	expected := map[string]string{
		"user_id":    "String",
		"email":      "String",
		"age":        "Float64",
		"active":     "Bool",
		"created_at": "DateTime64(3)",
		"address":    "String",
		"tags":       "String",
		"score":      "Float64",
	}

	for field, expectedType := range expected {
		got := sink.ChColumnType(event[field])
		if got != expectedType {
			t.Errorf("field %q: ChColumnType(%v) = %q, want %q",
				field, event[field], got, expectedType)
		}
	}
}

// ═══════════════════════════════════════════
// Parquet Encoder Tests
// ═══════════════════════════════════════════

func makeTestEvents() []*pipeline.Event {
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	return []*pipeline.Event{
		{
			Key:       []byte("key1"),
			Value:     map[string]any{"user_id": "u1", "amount": 99.5, "active": true, "address": map[string]any{"city": "Paris"}},
			Timestamp: ts,
			Topic:     "events.orders",
			Partition: 0,
			Offset:    100,
		},
		{
			Key:       []byte("key2"),
			Value:     map[string]any{"user_id": "u2", "amount": 42.0, "active": false, "tags": []any{"vip"}},
			Timestamp: ts,
			Topic:     "events.orders",
			Partition: 0,
			Offset:    101,
		},
	}
}

func TestEncodeParquetBasic(t *testing.T) {
	events := makeTestEvents()
	data, err := sink.EncodeParquet(events, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("EncodeParquet returned empty bytes")
	}
	// Parquet magic bytes: PAR1
	if string(data[:4]) != "PAR1" {
		t.Errorf("expected Parquet magic bytes PAR1, got %q", string(data[:4]))
	}
	// File should also end with PAR1
	if string(data[len(data)-4:]) != "PAR1" {
		t.Errorf("expected Parquet footer magic PAR1, got %q", string(data[len(data)-4:]))
	}
	t.Logf("Parquet output: %d bytes for %d events", len(data), len(events))
}

func TestEncodeParquetTypes(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	events := []*pipeline.Event{
		{
			Value: map[string]any{
				"name":    "Alice",
				"score":   float64(95.5),
				"active":  true,
				"address": map[string]any{"city": "NYC"},
				"tags":    []any{"admin", "user"},
			},
			Timestamp: ts,
			Topic:     "test",
			Offset:    1,
		},
	}
	data, err := sink.EncodeParquet(events, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet failed: %v", err)
	}
	if len(data) < 12 {
		t.Fatal("Parquet output too small")
	}
	// Verify magic bytes
	if string(data[:4]) != "PAR1" {
		t.Error("missing PAR1 header")
	}
}

func TestEncodeParquetCompression(t *testing.T) {
	events := makeTestEvents()

	compressions := []string{"snappy", "zstd", "gzip", "none"}
	for _, comp := range compressions {
		t.Run(comp, func(t *testing.T) {
			data, err := sink.EncodeParquet(events, comp)
			if err != nil {
				t.Fatalf("EncodeParquet(%s) failed: %v", comp, err)
			}
			if len(data) == 0 {
				t.Fatalf("EncodeParquet(%s) returned empty", comp)
			}
			if string(data[:4]) != "PAR1" {
				t.Errorf("EncodeParquet(%s) missing PAR1 magic", comp)
			}
		})
	}
}

func TestEncodeParquetEmpty(t *testing.T) {
	data, err := sink.EncodeParquet(nil, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet(nil) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeParquet(nil) should return nil, got %d bytes", len(data))
	}

	data, err = sink.EncodeParquet([]*pipeline.Event{}, "snappy")
	if err != nil {
		t.Fatalf("EncodeParquet([]) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeParquet([]) should return nil, got %d bytes", len(data))
	}
}

func TestS3SinkParquetFormat(t *testing.T) {
	cfg := map[string]any{"region": "us-east-1", "compression": "snappy"}
	s := sink.NewS3Sink("my-bucket", "raw/events", "parquet", cfg)
	if s == nil {
		t.Fatal("NewS3Sink returned nil")
	}
	if s.Name() != "s3:my-bucket/raw/events" {
		t.Errorf("unexpected name: %s", s.Name())
	}
	if s.Format() != "parquet" {
		t.Errorf("expected format parquet, got %s", s.Format())
	}
}

func TestGCSSinkParquetFormat(t *testing.T) {
	cfg := map[string]any{"project": "my-project", "compression": "zstd"}
	s := sink.NewGCSSink("my-bucket", "raw/events", "parquet", cfg)
	if s == nil {
		t.Fatal("NewGCSSink returned nil")
	}
	if s.Name() != "gcs:my-bucket/raw/events" {
		t.Errorf("unexpected name: %s", s.Name())
	}
	if s.Format() != "parquet" {
		t.Errorf("expected format parquet, got %s", s.Format())
	}
}

// ═══════════════════════════════════════════
// CSV Encoder Tests
// ═══════════════════════════════════════════

func TestEncodeCSVBasic(t *testing.T) {
	events := makeTestEvents()
	data, err := sink.EncodeCSV(events, ',')
	if err != nil {
		t.Fatalf("EncodeCSV failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("EncodeCSV returned empty bytes")
	}

	// Parse CSV output
	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV output: %v", err)
	}

	// Should have header + 2 data rows
	if len(records) != 3 {
		t.Errorf("expected 3 rows (header + 2 data), got %d", len(records))
	}

	// Header should be sorted
	header := records[0]
	for i := 1; i < len(header); i++ {
		if header[i] < header[i-1] {
			t.Errorf("header not sorted: %v", header)
			break
		}
	}

	t.Logf("CSV output: %d bytes, %d columns, %d rows", len(data), len(header), len(records)-1)
}

func TestEncodeCSVAllKeys(t *testing.T) {
	// Events with different key sets — CSV should have union of all keys.
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []*pipeline.Event{
		{Value: map[string]any{"a": "1", "b": "2"}, Timestamp: ts, Topic: "t", Offset: 1},
		{Value: map[string]any{"b": "3", "c": "4"}, Timestamp: ts, Topic: "t", Offset: 2},
	}
	data, err := sink.EncodeCSV(events, ',')
	if err != nil {
		t.Fatalf("EncodeCSV failed: %v", err)
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}

	header := records[0]
	// Should contain a, b, c plus metadata columns (_offset, _topic, _ts)
	colSet := make(map[string]bool)
	for _, h := range header {
		colSet[h] = true
	}
	for _, expected := range []string{"a", "b", "c", "_offset", "_topic", "_ts"} {
		if !colSet[expected] {
			t.Errorf("missing expected column %q in header %v", expected, header)
		}
	}
}

func TestEncodeCSVNested(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []*pipeline.Event{
		{
			Value:     map[string]any{"name": "Alice", "addr": map[string]any{"city": "NYC", "zip": "10001"}},
			Timestamp: ts,
			Topic:     "t",
			Offset:    1,
		},
	}
	data, err := sink.EncodeCSV(events, ',')
	if err != nil {
		t.Fatalf("EncodeCSV failed: %v", err)
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}

	// Find the addr column
	header := records[0]
	addrIdx := -1
	for i, h := range header {
		if h == "addr" {
			addrIdx = i
			break
		}
	}
	if addrIdx == -1 {
		t.Fatal("addr column not found in header")
	}

	// addr value should be JSON-serialized
	addrVal := records[1][addrIdx]
	var parsed map[string]any
	if err := json.Unmarshal([]byte(addrVal), &parsed); err != nil {
		t.Errorf("addr value should be valid JSON, got %q: %v", addrVal, err)
	}
	if parsed["city"] != "NYC" {
		t.Errorf("expected city=NYC, got %v", parsed["city"])
	}
}

func TestEncodeCSVDelimiter(t *testing.T) {
	events := makeTestEvents()

	cases := []struct {
		name  string
		delim rune
	}{
		{"tab", '\t'},
		{"semicolon", ';'},
		{"pipe", '|'},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := sink.EncodeCSV(events, tc.delim)
			if err != nil {
				t.Fatalf("EncodeCSV(%s) failed: %v", tc.name, err)
			}
			if len(data) == 0 {
				t.Fatal("empty output")
			}

			r := csv.NewReader(strings.NewReader(string(data)))
			r.Comma = tc.delim
			records, err := r.ReadAll()
			if err != nil {
				t.Fatalf("parse CSV with delimiter %q: %v", tc.delim, err)
			}
			if len(records) != 3 {
				t.Errorf("expected 3 rows, got %d", len(records))
			}
		})
	}
}

func TestEncodeCSVEmpty(t *testing.T) {
	data, err := sink.EncodeCSV(nil, ',')
	if err != nil {
		t.Fatalf("EncodeCSV(nil) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeCSV(nil) should return nil, got %d bytes", len(data))
	}

	data, err = sink.EncodeCSV([]*pipeline.Event{}, ',')
	if err != nil {
		t.Fatalf("EncodeCSV([]) should not error: %v", err)
	}
	if data != nil {
		t.Errorf("EncodeCSV([]) should return nil, got %d bytes", len(data))
	}
}

func TestS3SinkCSVFormat(t *testing.T) {
	cfg := map[string]any{"region": "us-east-1", "csv_delimiter": ";"}
	s := sink.NewS3Sink("my-bucket", "raw/exports", "csv", cfg)
	if s == nil {
		t.Fatal("NewS3Sink returned nil")
	}
	if s.Format() != "csv" {
		t.Errorf("expected format csv, got %s", s.Format())
	}
}

func TestGCSSinkCSVFormat(t *testing.T) {
	cfg := map[string]any{"project": "my-project", "csv_delimiter": "\\t"}
	s := sink.NewGCSSink("my-bucket", "raw/exports", "csv", cfg)
	if s == nil {
		t.Fatal("NewGCSSink returned nil")
	}
	if s.Format() != "csv" {
		t.Errorf("expected format csv, got %s", s.Format())
	}
}

// ═══════════════════════════════════════════
// Format helpers tests
// ═══════════════════════════════════════════

func TestFormatExtension(t *testing.T) {
	cases := map[string]string{
		"json":    "json",
		"jsonl":   "jsonl",
		"parquet": "parquet",
		"csv":     "csv",
		"":        "jsonl",
		"unknown": "jsonl",
	}
	for input, expected := range cases {
		got := sink.FormatExtension(input)
		if got != expected {
			t.Errorf("FormatExtension(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestFormatContentType(t *testing.T) {
	cases := map[string]string{
		"json":    "application/json",
		"jsonl":   "application/x-ndjson",
		"parquet": "application/octet-stream",
		"csv":     "text/csv",
		"":        "application/x-ndjson",
	}
	for input, expected := range cases {
		got := sink.FormatContentType(input)
		if got != expected {
			t.Errorf("FormatContentType(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestParseCSVDelimiter(t *testing.T) {
	cases := []struct {
		input    string
		expected rune
	}{
		{",", ','},
		{"", ','},
		{";", ';'},
		{"|", '|'},
		{"\\t", '\t'},
		{"tab", '\t'},
	}
	for _, tc := range cases {
		got := sink.ParseCSVDelimiter(tc.input)
		if got != tc.expected {
			t.Errorf("ParseCSVDelimiter(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestMain(m *testing.M) {
	// Change to project root for example loading
	if _, err := os.Stat("examples"); os.IsNotExist(err) {
		os.Chdir("..")
	}
	fmt.Println("🧪 Mako Test Suite")
	os.Exit(m.Run())
}
