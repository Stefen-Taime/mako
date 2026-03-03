// mako — Declarative Real-Time Data Pipelines
//
// Usage:
//
//	mako init                     # Create starter pipeline.yaml
//	mako validate pipeline.yaml   # Validate pipeline spec
//	mako apply pipeline.yaml      # Deploy pipeline
//	mako status                   # Show running pipelines
//	mako logs order-events        # Tail pipeline logs
//	mako destroy order-events     # Remove pipeline
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	v1 "github.com/Stefen-Taime/mako/api/v1"
	"github.com/Stefen-Taime/mako/internal/cli"
	"github.com/Stefen-Taime/mako/pkg/alerting"
	"github.com/Stefen-Taime/mako/pkg/config"
	"github.com/Stefen-Taime/mako/pkg/join"
	"github.com/Stefen-Taime/mako/pkg/kafka"
	"github.com/Stefen-Taime/mako/pkg/observability"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/schema"
	"github.com/Stefen-Taime/mako/pkg/sink"
	"github.com/Stefen-Taime/mako/pkg/source"
	"github.com/Stefen-Taime/mako/pkg/transform"
	"github.com/Stefen-Taime/mako/pkg/workflow"
)

func main() {
	if len(os.Args) < 2 {
		cli.PrintBanner()
		printUsage()
		os.Exit(0)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "init":
		err = cmdInit(args)
	case "validate":
		err = cmdValidate(args)
	case "run":
		err = cmdRun(args)
	case "workflow":
		err = cmdWorkflow(args)
	case "dry-run":
		err = cmdDryRun(args)
	case "version":
		fmt.Printf("mako %s\n", cli.Version)
	case "help", "--help", "-h":
		cli.PrintBanner()
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ %s\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: mako <command> [options]

Commands:
  init                         Create a starter pipeline.yaml
  validate <file|dir>          Validate pipeline or workflow specification
  run      <file> [--config]   Run pipeline locally (Source -> Transforms -> Sink)
  workflow <file>              Run a multi-pipeline workflow (DAG)
  dry-run  <file>              Process sample data locally (stdin)
  version                      Print version

Examples:
  mako init
  mako validate pipeline.yaml
  mako validate workflow.yaml
  mako validate pipelines/
  mako run pipeline.yaml
  mako workflow etl-daily.yaml
  mako dry-run pipeline.yaml < events.jsonl`)
}

// ═══════════════════════════════════════════
// init — Create starter pipeline YAML
// ═══════════════════════════════════════════

func cmdInit(args []string) error {
	filename := "pipeline.yaml"
	if len(args) > 0 {
		filename = args[0]
	}

	if _, err := os.Stat(filename); err == nil {
		return fmt.Errorf("%s already exists", filename)
	}

	if err := os.WriteFile(filename, []byte(starterPipeline), 0644); err != nil {
		return err
	}

	fmt.Printf("✅ Created %s\n", filename)
	fmt.Println("   Next steps:")
	fmt.Printf("   mako validate %s   # check syntax\n", filename)
	fmt.Printf("   mako run %s        # run pipeline\n", filename)
	return nil
}

const starterPipeline = `# Mako Pipeline — Starter Template
# Docs: https://github.com/Stefen-Taime/mako
#
# This pipeline fetches commerce data from a public JSON API,
# applies transforms, and prints results to stdout.
#
# Quick start:
#   mako validate pipeline.yaml
#   mako run pipeline.yaml
#
apiVersion: mako/v1
kind: Pipeline

pipeline:
  name: commerce-ingest
  description: "Ingest commerce data from open-source-data"
  owner: data-engineering

  source:
    type: http
    config:
      url: https://raw.githubusercontent.com/Stefen-Taime/open-source-data/main/commerce/json/json_bank_20240116_1.json
      method: GET
      auth_type: none
      response_type: json

  transforms:
    # Hash user_id for privacy
    - name: pii_mask
      type: hash_fields
      fields: [user_id]

    # Drop fields we don't need
    - name: cleanup
      type: drop_fields
      fields: [price_string, dt_current_timestamp]

    # Keep only items above $50
    - name: filter_price
      type: filter
      condition: "price > 50"

  sink:
    type: stdout

  # ── Uncomment below to write to PostgreSQL instead ──
  # sink:
  #   type: postgres
  #   database: mako
  #   schema: analytics
  #   table: commerce_events
  #   config:
  #     host: localhost
  #     port: "5432"
  #     user: mako
  #     password: mako

  monitoring:
    metrics:
      enabled: true
      port: 9090
`

// ═══════════════════════════════════════════
// validate — Check pipeline YAML
// ═══════════════════════════════════════════

func cmdValidate(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mako validate <file|dir>")
	}

	path := args[0]
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	// If it's a single file, detect kind first
	if !info.IsDir() {
		kind, err := config.DetectKind(path)
		if err != nil {
			return err
		}

		if kind == "Workflow" {
			return cmdValidateWorkflow(path)
		}
	}

	var specs []*config.SpecResult
	if info.IsDir() {
		loaded, err := config.LoadAll(path)
		if err != nil {
			return err
		}
		for i, s := range loaded {
			specs = append(specs, &config.SpecResult{Spec: s, Path: fmt.Sprintf("%s/[%d]", path, i)})
		}
	} else {
		spec, err := config.Load(path)
		if err != nil {
			return err
		}
		specs = append(specs, &config.SpecResult{Spec: spec, Path: path})
	}

	allValid := true
	for _, sr := range specs {
		result := config.Validate(sr.Spec)
		printValidation(sr.Path, sr.Spec.Pipeline.Name, result)
		if !result.IsValid() {
			allValid = false
		}
	}

	if !allValid {
		return fmt.Errorf("validation failed")
	}

	fmt.Printf("\n✅ All %d pipeline(s) valid\n", len(specs))
	return nil
}

func cmdValidateWorkflow(path string) error {
	spec, err := config.LoadWorkflow(path)
	if err != nil {
		return err
	}

	baseDir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("resolve workflow dir: %w", err)
	}

	result := config.ValidateWorkflow(spec, baseDir)

	fmt.Printf("\n📋 %s (%s)\n", spec.Workflow.Name, path)

	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			fmt.Printf("   ❌ %s: %s\n", e.Field, e.Message)
		}
	}
	if len(result.Warnings) > 0 {
		for _, w := range result.Warnings {
			fmt.Printf("   ⚠️  %s: %s\n", w.Field, w.Message)
		}
	}

	if !result.IsValid() {
		return fmt.Errorf("validation failed")
	}

	fmt.Printf("\n✅ %s — valid (%d steps, DAG OK)\n", path, len(spec.Workflow.Steps))
	return nil
}

func printValidation(path, name string, result *config.ValidationResult) {
	fmt.Printf("\n📋 %s (%s)\n", name, path)

	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			fmt.Printf("   ❌ %s: %s\n", e.Field, e.Message)
		}
	}
	if len(result.Warnings) > 0 {
		for _, w := range result.Warnings {
			fmt.Printf("   ⚠️  %s: %s\n", w.Field, w.Message)
		}
	}
	if result.IsValid() && len(result.Warnings) == 0 {
		fmt.Println("   ✅ Valid")
	}
}

// ═══════════════════════════════════════════
// dry-run — Process sample data locally
// ═══════════════════════════════════════════

func cmdDryRun(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mako dry-run <file> < events.jsonl")
	}

	spec, err := config.Load(args[0])
	if err != nil {
		return err
	}

	// Build transform chain
	chain, err := transform.NewChain(spec.Pipeline.Transforms)
	if err != nil {
		return fmt.Errorf("build transforms: %w", err)
	}

	fmt.Fprintf(os.Stderr, "🔧 Pipeline: %s (%d transforms)\n", spec.Pipeline.Name, chain.Len())
	fmt.Fprintf(os.Stderr, "📥 Reading events from stdin...\n\n")

	// Read events from stdin (JSONL format)
	var buf [64 * 1024]byte
	total, passed, filtered, errors := 0, 0, 0, 0

	scanner := json.NewDecoder(os.Stdin)
	for scanner.More() {
		var event map[string]any
		if err := scanner.Decode(&event); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Parse error: %s\n", err)
			errors++
			continue
		}
		total++

		result, err := chain.Apply(event)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Transform error on event %d: %s\n", total, err)
			errors++
			continue
		}

		if result == nil {
			filtered++
			continue
		}

		// Output transformed event
		out, _ := json.Marshal(result)
		fmt.Println(string(out))
		passed++
	}

	// Use buf to avoid unused variable error
	_ = buf

	fmt.Fprintf(os.Stderr, "\n📊 Results: %d in → %d out, %d filtered, %d errors\n",
		total, passed, filtered, errors)
	return nil
}

// ═══════════════════════════════════════════
// run — Run pipeline locally
// ═══════════════════════════════════════════

func cmdRun(args []string) error {
	configPath := ""
	for i, a := range args {
		if a == "--config" && i+1 < len(args) {
			configPath = args[i+1]
			break
		}
		if !strings.HasPrefix(a, "--") && configPath == "" {
			configPath = a
		}
	}
	if configPath == "" {
		return fmt.Errorf("usage: mako run <file> or mako run --config <file>")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\n⏹️  Shutting down gracefully...\n")
		cancel()
	}()

	status, err := runPipeline(ctx, configPath, nil)
	if err != nil {
		return err
	}

	if status.Errors > 0 {
		return fmt.Errorf("pipeline completed with %d errors", status.Errors)
	}

	return nil
}

// runPipeline executes a single pipeline and returns its final status.
// This is the extracted core of cmdRun, reusable by both `mako run` and `mako workflow`.
//
// sharedObsSrv is an optional shared observability server (used by `mako workflow`).
// When non-nil, pipeline metrics are registered in the server's MetricsRegistry
// instead of spinning up a dedicated HTTP server per pipeline.
// When nil, a per-pipeline server is started on the configured port (original behaviour).
func runPipeline(ctx context.Context, pipelineFile string, sharedObsSrv *observability.Server) (v1.PipelineStatus, error) {
	spec, err := config.Load(pipelineFile)
	if err != nil {
		return v1.PipelineStatus{}, err
	}

	result := config.Validate(spec)
	if !result.IsValid() {
		printValidation(pipelineFile, spec.Pipeline.Name, result)
		return v1.PipelineStatus{}, fmt.Errorf("fix validation errors before running")
	}

	// Build transform chain
	chain, err := transform.NewChain(spec.Pipeline.Transforms)
	if err != nil {
		return v1.PipelineStatus{}, fmt.Errorf("build transforms: %w", err)
	}

	// Build source
	p := spec.Pipeline
	defaultBrokers := p.Source.Brokers
	if defaultBrokers == "" {
		defaultBrokers = os.Getenv("KAFKA_BROKERS")
	}
	if defaultBrokers == "" {
		defaultBrokers = "localhost:9092"
	}

	var src pipeline.Source

	if len(p.Sources) > 0 {
		// Multi-source mode
		namedSources := make(map[string]pipeline.Source, len(p.Sources))
		sourceNames := make([]string, 0, len(p.Sources))

		for _, s := range p.Sources {
			var oneSrc pipeline.Source
			b := s.Brokers
			if b == "" {
				b = defaultBrokers
			}
			switch s.Type {
			case "kafka":
				oneSrc = kafka.NewSource(b, s.Topic, s.ConsumerGroup, s.StartOffset)
			case "http":
				oneSrc = source.NewHTTPSource(s.Config)
			case "postgres_cdc":
				oneSrc = source.NewPostgresCDCSource(s.Config)
			case "duckdb":
				oneSrc = source.NewDuckDBSource(s.Config)
			case "file":
				oneSrc = source.NewFileSource(s.Config)
			default:
				return v1.PipelineStatus{}, fmt.Errorf("unsupported source type %q for source %q", s.Type, s.Name)
			}
			namedSources[s.Name] = oneSrc
			sourceNames = append(sourceNames, s.Name)
		}

		joiner, err := join.New(p.Join, sourceNames)
		if err != nil {
			return v1.PipelineStatus{}, fmt.Errorf("build join engine: %w", err)
		}

		src = source.NewMultiSource(namedSources, sourceNames, joiner)
	} else {
		// Single source mode
		switch p.Source.Type {
		case "kafka":
			src = kafka.NewSource(defaultBrokers, p.Source.Topic, p.Source.ConsumerGroup, p.Source.StartOffset)
		case "file":
			src = source.NewFileSource(p.Source.Config)
		case "postgres_cdc":
			src = source.NewPostgresCDCSource(p.Source.Config)
		case "http":
			src = source.NewHTTPSource(p.Source.Config)
		case "duckdb":
			src = source.NewDuckDBSource(p.Source.Config)
		default:
			return v1.PipelineStatus{}, fmt.Errorf("unsupported source type for run: %s (supported: kafka, file, postgres_cdc, http, duckdb)", p.Source.Type)
		}
	}

	// Auto-tune batch size for bulk sources (file, http, duckdb) when not explicitly configured
	isBulkSource := false
	if len(p.Sources) == 0 {
		switch p.Source.Type {
		case "file", "http", "duckdb":
			isBulkSource = true
		}
	}
	if isBulkSource {
		if p.Sink.Batch == nil {
			p.Sink.Batch = &v1.BatchSpec{}
		}
		if p.Sink.Batch.Size == 0 {
			p.Sink.Batch.Size = 5000
			fmt.Fprintf(os.Stderr, "⚡ Auto-tuned batch size to %d for bulk source (%s)\n", p.Sink.Batch.Size, p.Source.Type)
		}
	}

	// Initialize Vault client if configured
	if p.Vault != nil {
		client, err := sink.InitVaultWithTTL(p.Vault.TTL)
		if err != nil {
			return v1.PipelineStatus{}, fmt.Errorf("vault init: %w", err)
		}
		if client != nil {
			fmt.Fprintf(os.Stderr, "🔐 Vault:    connected (auth: %s, ttl: %s)\n", client.AuthType(), client.TTL())
		}
	} else {
		// Auto-detect Vault from env even without explicit pipeline config
		client, _ := sink.InitVault()
		if client != nil {
			fmt.Fprintf(os.Stderr, "🔐 Vault:    connected via env (auth: %s)\n", client.AuthType())
		}
	}

	// Build sinks
	var sinks []pipeline.Sink
	if p.Sink.Type != "" {
		s, err := sink.BuildFromSpec(p.Sink)
		if err != nil {
			return v1.PipelineStatus{}, fmt.Errorf("build primary sink: %w", err)
		}
		sinks = append(sinks, s)
	}
	for _, sinkSpec := range p.Sinks {
		s, err := sink.BuildFromSpec(sinkSpec)
		if err != nil {
			return v1.PipelineStatus{}, fmt.Errorf("build sink %q: %w", sinkSpec.Name, err)
		}
		sinks = append(sinks, s)
	}
	if len(sinks) == 0 {
		return v1.PipelineStatus{}, fmt.Errorf("no sinks configured")
	}

	// Create and start pipeline
	pipe := pipeline.New(p, src, chain, sinks)

	// Configure DLQ if enabled
	var dlqSink *kafka.Sink
	if p.Isolation.DLQEnabled {
		dlqTopic := ""
		if p.Schema != nil && p.Schema.DLQTopic != "" {
			dlqTopic = p.Schema.DLQTopic
		} else if p.Source.Topic != "" {
			dlqTopic = p.Source.Topic + ".dlq"
		} else {
			dlqTopic = p.Name + ".dlq"
		}
		dlqSink = kafka.NewSink(defaultBrokers, dlqTopic).WithAutoTopicCreation()
		if err := dlqSink.Open(context.Background()); err != nil {
			return v1.PipelineStatus{}, fmt.Errorf("open DLQ sink %s: %w", dlqTopic, err)
		}
		pipe.SetDLQ(dlqSink)
		fmt.Fprintf(os.Stderr, "🗑️  DLQ:      %s\n", dlqTopic)
	}

	// Configure Schema Registry validation if enabled
	if p.Schema != nil && p.Schema.Enforce {
		registryURL := p.Schema.Registry
		if registryURL == "" {
			registryURL = os.Getenv("SCHEMA_REGISTRY_URL")
		}
		if registryURL == "" {
			registryURL = "http://localhost:8081"
		}

		// Resolve environment variables in registry URL
		registryURL = os.ExpandEnv(registryURL)

		subject := p.Schema.Subject
		if subject == "" {
			subject = p.Source.Topic + "-value"
		}

		validator := schema.NewValidator(registryURL, subject, true, p.Schema.OnFailure)
		pipe.SetSchemaValidator(validator)
		fmt.Fprintf(os.Stderr, "📐 Schema:   %s (subject: %s)\n", registryURL, subject)
	}

	// Start observability server (metrics + health + status)
	metricsPort := 9090
	if p.Monitoring != nil && p.Monitoring.Metrics != nil && p.Monitoring.Metrics.Port > 0 {
		metricsPort = p.Monitoring.Metrics.Port
	}
	metricsEnabled := true
	if p.Monitoring != nil && p.Monitoring.Metrics != nil {
		metricsEnabled = p.Monitoring.Metrics.Enabled
	}

	var obsSrv *observability.Server
	var registryMetrics *observability.PipelineMetrics // non-nil when using shared registry
	if sharedObsSrv != nil && sharedObsSrv.Registry() != nil {
		// Workflow mode: register this pipeline in the shared registry.
		// No per-pipeline HTTP server is started.
		registryMetrics = sharedObsSrv.Registry().Register(p.Name)
		obsSrv = nil // no per-pipeline server
	} else if metricsEnabled {
		obsSrv = observability.NewServer(fmt.Sprintf(":%d", metricsPort), p.Name)
		obsSrv.SetStatusFn(func() map[string]any {
			st := pipe.Status()
			return map[string]any{
				"state":     st.State,
				"source":    map[string]any{"connected": st.Source.Connected, "lag": st.Source.Lag},
				"events_in": st.EventsIn, "events_out": st.EventsOut, "errors": st.Errors,
			}
		})
		if err := obsSrv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Metrics server failed: %v\n", err)
		}
	}

	fmt.Fprintf(os.Stderr, "🦈 Mako Pipeline Runner v%s\n", cli.Version)
	fmt.Fprintf(os.Stderr, "📋 Pipeline: %s\n", p.Name)
	if len(p.Sources) > 0 {
		fmt.Fprintf(os.Stderr, "📥 Sources:  %d configured\n", len(p.Sources))
		for _, s := range p.Sources {
			fmt.Fprintf(os.Stderr, "   ├─ %s (%s)\n", s.Name, s.Type)
		}
		if p.Join != nil {
			fmt.Fprintf(os.Stderr, "🔗 Join:     %s on %s\n", p.Join.Type, p.Join.On)
			if p.Join.Window != "" {
				fmt.Fprintf(os.Stderr, "   └─ window: %s\n", p.Join.Window)
			}
		}
	} else {
		fmt.Fprintf(os.Stderr, "📥 Source:    %s (%s)\n", p.Source.Type, p.Source.Topic)
	}
	fmt.Fprintf(os.Stderr, "🔧 Transforms: %d steps\n", chain.Len())
	fmt.Fprintf(os.Stderr, "📤 Sinks:    %d configured\n", len(sinks))
	if obsSrv != nil {
		fmt.Fprintf(os.Stderr, "📊 Metrics:  http://localhost:%d/metrics\n", metricsPort)
		fmt.Fprintf(os.Stderr, "💚 Health:   http://localhost:%d/health\n", metricsPort)
		fmt.Fprintf(os.Stderr, "📋 Status:   http://localhost:%d/status\n", metricsPort)
	}
	// Create Slack alerter (nil-safe if not configured)
	slackAlerter := alerting.NewSlackAlerter(spec)
	if slackAlerter != nil {
		fmt.Fprintf(os.Stderr, "🔔 Slack:    alerts → %s\n", slackAlerter.Channel)
	}

	// Create rule engine for threshold-based alerts (nil-safe if no rules)
	var ruleEngine *alerting.RuleEngine
	if p.Monitoring != nil && len(p.Monitoring.Alerts) > 0 {
		ruleEngine = alerting.NewRuleEngine(p.Monitoring.Alerts, slackAlerter, p.Name)
		if ruleEngine != nil {
			fmt.Fprintf(os.Stderr, "📏 Rules:    %d alert rule(s) configured\n", len(p.Monitoring.Alerts))
		}
	}

	fmt.Fprintf(os.Stderr, "🚀 Starting pipeline...\n\n")

	startTime := time.Now()

	if err := pipe.Start(ctx); err != nil {
		slackAlerter.SendError(ctx, err, 0, 0)
		return v1.PipelineStatus{}, fmt.Errorf("start pipeline: %w", err)
	}

	// syncMetrics copies pipeline counters to the observability server
	// (per-pipeline mode) or shared registry (workflow mode).
	syncMetrics := func() {
		var target *observability.PipelineMetrics
		if registryMetrics != nil {
			target = registryMetrics
		} else if obsSrv != nil {
			target = obsSrv.Metrics()
		} else {
			return
		}
		target.EventsIn.Store(pipe.MetricsEventsIn().Load())
		target.EventsOut.Store(pipe.MetricsEventsOut().Load())
		target.Errors.Store(pipe.MetricsErrors().Load())
		target.DLQCount.Store(pipe.MetricsDLQCount().Load())
		target.SchemaFails.Store(pipe.MetricsSchemaFails().Load())
		target.SetSinkLatency(pipe.MetricsSinkLatency().Load())
	}

	// Parse freshnessSLA for SLA breach detection
	var freshnessSLA time.Duration
	if p.Monitoring != nil && p.Monitoring.FreshnessSLA != "" {
		freshnessSLA, _ = config.ParseDuration(p.Monitoring.FreshnessSLA)
	}

	// Sync metrics from pipeline counters to observability server
	var syncDone chan struct{}
	if obsSrv != nil {
		obsSrv.SetReady(true)
	}
	syncDone = make(chan struct{})
	// Sync pipeline counters to observability server periodically.
	// Also checks SLA breach and error counts for alerting.
	go func() {
		defer close(syncDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		var slaBreach bool
		var lastErrorCount int64
		var lastRuleEval time.Time
		var prevEventsIn int64
		for {
			select {
			case <-pipe.Done():
				syncMetrics()
				return
			case <-ctx.Done():
				syncMetrics()
				return
			case <-ticker.C:
				syncMetrics()

				// SLA breach detection
				if freshnessSLA > 0 && !slaBreach {
					st := pipe.Status()
					if st.LastEvent != nil {
						delay := time.Since(*st.LastEvent)
						if delay > freshnessSLA {
							slaBreach = true
							slackAlerter.SendSLABreach(ctx, freshnessSLA, delay)
						}
					}
				}

				// Error alert (send once per new error batch)
				errCount := pipe.MetricsErrors().Load()
				if errCount > lastErrorCount {
					slackAlerter.SendError(ctx,
						fmt.Errorf("%d new error(s) during pipeline execution", errCount-lastErrorCount),
						pipe.MetricsEventsIn().Load(),
						pipe.MetricsEventsOut().Load(),
					)
					lastErrorCount = errCount
				}

				// Alert rules evaluation (every 10s, not every 500ms)
				if ruleEngine != nil && time.Since(lastRuleEval) >= 10*time.Second {
					var lastEventAge time.Duration
					st := pipe.Status()
					if st.LastEvent != nil {
						lastEventAge = time.Since(*st.LastEvent)
					}
					snap := alerting.MetricsSnapshot{
						EventsIn:     pipe.MetricsEventsIn().Load(),
						EventsOut:    pipe.MetricsEventsOut().Load(),
						Errors:       pipe.MetricsErrors().Load(),
						LastEventAge: lastEventAge,
						PrevEventsIn: prevEventsIn,
						EvalInterval: time.Since(lastRuleEval),
					}
					ruleEngine.Evaluate(ctx, snap)
					prevEventsIn = snap.EventsIn
					lastRuleEval = time.Now()
				}
			}
		}
	}()

	fmt.Fprintf(os.Stderr, "✅ Pipeline running. Press Ctrl+C to stop.\n")

	// Wait for context cancellation OR pipeline completion (e.g. file source EOF)
	select {
	case <-ctx.Done():
		// context was cancelled (SIGINT in cmdRun, or workflow engine cancellation)
	case <-pipe.Done():
		fmt.Fprintf(os.Stderr, "\n📄 Source exhausted. Shutting down...\n")
	}

	if err := pipe.Stop(); err != nil {
		return v1.PipelineStatus{}, fmt.Errorf("stop pipeline: %w", err)
	}

	// Close the DLQ sink if it was opened
	if dlqSink != nil {
		dlqSink.Close()
	}

	// Wait for the sync goroutine to finish its final copy
	<-syncDone

	if obsSrv != nil {
		// Final sync after pipeline.Stop() to capture any events flushed during shutdown
		syncMetrics()
		obsSrv.SetReady(false)
		obsSrv.Stop()
	} else if registryMetrics != nil {
		// Final sync for shared registry mode
		syncMetrics()
	}

	status := pipe.Status()
	duration := time.Since(startTime)
	fmt.Fprintf(os.Stderr, "📊 Final stats: %d in, %d out, %d errors\n",
		status.EventsIn, status.EventsOut, status.Errors)

	// Send completion alert
	slackAlerter.SendComplete(ctx, status.EventsIn, status.EventsOut, status.Errors, duration)

	fmt.Fprintf(os.Stderr, "✅ Pipeline stopped.\n")

	// Small delay to let async Slack messages fire before process exits
	if slackAlerter != nil {
		time.Sleep(200 * time.Millisecond)
	}

	return status, nil
}

// ═══════════════════════════════════════════
// workflow — Run a multi-pipeline workflow (DAG)
// ═══════════════════════════════════════════

func cmdWorkflow(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mako workflow <file>")
	}

	workflowFile := args[0]

	// Detect kind to provide a helpful error if it's a pipeline file
	kind, err := config.DetectKind(workflowFile)
	if err != nil {
		return err
	}
	if kind != "Workflow" {
		return fmt.Errorf("%s is kind %q, expected Workflow (use 'mako run' for pipelines)", workflowFile, kind)
	}

	// Load and validate the workflow spec
	spec, err := config.LoadWorkflow(workflowFile)
	if err != nil {
		return err
	}

	baseDir, err := filepath.Abs(filepath.Dir(workflowFile))
	if err != nil {
		return fmt.Errorf("resolve workflow dir: %w", err)
	}

	result := config.ValidateWorkflow(spec, baseDir)
	if !result.IsValid() {
		fmt.Fprintf(os.Stderr, "\n📋 %s (%s)\n", spec.Workflow.Name, workflowFile)
		for _, e := range result.Errors {
			fmt.Fprintf(os.Stderr, "   ❌ %s: %s\n", e.Field, e.Message)
		}
		return fmt.Errorf("workflow validation failed")
	}

	// Set up context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Fprintf(os.Stderr, "\n⏹️  Workflow interrupted. Stopping running pipelines...\n")
		cancel()
	}()

	// Start a single shared observability server for all workflow pipelines.
	// All pipeline metrics are exposed on one port via the MetricsRegistry.
	metricsRegistry := observability.NewRegistry()
	sharedSrv := observability.NewRegistryServer(":9090", metricsRegistry)
	if err := sharedSrv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "⚠️  Shared metrics server failed: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "📊 Metrics:  http://localhost:9090/metrics (all pipelines)\n")
	}

	// Create and run the workflow engine.
	// Wrap runPipeline to inject the shared observability server.
	engine := workflow.New(spec, baseDir, func(ctx context.Context, pipelineFile string) (v1.PipelineStatus, error) {
		return runPipeline(ctx, pipelineFile, sharedSrv)
	})
	status := engine.Run(ctx)

	// Stop the shared metrics server after the workflow completes
	sharedSrv.Stop()

	// Return non-zero exit code if any step failed
	if status.State != v1.StateCompleted {
		return fmt.Errorf("workflow %q failed", spec.Workflow.Name)
	}

	return nil
}
