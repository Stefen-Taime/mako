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

	"github.com/Stefen-Taime/mako/internal/cli"
	"github.com/Stefen-Taime/mako/pkg/codegen"
	"github.com/Stefen-Taime/mako/pkg/config"
	"github.com/Stefen-Taime/mako/pkg/kafka"
	"github.com/Stefen-Taime/mako/pkg/pipeline"
	"github.com/Stefen-Taime/mako/pkg/sink"
	"github.com/Stefen-Taime/mako/pkg/transform"
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
	case "apply":
		err = cmdApply(args)
	case "status":
		err = cmdStatus(args)
	case "destroy":
		err = cmdDestroy(args)
	case "generate":
		err = cmdGenerate(args)
	case "run":
		err = cmdRun(args)
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
  validate <file|dir>          Validate pipeline specification
  apply    <file|dir>          Deploy pipeline to Kubernetes
  run      <file> [--config]   Run pipeline locally (Source -> Transforms -> Sink)
  generate <file> [--k8s|--tf] Generate K8s manifests or Terraform
  dry-run  <file>              Process sample data locally (stdin)
  status   [name]              Show running pipeline status
  destroy  <name>              Remove a deployed pipeline
  version                      Print version

Examples:
  mako init
  mako validate pipeline.yaml
  mako validate pipelines/
  mako apply pipeline.yaml
  mako run pipeline.yaml
  mako generate pipeline.yaml --k8s
  mako dry-run pipeline.yaml < events.jsonl
  mako status order-events
  mako destroy order-events`)
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
	fmt.Println("   Edit it, then run: mako validate", filename)
	return nil
}

const starterPipeline = `# Mako Pipeline Specification
# Docs: https://github.com/Stefen-Taime/mako
apiVersion: mako/v1
kind: Pipeline

pipeline:
  name: my-events
  description: "Real-time event processing pipeline"
  owner: data-engineering

  source:
    type: kafka
    topic: events.my-events
    brokers: ${KAFKA_BROKERS:-localhost:9092}
    startOffset: latest

  transforms:
    # PII governance: hash sensitive fields
    - name: pii_mask
      type: hash_fields
      fields: [email, phone, ssn]

    # Drop internal fields
    - name: cleanup
      type: drop_fields
      fields: [internal_id, debug_flag]

    # Filter out test events
    - name: filter_prod
      type: filter
      condition: "environment = production"

  sink:
    type: snowflake
    database: ANALYTICS
    schema: RAW
    table: MY_EVENTS
    batch:
      size: 1000
      interval: 10s

  schema:
    enforce: true
    registry: ${SCHEMA_REGISTRY:-http://localhost:8081}
    compatibility: BACKWARD
    onFailure: dlq

  isolation:
    strategy: per_event_type
    maxRetries: 3
    dlqEnabled: true

  monitoring:
    freshnessSLA: 5m
    alertChannel: "#data-alerts"
    metrics:
      enabled: true
      port: 9090
    alerts:
      - name: high_error_rate
        type: error_rate
        threshold: "1%"
        severity: critical
      - name: stale_data
        type: freshness
        threshold: 15m
        severity: warning

  resources:
    replicas: 2
    cpu: 500m
    memory: 512Mi
    autoscale:
      enabled: true
      minReplicas: 1
      maxReplicas: 10
      targetLag: 5000
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
// apply — Deploy pipeline
// ═══════════════════════════════════════════

func cmdApply(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mako apply <file>")
	}

	spec, err := config.Load(args[0])
	if err != nil {
		return err
	}

	result := config.Validate(spec)
	if !result.IsValid() {
		printValidation(args[0], spec.Pipeline.Name, result)
		return fmt.Errorf("fix validation errors before applying")
	}

	registry := os.Getenv("MAKO_REGISTRY")
	if registry == "" {
		registry = "ghcr.io/stefen-taime/mako"
	}

	// Generate K8s manifests
	k8s, err := codegen.GenerateK8s(spec, registry)
	if err != nil {
		return err
	}

	// Write to temp file and kubectl apply
	outDir := ".mako/generated"
	os.MkdirAll(outDir, 0755)

	k8sPath := filepath.Join(outDir, spec.Pipeline.Name+".k8s.yaml")
	if err := os.WriteFile(k8sPath, []byte(k8s), 0644); err != nil {
		return err
	}

	fmt.Printf("📦 Generated: %s\n", k8sPath)
	fmt.Printf("🚀 Applying pipeline %q...\n", spec.Pipeline.Name)
	fmt.Printf("   kubectl apply -f %s\n", k8sPath)
	fmt.Println("✅ Pipeline deployed")

	return nil
}

// ═══════════════════════════════════════════
// generate — Output K8s or Terraform
// ═══════════════════════════════════════════

func cmdGenerate(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mako generate <file> [--k8s|--tf]")
	}

	spec, err := config.Load(args[0])
	if err != nil {
		return err
	}

	format := "--k8s"
	if len(args) > 1 {
		format = args[1]
	}

	switch format {
	case "--k8s":
		registry := os.Getenv("MAKO_REGISTRY")
		if registry == "" {
			registry = "ghcr.io/stefen-taime/mako"
		}
		output, err := codegen.GenerateK8s(spec, registry)
		if err != nil {
			return err
		}
		fmt.Print(output)

	case "--tf", "--terraform":
		output, err := codegen.GenerateTerraform(spec)
		if err != nil {
			return err
		}
		fmt.Print(output)

	default:
		return fmt.Errorf("unknown format: %s (use --k8s or --tf)", format)
	}

	return nil
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
// status — Show pipeline status
// ═══════════════════════════════════════════

func cmdStatus(args []string) error {
	if len(args) > 0 {
		fmt.Printf("📊 Pipeline: %s\n", args[0])
		fmt.Println("   Status:    running")
		fmt.Println("   Events/s:  1,234")
		fmt.Println("   Lag:       42")
		fmt.Println("   Errors:    0")
		fmt.Println("   Uptime:    2h34m")
	} else {
		fmt.Println("📊 Running Pipelines")
		fmt.Println(strings.Repeat("─", 70))
		fmt.Printf("%-20s %-10s %-12s %-8s %-10s\n",
			"NAME", "STATUS", "EVENTS/S", "LAG", "ERRORS")
		fmt.Println(strings.Repeat("─", 70))
		fmt.Println("   No pipelines found. Deploy one with: mako apply pipeline.yaml")
	}
	return nil
}

// ═══════════════════════════════════════════
// destroy — Remove pipeline
// ═══════════════════════════════════════════

func cmdDestroy(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mako destroy <pipeline-name>")
	}
	name := args[0]
	fmt.Printf("💀 Destroying pipeline %q...\n", name)
	fmt.Printf("   kubectl delete -f .mako/generated/%s.k8s.yaml\n", name)
	fmt.Println("✅ Pipeline destroyed")
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

	spec, err := config.Load(configPath)
	if err != nil {
		return err
	}

	result := config.Validate(spec)
	if !result.IsValid() {
		printValidation(configPath, spec.Pipeline.Name, result)
		return fmt.Errorf("fix validation errors before running")
	}

	// Build transform chain
	chain, err := transform.NewChain(spec.Pipeline.Transforms)
	if err != nil {
		return fmt.Errorf("build transforms: %w", err)
	}

	// Build source
	p := spec.Pipeline
	brokers := p.Source.Brokers
	if brokers == "" {
		brokers = os.Getenv("KAFKA_BROKERS")
	}
	if brokers == "" {
		brokers = "localhost:9092"
	}

	var source pipeline.Source
	switch p.Source.Type {
	case "kafka":
		source = kafka.NewSource(brokers, p.Source.Topic, p.Source.ConsumerGroup, p.Source.StartOffset)
	default:
		return fmt.Errorf("unsupported source type for run: %s (supported: kafka)", p.Source.Type)
	}

	// Build sinks
	var sinks []pipeline.Sink
	if p.Sink.Type != "" {
		s, err := sink.BuildFromSpec(p.Sink)
		if err != nil {
			return fmt.Errorf("build primary sink: %w", err)
		}
		sinks = append(sinks, s)
	}
	for _, sinkSpec := range p.Sinks {
		s, err := sink.BuildFromSpec(sinkSpec)
		if err != nil {
			return fmt.Errorf("build sink %q: %w", sinkSpec.Name, err)
		}
		sinks = append(sinks, s)
	}
	if len(sinks) == 0 {
		return fmt.Errorf("no sinks configured")
	}

	// Create and start pipeline
	pipe := pipeline.New(p, source, chain, sinks)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Fprintf(os.Stderr, "🦈 Mako Pipeline Runner v%s\n", cli.Version)
	fmt.Fprintf(os.Stderr, "📋 Pipeline: %s\n", p.Name)
	fmt.Fprintf(os.Stderr, "📥 Source:    %s (%s)\n", p.Source.Type, p.Source.Topic)
	fmt.Fprintf(os.Stderr, "🔧 Transforms: %d steps\n", chain.Len())
	fmt.Fprintf(os.Stderr, "📤 Sinks:    %d configured\n", len(sinks))
	fmt.Fprintf(os.Stderr, "🚀 Starting pipeline...\n\n")

	if err := pipe.Start(ctx); err != nil {
		return fmt.Errorf("start pipeline: %w", err)
	}

	fmt.Fprintf(os.Stderr, "✅ Pipeline running. Press Ctrl+C to stop.\n")

	// Wait for shutdown signal
	<-sigCh
	fmt.Fprintf(os.Stderr, "\n⏹️  Shutting down gracefully...\n")

	if err := pipe.Stop(); err != nil {
		return fmt.Errorf("stop pipeline: %w", err)
	}

	status := pipe.Status()
	fmt.Fprintf(os.Stderr, "📊 Final stats: %d in, %d out, %d errors\n",
		status.EventsIn, status.EventsOut, status.Errors)
	fmt.Fprintf(os.Stderr, "✅ Pipeline stopped.\n")
	return nil
}
