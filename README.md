<p align="center">
  <img src="img/logo.png" alt="Mako" width="280" />
</p>

<h1 align="center">Mako</h1>

<p align="center">
  <strong>Declarative real-time data pipelines. YAML in, events out.</strong>
</p>

<p align="center">
  <em>Named after the shortfin mako — fastest shark in the ocean. Your data deserves the same speed.</em>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#sources">Sources</a> &middot;
  <a href="#transforms">Transforms</a> &middot;
  <a href="#sinks">Sinks</a> &middot;
  <a href="#observability">Observability</a> &middot;
  <a href="#local-infrastructure">Local Infra</a> &middot;
  <a href="#codegen">Codegen</a> &middot;
  <a href="#ci--testing">CI</a>
</p>

---

```yaml
pipeline:
  name: order-events
  source:
    type: kafka
    topic: events.orders
  transforms:
    - name: pii_mask
      type: hash_fields
      fields: [email, phone, ssn]
    - name: filter_prod
      type: filter
      condition: "environment = production"
  sink:
    type: snowflake
    database: ANALYTICS
    schema: RAW
    table: ORDER_EVENTS
  monitoring:
    freshnessSLA: 5m
    metrics:
      enabled: true
      port: 9090
```

```bash
mako validate pipeline.yaml
mako dry-run pipeline.yaml < events.jsonl
mako run pipeline.yaml
mako generate pipeline.yaml --k8s > deploy.yaml
mako generate pipeline.yaml --tf > infra.tf
```

---

## Why Mako?

**The problem:** Every team builds Kafka-to-warehouse pipelines differently. No schema enforcement, no self-service, no isolation. One bad event breaks everything.

**Mako** solves this with a declarative approach:

| Concept | Mako |
|---|---|
| Sources | Kafka (franz-go), File (JSONL, CSV, JSON) |
| Isolation | Per-pipeline Go workers |
| Transforms | Declarative: hash, mask, filter, rename, dedupe, SQL, aggregate |
| Sinks | PostgreSQL (pgx), Snowflake (gosnowflake), BigQuery (streaming), Kafka, Stdout, File |
| Schema | Confluent Schema Registry (JSON Schema validation) |
| Observability | Prometheus /metrics, /health, /ready, /status |
| Codegen | `mako generate --k8s` + `--tf` |
| Fault tolerance | DLQ + retries + exponential backoff |
| CI | GitHub Actions with integration tests (Kafka, PG, Schema Registry) |

---

## Quick Start

```bash
# Clone and build
git clone https://github.com/Stefen-Taime/mako.git
cd mako
go build -o bin/mako .

# Create your first pipeline
./bin/mako init

# Validate
./bin/mako validate pipeline.yaml

# Test locally with sample data
echo '{"email":"john@test.com","amount":99.99,"status":"completed","environment":"production"}' | \
  ./bin/mako dry-run pipeline.yaml

# Run pipeline (Kafka -> transforms -> sink)
./bin/mako run pipeline.yaml

# Generate Kubernetes manifests
./bin/mako generate pipeline.yaml --k8s > deploy.yaml

# Generate Terraform (Kafka topics, Snowflake tables, Schema Registry)
./bin/mako generate pipeline.yaml --tf > infra.tf
```

**Output of dry-run:**
```json
{"_pii_processed":true,"amount":99.99,"email":"243b73234c6433b8","environment":"production","status":"completed"}
```

The email is hashed (PII compliance), the event passes the production filter, and it's ready for the warehouse.

---

## Sources

### Kafka (default)

Real-time consumer using [franz-go](https://github.com/twmb/franz-go) (pure Go, zero CGO).

```yaml
source:
  type: kafka
  topic: events.orders
  brokers: localhost:9092
  consumerGroup: mako-order-events
  startOffset: earliest    # earliest | latest
```

Features:
- Consumer group with manual offset commit
- Automatic JSON parsing (non-JSON wrapped as `_raw`)
- Header propagation
- Graceful shutdown with offset commit

### File (JSONL, CSV, JSON)

Read events from local files. Useful for backfill, testing, and batch processing.

```yaml
source:
  type: file
  config:
    path: /data/events.jsonl           # single file
    # path: /data/events/*.jsonl       # glob pattern supported
    format: jsonl                       # jsonl | csv | json (auto-detected)
    csv_header: true                    # first line is header (CSV)
    csv_delimiter: ","                  # field separator (CSV)
```

Supported formats:
- **JSONL** (`.jsonl`, `.ndjson`): one JSON object per line
- **CSV** (`.csv`): with optional header row, configurable delimiter
- **JSON** (`.json`): single object or array of objects

---

## Transforms

Write YAML, not code. Each transform is a pure function chained in sequence.

### Data Governance

```yaml
transforms:
  # SHA-256 hash with salt (PII compliance)
  - name: pii_hash
    type: hash_fields
    fields: [email, phone, ssn, credit_card_number]

  # Partial masking: john@email.com -> j***@email.com
  - name: mask_display
    type: mask_fields
    fields: [email_display, card_display]

  # Remove internal/debug fields
  - name: cleanup
    type: drop_fields
    fields: [internal_trace_id, debug_payload]
```

### Filtering & Enrichment

```yaml
transforms:
  # SQL WHERE-style filter (supports AND, OR, IS NULL, IS NOT NULL)
  - name: prod_only
    type: filter
    condition: "environment = production AND status != test"

  # Rename fields for warehouse convention
  - name: standardize
    type: rename_fields
    mapping:
      amt: amount
      ts: event_timestamp
      cust_id: customer_id

  # Deduplication by key
  - name: dedupe
    type: deduplicate
    fields: [event_id]
```

---

## Sinks

### PostgreSQL (pgx)

High-performance sink using [pgx](https://github.com/jackc/pgx) with connection pooling and COPY protocol.

```yaml
sink:
  type: postgres
  database: mako
  schema: analytics
  table: pipeline_events
  config:
    host: localhost
    port: "5432"
    user: mako
    password: mako
    # Or use a full DSN:
    # dsn: postgres://mako:mako@localhost:5432/mako
```

Features: `pgxpool` connection pool, bulk `COPY FROM` with batch INSERT fallback, automatic JSONB serialization.

### Snowflake (gosnowflake)

Production sink using the official [gosnowflake](https://github.com/snowflakedb/gosnowflake) driver.

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: ORDER_EVENTS
  config:
    account: xy12345.us-east-1
    user: MAKO_USER
    password: ${SNOWFLAKE_PASSWORD}
    warehouse: COMPUTE_WH
    role: MAKO_ROLE
    # Or use a full DSN:
    # dsn: user:password@account/database/schema?warehouse=WH
```

Events stored as `VARIANT` (JSON) via `PARSE_JSON()`. Target table schema:

```sql
CREATE TABLE ORDER_EVENTS (
    event_data    VARIANT NOT NULL,
    event_key     VARCHAR,
    event_topic   VARCHAR,
    event_offset  NUMBER,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### BigQuery (streaming inserter)

Streaming sink using `cloud.google.com/go/bigquery` with deduplication.

```yaml
sink:
  type: bigquery
  schema: raw_events        # BigQuery dataset
  table: events
  config:
    project: my-gcp-project
```

Authentication via Application Default Credentials (ADC). Set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login`.

Features: streaming inserter with per-row `InsertID` dedup, `PutMultiError` handling, JSON-column alternative mode.

### Multi-Sink

Send events to multiple destinations simultaneously:

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: ORDER_EVENTS

sinks:
  - name: bigquery-mirror
    type: bigquery
    schema: raw_events
    table: events
    config:
      project: my-project

  - name: downstream
    type: kafka
    topic: events.orders.enriched
```

**All sinks:** stdout, file, PostgreSQL, Snowflake, BigQuery, Kafka.

---

## Schema Enforcement

Validate events against Confluent Schema Registry at runtime.

```yaml
schema:
  enforce: true
  registry: http://schema-registry:8081
  subject: events.orders-value
  compatibility: BACKWARD
  onFailure: reject     # reject | dlq | log
  dlqTopic: events.orders.dlq
```

Features:
- JSON Schema validation with type checking
- Required fields enforcement
- Schema caching (fetched once, refreshable)
- Failed events routed to DLQ or rejected

---

## Fault Isolation

Each pipeline runs independently. Failed events go to a Dead Letter Queue instead of blocking the pipeline.

```yaml
isolation:
  strategy: per_event_type
  maxRetries: 3
  backoffMs: 1000
  dlqEnabled: true
```

Retry flow: sink write failure → exponential backoff → retry (up to N times) → DLQ → degrade state.

---

## Observability

When running `mako run`, an HTTP server exposes Prometheus metrics, health probes, and pipeline status.

```yaml
monitoring:
  freshnessSLA: 5m
  alertChannel: "#data-alerts"
  metrics:
    enabled: true
    port: 9090
```

### Endpoints

| Endpoint | Description | Use |
|----------|-------------|-----|
| `GET /metrics` | Prometheus text format | Scraping by Prometheus/Grafana |
| `GET /health` | Liveness probe (always 200) | Kubernetes `livenessProbe` |
| `GET /ready` | Readiness probe (200 when pipeline running) | Kubernetes `readinessProbe` |
| `GET /status` | Pipeline status JSON | Monitoring dashboards |

### Prometheus Metrics

```
mako_events_in_total{pipeline="order-events"} 15234
mako_events_out_total{pipeline="order-events"} 15230
mako_errors_total{pipeline="order-events"} 4
mako_dlq_total{pipeline="order-events"} 2
mako_schema_failures_total{pipeline="order-events"} 1
mako_throughput_events_per_second{pipeline="order-events"} 1523.40
mako_uptime_seconds{pipeline="order-events"} 3600.0
mako_pipeline_ready{pipeline="order-events"} 1
```

### Kubernetes Probes

```yaml
# In generated K8s manifest (mako generate --k8s)
livenessProbe:
  httpGet:
    path: /health
    port: 9090
readinessProbe:
  httpGet:
    path: /ready
    port: 9090
```

---

## Codegen

Generate deployment artifacts from your YAML:

```bash
# Kubernetes manifests (Deployment, Service, ConfigMap, HPA)
mako generate pipeline.yaml --k8s

# Terraform (Kafka topics, Snowflake tables/pipes, Schema Registry, DLQ)
mako generate pipeline.yaml --tf
```

---

## Local Infrastructure

The `docker/` directory contains a full local stack for development:

```bash
cd docker/
docker compose up -d           # Start all services
./create-topics.sh             # Create Kafka topics
./produce-sample.sh            # Produce test events
```

| Service | Port | Description |
|---|---|---|
| Kafka (KRaft) | `localhost:9092` | Event broker |
| Schema Registry | `localhost:8081` | Confluent Schema Registry |
| PostgreSQL | `localhost:5432` | Sink database (user: `mako`, pass: `mako`) |
| Flink SQL | `localhost:8082` | Stream processing dashboard |
| Kafka UI | `localhost:8080` | Web UI for topics and messages |

---

## CI / Testing

GitHub Actions runs on every push/PR:

**Unit tests** (fast, no Docker):
- 25+ tests covering config, validation, transforms, codegen
- Benchmarks for transform chain performance
- Example validation + dry-run

**Integration tests** (Docker services):
- Kafka (KRaft) + PostgreSQL + Schema Registry
- Full pipeline: produce messages → consume → transform → write to PG
- HTTP endpoint verification (/metrics, /health, /ready, /status)
- File source validation

```bash
# Run locally
go test -v -count=1 ./...
go test -bench=. -benchmem ./...
```

---

## Architecture

```
pipeline.yaml
       |
       v
  +----------+
  |   mako   |  CLI: validate, generate, dry-run, run
  |   (Go)   |
  +----+-----+
       |
       +---> Kubernetes manifests (Deployment, HPA, ConfigMap)
       +---> Terraform HCL (Kafka topics, warehouse tables)
       |
       v
  +------------------------------------------+
  |  mako-runner (per-pipeline container)    |
  |                                          |
  |  Source ──> Transform Chain ──> Sink(s)  |
  |  (Kafka)    hash_fields        Postgres  |
  |  (File)     mask_fields        Snowflake |
  |             filter             BigQuery  |
  |             rename             Kafka     |
  |             deduplicate        Stdout    |
  |                                          |
  |  Schema Registry ──> Validate            |
  |  Prometheus    ──> /metrics              |
  |  Health        ──> /health, /ready       |
  |  DLQ + Retries + Backoff                 |
  +------------------------------------------+
```

---

## Project Structure

```
mako/
├── api/v1/types.go                 # Pipeline spec (the YAML DSL model)
├── pkg/
│   ├── config/config.go            # YAML parser + validator
│   ├── pipeline/engine.go          # Runtime: Source -> Transforms -> Sink
│   ├── transform/transform.go      # Transform implementations
│   ├── source/file.go              # File source (JSONL, CSV, JSON)
│   ├── sink/
│   │   ├── sink.go                 # Stdout, File sinks + BuildFromSpec
│   │   ├── postgres.go             # PostgreSQL sink (pgx)
│   │   ├── snowflake.go            # Snowflake sink (gosnowflake)
│   │   └── bigquery.go             # BigQuery sink (streaming inserter)
│   ├── kafka/kafka.go              # Kafka source + sink (franz-go)
│   ├── schema/registry.go          # Schema Registry client + validator
│   ├── observability/server.go     # Prometheus metrics + health + status HTTP
│   └── codegen/codegen.go          # K8s + Terraform generators
├── internal/cli/cli.go             # CLI helpers
├── main.go                         # CLI entry point
├── main_test.go                    # 25+ tests + benchmarks
├── docker/                         # Local infra (Kafka, PostgreSQL, Flink, Schema Registry)
├── .github/workflows/ci.yml        # CI: unit + integration tests
├── Dockerfile                      # Production image
├── Dockerfile.runner               # Per-pipeline runner image
├── examples/
│   ├── simple/pipeline.yaml
│   └── advanced/pipeline.yaml
└── test/fixtures/events.jsonl
```

---

## Roadmap

- [x] Kafka consumer/producer (franz-go)
- [x] PostgreSQL sink (pgx + COPY)
- [x] Snowflake sink (gosnowflake)
- [x] BigQuery sink (streaming inserter)
- [x] Schema Registry validation (JSON Schema)
- [x] File source (JSONL, CSV, JSON)
- [x] Prometheus metrics (/metrics)
- [x] Health/readiness probes (/health, /ready)
- [x] Pipeline status API (/status)
- [x] CI with integration tests (Kafka + PG + Schema Registry)
- [ ] S3/GCS object storage sink
- [ ] ClickHouse sink
- [ ] Helm chart
- [ ] Grafana dashboard templates
- [ ] WASM plugin transforms

---

## License

MIT

---

*Built by [Stefen Taime](https://github.com/Stefen-Taime)*
