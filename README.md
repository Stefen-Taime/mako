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
  <a href="#transforms">Transforms</a> &middot;
  <a href="#local-infrastructure">Local Infra</a> &middot;
  <a href="#codegen">Codegen</a> &middot;
  <a href="#roadmap">Roadmap</a>
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
    alertChannel: "#data-alerts"
```

```bash
mako validate pipeline.yaml
mako dry-run pipeline.yaml < events.jsonl
mako generate pipeline.yaml --k8s > deploy.yaml
mako generate pipeline.yaml --tf > infra.tf
```

---

## Why Mako?

**The problem:** Every team builds Kafka-to-warehouse pipelines differently. No schema enforcement, no self-service, no isolation. One bad event breaks everything.

**Mako** solves this with a declarative approach:

| Concept | Mako |
|---|---|
| Source | YAML-defined Kafka sources |
| Isolation | Per-pipeline Go workers |
| Transforms | Declarative: hash, mask, filter, rename, dedupe, SQL, aggregate |
| Sinks | Stdout, File, Snowflake, BigQuery, Kafka, PostgreSQL |
| Codegen | `mako generate --k8s` + `--tf` |
| Fault tolerance | DLQ + retries + exponential backoff |

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

  # Type casting
  - name: types
    type: cast_fields
    mapping:
      amount: float
      customer_id: string

  # SQL enrichment (SELECT *, arithmetic, CASE WHEN)
  - name: enrich
    type: sql
    query: "SELECT *, amount * 1.15 as amount_with_tax FROM events"

  # Deduplication by key
  - name: dedupe
    type: deduplicate
    fields: [event_id]
```

---

## Multi-Sink

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

  - name: downstream
    type: kafka
    topic: events.orders.enriched
```

**Supported sinks:** stdout, file, Snowflake, BigQuery, PostgreSQL, Kafka.

---

## Fault Isolation

Each pipeline runs independently. Failed events go to a Dead Letter Queue instead of blocking the pipeline.

```yaml
isolation:
  strategy: per_event_type
  maxRetries: 3
  backoffMs: 1000
  dlqEnabled: true

schema:
  enforce: true
  registry: http://schema-registry:8081
  compatibility: BACKWARD
  onFailure: dlq
  dlqTopic: events.orders.dlq
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

See [docker/README.md](docker/README.md) for full documentation.

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
  |  Source (Kafka) --> Transform Chain --> Sink(s)
  |       |               |                  |
  |       |    hash_fields | stdout          |
  |       |    mask_fields | file            |
  |       |    filter      | Snowflake       |
  |       |    rename      | BigQuery        |
  |       |    deduplicate | PostgreSQL      |
  |       |    sql         | Kafka           |
  |       |                                  |
  |  DLQ + Retries + Backoff                 |
  +------------------------------------------+
```

---

## Project Structure

```
mako/
├── api/v1/types.go              # Pipeline spec (the YAML DSL model)
├── pkg/
│   ├── config/config.go         # YAML parser + validator
│   ├── pipeline/engine.go       # Runtime: Source -> Transforms -> Sink
│   ├── transform/transform.go   # Transform implementations
│   ├── sink/sink.go             # Sink adapters
│   ├── kafka/kafka.go           # Kafka source + sink
│   └── codegen/codegen.go       # K8s + Terraform generators
├── internal/cli/cli.go          # CLI helpers
├── main.go                      # CLI entry point
├── main_test.go                 # 25 tests + benchmarks
├── docker/                      # Local infra (Kafka, PostgreSQL, Flink, Schema Registry)
├── examples/
│   ├── simple/pipeline.yaml
│   └── advanced/pipeline.yaml
└── test/fixtures/events.jsonl
```

---

## Roadmap

- [ ] Full Kafka consumer (franz-go) for `mako run`
- [ ] PostgreSQL sink (pgx)
- [ ] Schema Registry validation at runtime
- [ ] Prometheus metrics endpoint (`/metrics`)
- [ ] Health probes (`/healthz`, `/readyz`)
- [ ] Snowflake Snowpipe sink
- [ ] BigQuery Storage Write API sink
- [ ] Helm chart
- [ ] Grafana dashboard templates
- [ ] WASM plugin transforms

---

## License

MIT

---

*Built by [Stefen Taime](https://github.com/Stefen-Taime)*
