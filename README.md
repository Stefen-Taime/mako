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
  <a href="docs/sources.md">Sources</a> &middot;
  <a href="docs/transforms.md">Transforms</a> &middot;
  <a href="docs/sinks.md">Sinks</a> &middot;
  <a href="docs/observability.md">Observability</a> &middot;
  <a href="docs/helm.md">Helm</a> &middot;
  <a href="docs/codegen.md">Codegen</a> &middot;
  <a href="docs/local-infra.md">Local Infra</a> &middot;
  <a href="CONTRIBUTING.md">Contributing</a>
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
| Sources | Kafka, File, PostgreSQL CDC, HTTP/API — [docs](docs/sources.md) |
| Transforms | hash, mask, filter, rename, dedupe + WASM plugins — [docs](docs/transforms.md) |
| Sinks | PostgreSQL, Snowflake, BigQuery, ClickHouse, S3, GCS, Kafka — [docs](docs/sinks.md) |
| Schema | Confluent Schema Registry (JSON Schema + Avro) — [docs](docs/observability.md#schema-enforcement) |
| Observability | Prometheus /metrics, /health, /ready, /status — [docs](docs/observability.md) |
| Deploy | Helm chart, `mako generate --k8s` + `--tf` — [helm](docs/helm.md) / [codegen](docs/codegen.md) |
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

## Architecture

```text
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
  |  Source --> Transform Chain --> Sink(s)   |
  |  (Kafka)    hash_fields       Postgres   |
  |  (File)     mask_fields       Snowflake  |
  |  (PG CDC)   filter            BigQuery   |
  |  (HTTP)     rename            ClickHouse |
  |             deduplicate       S3 / GCS   |
  |             wasm_plugin       Kafka      |
  |                               Stdout     |
  |                                          |
  |  Schema Registry --> Validate            |
  |  Prometheus    --> /metrics              |
  |  Health        --> /health, /ready       |
  |  DLQ + Retries + Backoff                 |
  +------------------------------------------+
```

---

## Project Structure

```text
mako/
├── api/v1/types.go                 # Pipeline spec (the YAML DSL model)
├── pkg/
│   ├── config/config.go            # YAML parser + validator
│   ├── pipeline/engine.go          # Runtime: Source -> Transforms -> Sink
│   ├── transform/
│   │   ├── transform.go            # Transform implementations
│   │   └── wasm.go                 # WASM plugin runtime (wazero)
│   ├── source/
│   │   ├── file.go                 # File source (JSONL, CSV, JSON)
│   │   ├── postgres_cdc.go         # PostgreSQL CDC source (pgx + pglogrepl)
│   │   └── http.go                 # HTTP/API source (REST, pagination, OAuth2)
│   ├── sink/
│   │   ├── sink.go                 # Stdout, File sinks + BuildFromSpec
│   │   ├── postgres.go             # PostgreSQL sink (pgx)
│   │   ├── snowflake.go            # Snowflake sink (gosnowflake)
│   │   ├── bigquery.go             # BigQuery sink (streaming inserter)
│   │   ├── clickhouse.go           # ClickHouse sink (clickhouse-go v2)
│   │   ├── s3.go                   # S3 sink (AWS SDK v2)
│   │   ├── gcs.go                  # GCS sink (cloud.google.com/go/storage)
│   │   ├── encode.go               # Shared Parquet + CSV encoders
│   │   └── resolve.go              # Secret resolution chain (config → env → Vault)
│   ├── kafka/kafka.go              # Kafka source + sink (franz-go)
│   ├── schema/registry.go          # Schema Registry client + validator
│   ├── observability/server.go     # Prometheus metrics + health + status HTTP
│   ├── vault/vault.go              # HashiCorp Vault client (optional)
│   └── codegen/codegen.go          # K8s + Terraform generators
├── internal/cli/cli.go             # CLI helpers
├── main.go                         # CLI entry point
├── main_test.go                    # 70+ tests + benchmarks
├── docs/                           # Detailed documentation
│   ├── sources.md
│   ├── transforms.md
│   ├── sinks.md
│   ├── observability.md
│   ├── helm.md
│   ├── codegen.md
│   └── local-infra.md
├── charts/mako/                    # Helm chart for Kubernetes
│   ├── Chart.yaml
│   ├── values.yaml
│   └── templates/
├── docker/                         # Local infra (Kafka, PostgreSQL, Flink, Schema Registry)
├── .github/workflows/ci.yml        # CI: unit + integration tests
├── Dockerfile                      # Production image
├── Dockerfile.runner               # Per-pipeline runner image
├── grafana/
│   └── mako-dashboard.json         # Grafana dashboard template
├── examples/
│   ├── simple/pipeline.yaml
│   ├── advanced/pipeline.yaml
│   ├── snowflake/                  # Snowflake flatten examples
│   │   ├── pipeline-movies.yaml
│   │   └── pipeline-users.yaml
│   └── wasm-plugin/                # WASM plugin example (Go/TinyGo)
│       ├── main.go
│       └── pipeline.yaml
└── test/fixtures/events.jsonl
```

---

## CI / Testing

GitHub Actions runs on every push/PR:

**Unit tests** (fast, no Docker):

- 70+ tests covering config, validation, transforms, WASM plugins, codegen, sources, sinks
- Benchmarks for transform chain performance
- Example validation + dry-run

**Integration tests** (Docker services):

- Kafka (KRaft) + PostgreSQL + Schema Registry
- Full pipeline: produce messages -> consume -> transform -> write to PG
- HTTP endpoint verification (/metrics, /health, /ready, /status)
- File source validation

```bash
# Run locally
go test -v -count=1 ./...
go test -bench=. -benchmem ./...
```

---

## Roadmap

- [x] Kafka consumer/producer (franz-go)
- [x] PostgreSQL sink (pgx + COPY)
- [x] Snowflake sink (gosnowflake) + flatten mode (auto-typed columns)
- [x] BigQuery sink (streaming inserter)
- [x] Schema Registry validation (JSON Schema)
- [x] File source (JSONL, CSV, JSON)
- [x] Prometheus metrics (/metrics)
- [x] Health/readiness probes (/health, /ready)
- [x] Pipeline status API (/status)
- [x] CI with integration tests (Kafka + PG + Schema Registry)
- [x] S3/GCS object storage sink
- [x] Grafana dashboard templates
- [x] ClickHouse sink (clickhouse-go v2)
- [x] Helm chart
- [x] WASM plugin transforms (wazero)
- [x] Flatten mode for PostgreSQL, BigQuery, ClickHouse sinks
- [x] Parquet + CSV output formats for S3/GCS
- [x] HashiCorp Vault integration (secret resolution chain)
- [x] PostgreSQL CDC source (snapshot, cdc, snapshot+cdc)
- [x] HTTP/API source (pagination, OAuth2, rate limiting, retries)

---

## License

MIT

---

*Built by [Stefen Taime](https://github.com/Stefen-Taime)*
