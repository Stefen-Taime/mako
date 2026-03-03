<p align="center">
  <img src="img/logo.png" alt="Mako" width="280" />
</p>

<h1 align="center">Mako</h1>

<p align="center">
  <strong>Declarative real-time data pipelines. YAML in, events out.</strong>
</p>

<p align="center">
  <em>Named after the shortfin mako -- fastest shark in the ocean. Your data deserves the same speed.</em>
</p>

<p align="center">
  <a href="#connector-catalog">Catalog</a> &middot;
  <a href="examples/sources/">Sources</a> &middot;
  <a href="examples/sinks/">Sinks</a> &middot;
  <a href="examples/transforms/">Transforms</a> &middot;
  <a href="examples/workflows/">Workflows</a> &middot;
  <a href="#observability">Observability</a> &middot;
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
mako workflow workflow.yaml          # DAG orchestration
```

---

## Connector Catalog

<h3 align="center">Sources</h3>

<p align="center"><em>Where your data comes from</em></p>

<table align="center">
<thead>
<tr><th>Connector</th><th>Type</th><th>Highlights</th><th>Examples</th></tr>
</thead>
<tbody>
<tr><td><strong>HTTP / REST API</strong></td><td><code>http</code></td><td>Pagination, OAuth2, Bearer, Basic, API Key, rate limiting</td><td><a href="examples/sources/http/">8 pipelines</a></td></tr>
<tr><td><strong>File</strong></td><td><code>file</code></td><td>JSON, CSV, Parquet, gzip -- local or remote URL</td><td><a href="examples/sources/file/">5 pipelines</a></td></tr>
<tr><td><strong>Apache Kafka</strong></td><td><code>kafka</code></td><td>Consumer groups, earliest/latest offset (franz-go)</td><td><a href="examples/sources/kafka/">2 pipelines</a></td></tr>
<tr><td><strong>PostgreSQL CDC</strong></td><td><code>postgres_cdc</code></td><td>Snapshot, CDC, snapshot+CDC (pglogrepl)</td><td><a href="examples/sources/postgres-cdc/">1 pipeline</a></td></tr>
<tr><td><strong>DuckDB</strong></td><td><code>duckdb</code></td><td>Embedded SQL, native Parquet/CSV, S3/GCS/Azure</td><td><a href="examples/sources/duckdb/">3 pipelines</a></td></tr>
</tbody>
</table>

<p align="center"><a href="docs/sources.md">Full source documentation &rarr;</a></p>

---

<h3 align="center">Sinks</h3>

<p align="center"><em>Where your data goes</em></p>

<table align="center">
<thead>
<tr><th>Connector</th><th>Type</th><th>Highlights</th><th>Examples</th></tr>
</thead>
<tbody>
<tr><td><strong>PostgreSQL</strong></td><td><code>postgres</code></td><td>Auto-flatten, COPY protocol, Vault secrets</td><td><a href="examples/sinks/postgres/">2 pipelines</a></td></tr>
<tr><td><strong>Snowflake</strong></td><td><code>snowflake</code></td><td>Auto-DDL, flatten mode, batch loading</td><td><a href="examples/sinks/snowflake/">2 pipelines</a></td></tr>
<tr><td><strong>DuckDB</strong></td><td><code>duckdb</code></td><td>Auto-table, schema evolution, Parquet/CSV export</td><td><a href="examples/sinks/duckdb/">2 pipelines</a></td></tr>
<tr><td><strong>Google Cloud Storage</strong></td><td><code>gcs</code></td><td>Parquet + CSV, Snappy compression</td><td><a href="examples/sinks/gcs/">3 pipelines</a></td></tr>
<tr><td><strong>Apache Kafka</strong></td><td><code>kafka</code></td><td>Schema Registry validation, DLQ</td><td><a href="examples/sinks/kafka/">1 pipeline</a></td></tr>
<tr><td><strong>BigQuery</strong></td><td><code>bigquery</code></td><td>Streaming inserter</td><td>--</td></tr>
<tr><td><strong>ClickHouse</strong></td><td><code>clickhouse</code></td><td>clickhouse-go v2, flatten mode</td><td>--</td></tr>
<tr><td><strong>S3</strong></td><td><code>s3</code></td><td>Parquet + CSV, AWS SDK v2</td><td>--</td></tr>
<tr><td><strong>Stdout</strong></td><td><code>stdout</code></td><td>Debug output to console</td><td><a href="examples/sinks/stdout/">1 pipeline</a></td></tr>
</tbody>
</table>

<p align="center"><a href="docs/sinks.md">Full sink documentation &rarr;</a></p>

---

<h3 align="center">Transforms</h3>

<p align="center"><em>How your data is processed</em></p>

<table align="center">
<thead>
<tr><th>Transform</th><th>Type</th><th>Description</th><th>Examples</th></tr>
</thead>
<tbody>
<tr><td><strong>SQL Enrichment</strong></td><td><code>sql</code></td><td>CASE WHEN, computed fields, DuckDB functions</td><td><a href="examples/transforms/sql/">2 pipelines</a></td></tr>
<tr><td><strong>WASM Plugins</strong></td><td><code>plugin</code></td><td>Custom logic in Go (TinyGo) or Rust</td><td><a href="examples/transforms/wasm/">2 pipelines + source</a></td></tr>
<tr><td><strong>Schema Validation</strong></td><td><code>schema</code></td><td>Confluent Schema Registry (log / reject / DLQ)</td><td><a href="examples/transforms/schema/">3 pipelines</a></td></tr>
<tr><td><strong>Data Quality</strong></td><td><code>dq_check</code></td><td>not_null, range, in_set, regex, type checks</td><td><a href="examples/transforms/dq-check/">2 pipelines</a></td></tr>
<tr><td><strong>PII Masking</strong></td><td><code>hash_fields</code></td><td>SHA-256 hash for emails, phones, cards, SSNs</td><td><a href="examples/transforms/pii/">2 pipelines</a></td></tr>
<tr><td><strong>Filter</strong></td><td><code>filter</code></td><td>Keep/discard events by condition</td><td><a href="examples/transforms/filter/">1 pipeline</a></td></tr>
<tr><td><strong>Rename Fields</strong></td><td><code>rename_fields</code></td><td>Rename columns for target convention</td><td>used across examples</td></tr>
<tr><td><strong>Drop Fields</strong></td><td><code>drop_fields</code></td><td>Remove unnecessary columns</td><td>used across examples</td></tr>
<tr><td><strong>Cast Fields</strong></td><td><code>cast_fields</code></td><td>Type conversion (string, int, float, bool)</td><td>used across examples</td></tr>
<tr><td><strong>Flatten</strong></td><td><code>flatten</code></td><td>Flatten nested JSON objects</td><td>used across examples</td></tr>
<tr><td><strong>Default Values</strong></td><td><code>default_values</code></td><td>Set defaults for missing fields</td><td>used across examples</td></tr>
<tr><td><strong>Deduplicate</strong></td><td><code>deduplicate</code></td><td>Remove duplicates by key</td><td>used across examples</td></tr>
</tbody>
</table>

<p align="center"><a href="docs/transforms.md">Full transform documentation &rarr;</a></p>

---

<h3 align="center">Workflows</h3>

<p align="center"><em>DAG orchestration for multi-pipeline jobs</em></p>

<table align="center">
<thead>
<tr><th>Workflow</th><th>Steps</th><th>Highlights</th></tr>
</thead>
<tbody>
<tr><td><a href="examples/workflows/nyc-tlc-star-schema/"><strong>NYC TLC Star Schema</strong></a></td><td>9 + quality gate</td><td>Star schema from 700K+ taxi trips, 6 dimensions, fact table, daily aggregation, SQL assertions</td></tr>
<tr><td><a href="examples/workflows/multi-source-demo/"><strong>Multi-Source Demo</strong></a></td><td>3 (parallel)</td><td>HTTP + CSV sources, DuckDB + PostgreSQL sinks, parallel execution</td></tr>
<tr><td><a href="examples/workflows/etl-demo/"><strong>ETL Demo</strong></a></td><td>3 (sequential)</td><td>Simple ingest &rarr; transform &rarr; load chain</td></tr>
</tbody>
</table>

<p align="center"><a href="docs/workflows.md">Full workflow documentation &rarr;</a></p>

---

<h3 align="center">Cross-Reference: Connectors as Source & Sink</h3>

<table align="center">
<thead>
<tr><th>Connector</th><th>As Source</th><th>As Sink</th></tr>
</thead>
<tbody>
<tr><td>PostgreSQL</td><td><a href="examples/sources/postgres-cdc/">CDC source</a></td><td><a href="examples/sinks/postgres/">Sink</a></td></tr>
<tr><td>DuckDB</td><td><a href="examples/sources/duckdb/">Query source</a></td><td><a href="examples/sinks/duckdb/">Sink + export</a></td></tr>
<tr><td>Kafka</td><td><a href="examples/sources/kafka/">Consumer</a></td><td><a href="examples/sinks/kafka/">Producer</a></td></tr>
<tr><td>GCS</td><td>via DuckDB httpfs</td><td><a href="examples/sinks/gcs/">Sink</a></td></tr>
</tbody>
</table>

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

# Run pipeline (Source -> Transforms -> Sink)
./bin/mako run pipeline.yaml

# Run a workflow (DAG of multiple pipelines)
./bin/mako workflow workflow.yaml
```

**Output of dry-run:**

```json
{"_pii_processed":true,"amount":99.99,"email":"243b73234c6433b8","environment":"production","status":"completed"}
```

The email is hashed (PII compliance), the event passes the production filter, and it's ready for the warehouse.

---

## Observability

All pipelines expose Prometheus metrics, health probes, and a status API:

| Endpoint | Description |
|----------|-------------|
| `/metrics` | Prometheus metrics (events processed, errors, latency, batch size) |
| `/health` | Liveness probe |
| `/ready` | Readiness probe |
| `/status` | Pipeline status JSON (state, counts, uptime) |

**Workflow mode:** All pipelines in a workflow share a single Prometheus endpoint (`:9090`) via a shared metrics registry.

Slack alerting is supported for freshness SLA violations, error spikes, and pipeline completion.

See [docs/observability.md](docs/observability.md) for details.

---

## Architecture

```text
pipeline.yaml
       |
       v
  +----------+
  |   mako   |  CLI: validate, dry-run, run, workflow
  |   (Go)   |
  +----+-----+
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
  |  (DuckDB)   sql / dq_check   DuckDB     |
  |             wasm_plugin       S3 / GCS   |
  |             deduplicate       Kafka      |
  |             cast_fields       Stdout     |
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
├── main.go                         # CLI entry point (run, workflow, validate, generate)
├── api/v1/types.go                 # Pipeline + Workflow spec (the YAML DSL model)
├── pkg/
│   ├── config/config.go            # YAML parser + validator
│   ├── pipeline/engine.go          # Runtime: Source -> Transforms -> Sink
│   ├── source/
│   │   ├── file.go                 # File source (JSONL, CSV, JSON, Parquet + gzip)
│   │   ├── postgres_cdc.go         # PostgreSQL CDC (pgx + pglogrepl)
│   │   ├── http.go                 # HTTP/API source (pagination, OAuth2)
│   │   ├── duckdb.go               # DuckDB source (SQL, Parquet/CSV/JSON + S3/GCS)
│   │   └── multi.go                # Multi-source with join support
│   ├── sink/
│   │   ├── sink.go                 # Stdout, File sinks + BuildFromSpec
│   │   ├── postgres.go             # PostgreSQL (pgx + COPY)
│   │   ├── snowflake.go            # Snowflake (gosnowflake)
│   │   ├── bigquery.go             # BigQuery (streaming inserter)
│   │   ├── clickhouse.go           # ClickHouse (clickhouse-go v2)
│   │   ├── s3.go                   # S3 (AWS SDK v2)
│   │   ├── gcs.go                  # GCS (cloud.google.com/go/storage)
│   │   ├── duckdb.go               # DuckDB (embedded, Parquet/CSV export)
│   │   ├── encode.go               # Shared Parquet + CSV encoders
│   │   └── resolve.go              # Secret resolution (config -> env -> Vault)
│   ├── transform/
│   │   ├── transform.go            # All built-in transforms
│   │   └── wasm.go                 # WASM plugin runtime (wazero)
│   ├── workflow/
│   │   ├── engine.go               # DAG engine (parallel steps, failure policies)
│   │   └── quality_gate.go         # SQL assertions against DuckDB
│   ├── observability/
│   │   ├── server.go               # Prometheus metrics + health + status HTTP
│   │   └── registry.go             # Shared metrics registry (workflow mode)
│   ├── kafka/kafka.go              # Kafka source + sink (franz-go)
│   ├── schema/registry.go          # Schema Registry client + validator
│   ├── join/join.go                # Multi-source join engine
│   ├── duckdbext/cloud.go          # DuckDB httpfs + cloud credentials
│   ├── alerting/                   # Slack alert rules + notifications
│   └── vault/vault.go              # HashiCorp Vault client
├── examples/                       # Pipeline catalog (see below)
│   ├── sources/                    # HTTP, File, Kafka, PostgreSQL CDC, DuckDB
│   ├── sinks/                      # PostgreSQL, Snowflake, DuckDB, GCS, Kafka, Stdout
│   ├── transforms/                 # SQL, WASM, Schema, DQ Check, PII, Filter
│   └── workflows/                  # NYC TLC Star Schema, ETL Demo, Multi-Source
├── docs/                           # Detailed documentation
├── docker/                         # Local infra (Kafka, PostgreSQL, Prometheus)
├── grafana/                        # Grafana dashboard template
├── .github/workflows/ci.yml        # CI: unit + integration tests
└── Dockerfile                      # Production image
```

---

## CI / Testing

GitHub Actions runs on every push/PR:

**Unit tests** (fast, no Docker):

- 70+ tests covering config, validation, transforms, WASM plugins, sources, sinks
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
- [x] Snowflake sink (gosnowflake) + flatten mode
- [x] BigQuery sink (streaming inserter)
- [x] Schema Registry validation (JSON Schema)
- [x] File source (JSONL, CSV, JSON + transparent gzip)
- [x] Prometheus metrics (/metrics)
- [x] Health/readiness probes (/health, /ready)
- [x] Pipeline status API (/status)
- [x] CI with integration tests (Kafka + PG + Schema Registry)
- [x] S3/GCS object storage sinks
- [x] Grafana dashboard templates
- [x] ClickHouse sink (clickhouse-go v2)
- [x] WASM plugin transforms (wazero)
- [x] Parquet + CSV output formats for S3/GCS
- [x] HashiCorp Vault integration (secret resolution chain)
- [x] PostgreSQL CDC source (snapshot, cdc, snapshot+cdc)
- [x] HTTP/API source (pagination, OAuth2, rate limiting, retries)
- [x] Real-time observability metrics (500ms sync, sink latency)
- [x] Rust WASM plugin example
- [x] DuckDB embedded source + sink
- [x] Parquet file source (native reading via parquet-go)
- [x] DuckDB cloud storage (S3/GCS/Azure via httpfs)
- [x] Workflow engine (DAG orchestration, parallel steps, failure policies)
- [x] Data quality: inline `dq_check` transform
- [x] Data quality: `quality_gate` workflow step (SQL assertions)
- [x] Shared Prometheus metrics registry (single port for workflows)
- [ ] Helm chart for Kubernetes deployment
- [ ] Codegen: `mako generate --k8s` + `--tf` (Kubernetes manifests, Terraform HCL)

---

## License

MIT

---

*Built by [Stefen Taime](https://github.com/Stefen-Taime)*
