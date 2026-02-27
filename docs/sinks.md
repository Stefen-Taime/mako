# Sinks

## PostgreSQL (pgx)

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

## Snowflake (gosnowflake)

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

By default, events are stored as `VARIANT` (JSON) via `PARSE_JSON()`. Target table is auto-created:

```sql
CREATE TABLE ORDER_EVENTS (
    event_data    VARIANT NOT NULL,
    event_key     VARCHAR,
    event_topic   VARCHAR,
    event_offset  NUMBER,
    loaded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Flatten mode

With `flatten: true`, each top-level key in the event JSON becomes its own typed column instead of being stored in a single VARIANT column. The table schema is auto-detected from the first batch of events.

```yaml
sink:
  type: snowflake
  database: ANALYTICS
  schema: RAW
  table: USERS
  flatten: true
  config:
    account: xy12345.us-east-1
    user: MAKO_USER
    warehouse: COMPUTE_WH
    role: MAKO_ROLE
```

Type mapping:

| Go type | Snowflake type |
|---|---|
| `string` | `VARCHAR` |
| `float64` | `FLOAT` |
| `bool` | `BOOLEAN` |
| `map` / `array` (nested) | `VARIANT` |

The generated table looks like:

```sql
CREATE TABLE USERS (
    city        VARCHAR,
    email       VARCHAR,
    first_name  VARCHAR,
    popularity  FLOAT,
    address     VARIANT,
    loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

Schema evolution is handled automatically: if new keys appear in later batches, columns are added via `ALTER TABLE ADD COLUMN IF NOT EXISTS`.

Nested objects (like `address`) are inserted using `PARSE_JSON()` and stored as `VARIANT`, so they remain queryable with Snowflake's semi-structured data syntax (`address:city`, `address:zip`).

## BigQuery (streaming inserter)

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

## S3 (AWS SDK v2)

Object storage sink using [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2). Events are buffered and flushed as time-partitioned objects.

```yaml
sink:
  type: s3
  bucket: my-data-lake
  prefix: raw/events
  format: jsonl                     # jsonl | json
  config:
    region: us-east-1
    # Optional: explicit credentials (defaults to AWS SDK credential chain)
    access_key_id: AKIA...
    secret_access_key: ...
    # Optional: custom endpoint for MinIO / LocalStack
    endpoint: http://localhost:9000
```

Object key pattern: `<prefix>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<count>.jsonl`

Features: Hive-style time partitioning, AWS credential chain (IAM, env, config), MinIO/LocalStack compatible via custom endpoint, buffered writes with retry on failure.

## GCS (cloud.google.com/go/storage)

Object storage sink using the official [Google Cloud Storage](https://pkg.go.dev/cloud.google.com/go/storage) client.

```yaml
sink:
  type: gcs
  bucket: my-data-lake
  prefix: raw/events
  format: jsonl                     # jsonl | json
  config:
    project: my-gcp-project
```

Authentication via Application Default Credentials (ADC). Set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login`.

Object name pattern: `<prefix>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<count>.jsonl`

Features: Hive-style time partitioning, ADC authentication, buffered writes with retry on failure.

## ClickHouse (clickhouse-go v2)

High-performance OLAP sink using the official [clickhouse-go](https://github.com/ClickHouse/clickhouse-go) native protocol driver with LZ4 compression and batch inserts.

```yaml
sink:
  type: clickhouse
  database: analytics
  table: pipeline_events
  config:
    host: localhost
    port: "9000"
    user: default
    password: ""
    secure: "false"          # set "true" for TLS
    # Or use a full DSN:
    # dsn: clickhouse://default:@localhost:9000/analytics
```

Target table schema:

```sql
CREATE TABLE analytics.pipeline_events (
    event_date    Date DEFAULT toDate(event_ts),
    event_ts      DateTime64(3) DEFAULT now64(),
    event_key     String,
    event_data    String,
    topic         String,
    partition_id  UInt32,
    offset_id     UInt64
) ENGINE = MergeTree()
ORDER BY (event_date, event_ts)
PARTITION BY toYYYYMM(event_date);
```

Features: native TCP protocol, LZ4 compression, batch inserts via `PrepareBatch`, automatic date partitioning.

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
    config:
      project: my-project

  - name: downstream
    type: kafka
    topic: events.orders.enriched
```

**All sinks:** stdout, file, PostgreSQL, Snowflake, BigQuery, ClickHouse, S3, GCS, Kafka.
