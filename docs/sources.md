# Sources

## Kafka (default)

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

## File (JSONL, CSV, JSON + HTTP URLs)

Read events from local files **or remote HTTP/HTTPS URLs**. Useful for backfill, testing, and batch processing.

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

### HTTP/HTTPS URLs

Point `path` directly to a remote file — no download needed. The response body is streamed directly to the reader (no temp file).

```yaml
source:
  type: file
  config:
    path: https://raw.githubusercontent.com/user/repo/main/data.json
    format: json
```

Works with any format (JSON, JSONL, CSV). At `Open()` a HEAD request validates reachability; at `Read()` a GET streams the data.

### Supported formats

- **JSONL** (`.jsonl`, `.ndjson`): one JSON object per line
- **CSV** (`.csv`): with optional header row, configurable delimiter
- **JSON** (`.json`): single object or array of objects

### Auto-termination

When the file source (local or URL) reaches EOF, the pipeline shuts down automatically — no need for `Ctrl+C`.

## PostgreSQL CDC (pgx + pglogrepl)

Change Data Capture source using [pgx](https://github.com/jackc/pgx) for queries and [pglogrepl](https://github.com/jackc/pglogrepl) for logical replication.

```yaml
source:
  type: postgres_cdc
  config:
    host: localhost
    port: "5432"
    user: postgres
    password: secret
    database: myapp
    tables: [users, orders, payments]
    schema: public                   # default: public
    mode: snapshot+cdc               # snapshot | cdc | snapshot+cdc
    snapshot_batch_size: 10000       # rows per SELECT (default: 10000)
    snapshot_order_by: id            # keyset pagination column (auto-detected PK)
    slot_name: mako_slot             # replication slot name (default: mako_slot)
    publication: mako_pub            # publication name (default: mako_pub)
    start_lsn: ""                    # resume CDC from a specific LSN
    # Or use a full DSN:
    # dsn: postgres://postgres:secret@localhost:5432/myapp
```

### Modes

| Mode | Description |
|---|---|
| `snapshot` | Bulk load via `SELECT` with keyset pagination (efficient on large tables). Pipeline terminates after snapshot completes. |
| `cdc` | Real-time streaming via PostgreSQL logical replication (pgoutput plugin). Runs continuously. |
| `snapshot+cdc` | Full snapshot first, then seamlessly switches to CDC streaming. Best for initial loads + ongoing sync. |

### Snapshot mode

- Auto-discovers primary key from `pg_index` / `pg_attribute` for efficient keyset pagination
- Falls back to `ctid` system column if no PK exists
- Configurable batch size and order-by column
- Progress logging every 10,000 rows
- Events include `metadata.operation = "snapshot"` and `metadata.table`

### CDC mode

- Creates publication and replication slot automatically
- Parses WAL messages via pgoutput plugin
- Handles `INSERT`, `UPDATE` (with old values in `metadata.old_values`), and `DELETE` events
- Sends standby status updates every 10 seconds
- Events include `metadata.operation` (`insert` / `update` / `delete`), `metadata.table`, `metadata.lsn`

### Event format

```json
{
  "key": "users:42",
  "value": {"id": 42, "name": "Alice", "email": "alice@example.com"},
  "topic": "users",
  "metadata": {
    "operation": "insert",
    "table": "users",
    "lsn": "0/16B3748"
  }
}
```

### Prerequisites

PostgreSQL must be configured for logical replication:

```sql
-- postgresql.conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Credentials can be resolved via config, environment variables (`POSTGRES_SOURCE_*`), or [Vault](../README.md#vault-integration).

## HTTP / API

REST API source with pagination, authentication, rate limiting, and retry logic.

```yaml
source:
  type: http
  config:
    url: https://api.example.com/v1/users
    method: GET                      # default: GET
    timeout: 30s                     # request timeout (default: 30s)

    # Authentication
    auth_type: bearer                # none | bearer | basic | api_key | oauth2
    auth_token: ${API_TOKEN}

    # Response parsing
    response_type: json              # json | jsonl | csv
    data_path: data.results          # dot-notation path to records array

    # Pagination
    pagination_type: offset          # none | offset | cursor | next_url | page
    pagination_limit: 100

    # Polling
    poll_interval: 5m                # 0 = one-shot (default)
    incremental_key: updated_at      # track max value for incremental fetches

    # Rate limiting
    rate_limit_rps: 10               # requests per second

    # Retries
    max_retries: 3                   # default: 3
```

### Authentication methods

| Method | Config keys | Description |
|---|---|---|
| `bearer` | `auth_token` | `Authorization: Bearer <token>` header |
| `basic` | `auth_user`, `auth_password` | HTTP Basic Auth (base64 encoded) |
| `api_key` | `api_key_header`, `api_key_value` | Custom header (e.g., `X-API-Key`) |
| `oauth2` | `oauth2_token_url`, `oauth2_client_id`, `oauth2_client_secret`, `oauth2_content_type` | Client credentials grant with automatic token refresh |

### Response parsing

| Type | Description |
|---|---|
| `json` | JSON object or array. Use `data_path` to navigate to the records (e.g., `data.results`) |
| `jsonl` | Newline-delimited JSON (one object per line) |
| `csv` | CSV with header row. Each row becomes a `map[string]string` event |

### Pagination strategies

| Strategy | Description |
|---|---|
| `offset` | Increment offset by limit each page. Stops when `total` in response is reached or empty page. Config: `pagination_limit`, `pagination_offset_param` |
| `cursor` | Pass cursor from previous response. Config: `pagination_cursor_param`, `pagination_cursor_path` |
| `next_url` | Follow `next` URL from response. Config: `pagination_next_path` |
| `page` | Increment page number. Stops when `has_more` is false or empty page. Config: `pagination_limit` |

### Polling

- **One-shot** (`poll_interval: 0`): fetch all pages once, then pipeline terminates
- **Periodic** (`poll_interval: 5m`): fetch all pages, wait, repeat. Runs continuously
- **Incremental** (`incremental_key: updated_at`): tracks the max value of the given field across fetches, available via `incremental_value` for filtering

### Rate limiting and retries

- Rate limiter via `golang.org/x/time/rate` — configurable requests-per-second
- Retries with exponential backoff (1s, 2s, 4s, ...):
  - **Retry:** `429 Too Many Requests`, `5xx` server errors
  - **No retry:** `4xx` client errors (400, 401, 403, 404)

### OAuth2 content type

By default, token requests use `application/x-www-form-urlencoded` (RFC 6749 standard). Some APIs require `application/json` instead. Use `oauth2_content_type` to switch:

| Value | Content-Type | Body format |
|---|---|---|
| `form` (default) | `application/x-www-form-urlencoded` | `grant_type=client_credentials&client_id=...` |
| `json` | `application/json` | `{"grant_type":"client_credentials","client_id":"..."}` |

```yaml
source:
  type: http
  config:
    auth_type: oauth2
    oauth2_token_url: http://api.example.com/oauth2/token
    oauth2_client_id: ${CLIENT_ID}
    oauth2_client_secret: ${CLIENT_SECRET}
    oauth2_content_type: json    # send token request as JSON
```

### Example: paginated API with OAuth2

```yaml
source:
  type: http
  config:
    url: https://api.hubspot.com/crm/v3/objects/contacts
    auth_type: oauth2
    oauth2_token_url: https://api.hubspot.com/oauth/v1/token
    oauth2_client_id: ${HUBSPOT_CLIENT_ID}
    oauth2_client_secret: ${HUBSPOT_CLIENT_SECRET}
    response_type: json
    data_path: results
    pagination_type: cursor
    pagination_cursor_param: after
    pagination_cursor_path: paging.next.after
    pagination_limit: 100
    rate_limit_rps: 5
    poll_interval: 15m
```

## DuckDB (embedded)

Embedded analytical source using [go-duckdb](https://github.com/marcboeker/go-duckdb) (CGO, embeds DuckDB). Reads data via SQL queries — including DuckDB's native Parquet, CSV, and JSON file readers.

```yaml
source:
  type: duckdb
  config:
    database: /data/analytics.duckdb   # ":memory:" by default
    query: "SELECT * FROM read_parquet('/data/*.parquet')"
    batch_size: 10000                   # rows per batch (default: 10000)
```

### Configuration

| Key | Default | Description |
|---|---|---|
| `database` | `:memory:` | Path to DuckDB database file, or `:memory:` for in-memory |
| `query` | — | SQL query to execute (supports DuckDB functions like `read_parquet`, `read_csv`, `read_json`) |
| `table` | — | Table name (shorthand for `SELECT * FROM <table>`). Either `query` or `table` is required |
| `batch_size` | `10000` | Number of rows buffered in the event channel |

### Native file reading

DuckDB can read external files directly via SQL — no separate file source needed:

```yaml
# Read Parquet files (local or S3)
query: "SELECT * FROM read_parquet('/data/*.parquet')"
query: "SELECT * FROM read_parquet('s3://bucket/path/*.parquet')"

# Read CSV files
query: "SELECT * FROM read_csv('/data/events.csv', header=true)"

# Read JSON files
query: "SELECT * FROM read_json('/data/events.json')"

# Analytical queries with DuckDB SQL
query: "SELECT user_id, count(*) as cnt, sum(amount) as total FROM read_parquet('/data/*.parquet') GROUP BY user_id"
```

### Auto-termination

When the query finishes reading all rows, the pipeline shuts down automatically (same behavior as the file source).

### Type conversion

DuckDB column types are automatically converted to JSON-friendly Go types:

| DuckDB type | Go type |
|---|---|
| `VARCHAR`, `TEXT` | `string` |
| `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT` | `int64` |
| `DOUBLE`, `FLOAT`, `REAL` | `float64` |
| `BOOLEAN` | `bool` |
| `TIMESTAMP`, `DATE`, `TIME` | `string` (RFC 3339) |
| `LIST`, `STRUCT`, `MAP` | parsed JSON (via `[]byte` unmarshalling) |

### Example: Parquet to DuckDB

```yaml
pipeline:
  name: parquet-to-duckdb
  source:
    type: duckdb
    config:
      query: "SELECT * FROM read_parquet('/data/*.parquet')"
      batch_size: 10000
  sink:
    type: duckdb
    database: /data/analytics.duckdb
    table: events
    config:
      create_table: true
```

## Multi-Source with Join

Pipelines can define multiple named sources joined together before processing. The single `source:` field continues to work for backward compatibility.

```yaml
pipeline:
  name: enriched-orders
  sources:
    - name: orders
      type: kafka
      topic: events.orders
      brokers: localhost:9092
      startOffset: latest

    - name: customers
      type: http
      config:
        url: http://localhost:8000/customers/
        method: GET
        auth_type: none
        response_type: json

  join:
    type: left
    on: "orders.customer_id = customers.id"
    window: 5m
```

### Join types

| Type | Description |
|---|---|
| `inner` | Emit only when all sources have a matching key |
| `left` | Emit when the first source has a key, with right-side data if available |
| `right` | Emit when the last source has a key, with left-side data if available |
| `full` | Emit when any two sources have a matching key |

### ON clause

The `on` field specifies the join condition as `source_name.field = source_name.field`. Source names must match the `name:` of entries in the `sources:` array.

### Window

Optional `window` field (e.g., `5m`, `1h`) sets a time window for stream joins. Without a window, a simple in-memory hash join is used.

### Field conflicts

When multiple sources have the same field name, the output prefixes conflicting fields with the source name:

```json
{
  "orders.name": "Order #123",
  "customers.name": "Alice",
  "customer_id": "42",
  "amount": 99.50
}
```

### Validation rules

- `source` and `sources` cannot be defined together
- Each source in `sources` must have a unique `name`
- `join` is required when 2+ sources are defined
- `join.type` must be one of: `inner`, `left`, `right`, `full`
- `join.on` must not be empty
- A single source in `sources` triggers a warning (use `source` instead)
