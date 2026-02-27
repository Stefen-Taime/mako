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
