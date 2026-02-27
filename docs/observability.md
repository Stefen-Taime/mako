# Observability

When running `mako run`, an HTTP server exposes Prometheus metrics, health probes, and pipeline status.

```yaml
monitoring:
  freshnessSLA: 5m
  alertChannel: "#data-alerts"
  metrics:
    enabled: true
    port: 9090
```

## Endpoints

| Endpoint | Description | Use |
|----------|-------------|-----|
| `GET /metrics` | Prometheus text format | Scraping by Prometheus/Grafana |
| `GET /health` | Liveness probe (always 200) | Kubernetes `livenessProbe` |
| `GET /ready` | Readiness probe (200 when pipeline running) | Kubernetes `readinessProbe` |
| `GET /status` | Pipeline status JSON | Monitoring dashboards |

## Prometheus Metrics

```text
mako_events_in_total{pipeline="order-events"} 15234
mako_events_out_total{pipeline="order-events"} 15230
mako_errors_total{pipeline="order-events"} 4
mako_dlq_total{pipeline="order-events"} 2
mako_schema_failures_total{pipeline="order-events"} 1
mako_sink_latency_microseconds{pipeline="order-events"} 4523
mako_throughput_events_per_second{pipeline="order-events"} 1523.40
mako_uptime_seconds{pipeline="order-events"} 3600.0
mako_pipeline_ready{pipeline="order-events"} 1
```

### Real-time metrics

Metrics are synced from the pipeline engine to the observability server every 500ms. The `/metrics` and `/status` endpoints reflect live counters during pipeline execution, not just final stats at shutdown. A final sync is performed after `pipeline.Stop()` to capture events flushed during graceful shutdown.

## Kubernetes Probes

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

## Schema Enforcement

Validate events against Confluent Schema Registry at runtime. Supports **JSON Schema** and **Avro** schema types.

```yaml
schema:
  enforce: true
  registry: http://schema-registry:8081
  subject: events.orders-value
  compatibility: BACKWARD
  onFailure: reject     # reject | dlq | log
  dlqTopic: events.orders.dlq
```

### JSON Schema

- Type checking (string, number, integer, boolean, object, array, null)
- Required fields enforcement
- `additionalProperties: false` support

### Avro

Powered by [hamba/avro](https://github.com/hamba/avro) (pure Go, high performance).

- Full record validation (field types, required fields, defaults)
- Numeric type coercion (JSON `float64` -> Avro `int`, `long`, `float`)
- Union types (`["null", "string"]`)
- Nested records and arrays
- Enum validation

Example Avro schema in the registry:

```json
{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "quantity", "type": "int"},
    {"name": "status", "type": ["null", "string"], "default": null}
  ]
}
```

### Common features

- Schema caching (fetched once, refreshable)
- Failed events routed to DLQ or rejected
- Automatic schema type detection from Registry (`schemaType` field)

## Fault Isolation

Each pipeline runs independently. Failed events go to a Dead Letter Queue instead of blocking the pipeline.

```yaml
isolation:
  strategy: per_event_type
  maxRetries: 3
  backoffMs: 1000
  dlqEnabled: true
```

Retry flow: sink write failure -> exponential backoff -> retry (up to N times) -> DLQ -> degrade state.
