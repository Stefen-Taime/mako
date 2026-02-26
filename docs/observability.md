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
mako_throughput_events_per_second{pipeline="order-events"} 1523.40
mako_uptime_seconds{pipeline="order-events"} 3600.0
mako_pipeline_ready{pipeline="order-events"} 1
```

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
