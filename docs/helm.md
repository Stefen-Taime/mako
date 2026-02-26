# Helm Chart

Deploy Mako pipelines to Kubernetes with the official Helm chart.

## Install

```bash
helm install my-pipeline ./charts/mako
```

## Custom pipeline

```bash
helm install my-pipeline ./charts/mako \
  --set-file pipeline.config=pipeline.yaml
```

## Override values

```bash
helm install my-pipeline ./charts/mako \
  --set replicaCount=2 \
  --set image.tag=latest \
  --set metrics.serviceMonitor.enabled=true
```

## Pass secrets via environment

```yaml
# values-prod.yaml
pipeline:
  env:
    - name: POSTGRES_DSN
      valueFrom:
        secretKeyRef:
          name: mako-secrets
          key: postgres-dsn
    - name: SNOWFLAKE_PASSWORD
      valueFrom:
        secretKeyRef:
          name: mako-secrets
          key: snowflake-password
```

```bash
helm install my-pipeline ./charts/mako -f values-prod.yaml
```

## Chart contents

| Template | Description |
|---|---|
| `deployment.yaml` | Pipeline Deployment with config volume, probes, env |
| `service.yaml` | ClusterIP Service exposing metrics port |
| `configmap.yaml` | Pipeline YAML mounted at `/etc/mako/pipeline.yaml` |
| `serviceaccount.yaml` | Optional ServiceAccount with annotations |
| `servicemonitor.yaml` | Prometheus ServiceMonitor (opt-in) |

## Features

- ConfigMap checksum annotation for automatic rollout on config change
- Liveness/readiness probes via `/metrics` endpoint
- Extra volumes for WASM plugins
- ServiceMonitor for Prometheus Operator
- Full resource limits, node selectors, tolerations, affinity support
