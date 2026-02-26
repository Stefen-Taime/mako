# Codegen

Generate deployment artifacts from your pipeline YAML.

## Kubernetes manifests

```bash
mako generate pipeline.yaml --k8s
```

Generates: Deployment, Service, ConfigMap, HPA.

## Terraform

```bash
mako generate pipeline.yaml --tf
```

Generates: Kafka topics, Snowflake tables/pipes, Schema Registry subjects, DLQ topics.
