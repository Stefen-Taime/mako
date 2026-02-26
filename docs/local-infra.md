# Local Infrastructure

The `docker/` directory contains a full local stack for development.

## Quick start

```bash
cd docker/
docker compose up -d           # Start all services
./create-topics.sh             # Create Kafka topics
./produce-sample.sh            # Produce test events
```

## Services

| Service | Port | Description |
|---|---|---|
| Kafka (KRaft) | `localhost:9092` | Event broker |
| Schema Registry | `localhost:8081` | Confluent Schema Registry |
| PostgreSQL | `localhost:5432` | Sink database (user: `mako`, pass: `mako`) |
| Flink SQL | `localhost:8082` | Stream processing dashboard |
| Kafka UI | `localhost:8080` | Web UI for topics and messages |
