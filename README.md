# Relay — Eventing and Caching SDK

A Go SDK for building resilient Kafka-based microservices with built-in retries, circuit breaking, caching, and observability.

## Features

- **Producer** — Kafka publisher with idempotency, retry, and circuit breaker
- **Consumer** — At-least-once delivery with automatic dead-letter queue (DLQ) routing
- **Cache** — Redis read-through cache with stampede protection via singleflight
- **Telemetry** — Prometheus metrics and OpenTelemetry tracing out of the box

## Project Structure

```
sdk/
  producer/   # Kafka producer
  consumer/   # Kafka consumer with DLQ
  cache/      # Redis cache
  breaker/    # Circuit breaker
  telemetry/  # Metrics and tracing
services/
  order-service/          # Example producer service (HTTP → Kafka)
  notification-consumer/  # Example consumer service (Kafka → Redis → notify)
e2e/          # End-to-end tests
integration/  # Integration tests (testcontainers)
```

## Quick Start

```bash
# Start the full stack (Kafka, Redis, services, Prometheus, Grafana, Jaeger)
docker compose up --build
```

| Service       | URL                    |
|---------------|------------------------|
| Order API     | http://localhost:8080  |
| Grafana       | http://localhost:3000  |
| Jaeger UI     | http://localhost:16686 |
| Prometheus    | http://localhost:9093  |

Publish an order:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "u1", "items": ["item-a"]}'
```

## Running Tests

```bash
# Unit tests
go test ./internal/... ./sdk/...

# Integration tests (requires Docker)
go test -tags integration ./integration/

# E2E tests (requires Docker)
go test -tags e2e ./e2e/
```

## Tech Stack

Go · Kafka · Redis · Prometheus · OpenTelemetry · Jaeger · testcontainers
