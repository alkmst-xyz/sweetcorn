# sweetcorn

A DuckDB backend for OpenTelemetry data.

> **CAUTION:** This project is currently in **pre-alpha**.

## Quick start

```bash
cd example
docker compose up -d
```

| Service      | URL                    |
| ------------ | ---------------------- |
| OTLP gRPC    | localhost:4317         |
| OTLP HTTP    | http://localhost:4318  |
| Sweetcorn UI | http://localhost:13579 |
| ---          | ---                    |
| Grafana      | http://localhost:3000  |
| HotROD       | http://localhost:8080  |

## Features

- [x] Logs
  - [x] Basic insert
  - [x] Basic query
  - [x] Create and query views
- [x] Traces
- [ ] Metrics
- [x] Basic HTTP server
- [x] Basic GRPC server
- [x] Handle protobuf payload
- [x] Handle JSON payload in HTTP
- [ ] Docker Image
- [ ] ~~Exporter for open telemetry collector~~: not planned for v0.1.0.
- [ ] TTL for rows (duck db does not provide it)
  - Table specific TTL configuration
- [ ] Refresh views periodically
  - This way the schemas will remain up to date
- [ ] Add configuration parameters for DuckDB and add to `config.yaml`
- [ ] Explore compression:
  - DuckDB has built-in compression with lightweight compression algorithms.
  - Validate compression is working.

### Development

```bash
make test
```

### DuckDB

```bash
curl https://install.duckdb.org | sh
```

### Telemetry generation

Mock telemetry data can be generated using [`telemetrygen`](github.com/opentelemetry-collector-contrib/cmd/telemetrygen@latest).
Run `make dev-tools` to install it (installed in `$(go env GOPATH)/bin`).

```bash
# example: send logs/traces via grpc
telemetrygen logs --otlp-insecure --otlp-endpoint localhost:4317 --duration 5s
telemetrygen traces --otlp-insecure --otlp-endpoint localhost:4317 --duration 5s

# example: send logs/traces via http
telemetrygen logs --otlp-http --otlp-insecure --otlp-endpoint localhost:4318 --duration 5s
telemetrygen traces --otlp-http --otlp-insecure --otlp-endpoint localhost:4318 --duration 5s
```

## References

1. [Observability 3](https://charity.wtf/2025/03/24/another-observability-3-0-appears-on-the-horizon/)
2. [LogHouse](https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog#schema)
