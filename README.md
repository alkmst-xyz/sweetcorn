<div align="center">
	<img
		alt="Sweetcorn logo"
		src="./docs/assets/sweetcorn-brand-assets-v1/sweetcorn-logo.png"
		height="100"
	/>
</div>

<h1 align="center">Sweetcorn</h1>

<p align="center">
	<a href="https://goreportcard.com/report/github.com/alkmst-xyz/sweetcorn">
		<img
			src="https://goreportcard.com/badge/github.com/alkmst-xyz/sweetcorn"
			alt="Go Report Card"
		/>
	</a>
	<a href="https://opensource.org/licenses/Apache-2.0">
		<img
			src="https://img.shields.io/badge/License-Apache%202.0-blue.svg"
			alt="License"
		/>
	</a>
</p>

Sweetcorn is a DuckDB/DuckLake storage backend for OpenTelemetry signals.

> **CAUTION:** This project is currently in **pre-alpha**.

## Quick start

```bash
cd examples/example
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
- [ ] Traces
  - [x] Basic ingest
  - [x] Basic query
  - [ ] Determine `ProcessID`
- [ ] Metrics
  - [x] Gauge
  - [x] Sum
  - [x] Histogram
  - [x] Exponential Histogram
  - [x] Summary
  - [ ] Support Exemplars
- [x] Basic HTTP server
- [x] Basic GRPC server
- [x] Handle protobuf payload
- [x] Handle JSON payload in HTTP
- [x] Docker Image
- [ ] Use `zap` logger.
- [ ] ~~Exporter for open telemetry collector~~: not planned for v0.1.0.
- [ ] TTL for rows (duck db does not provide it)
  - Table specific TTL configuration
- [ ] Refresh views periodically
  - This way the schemas will remain up to date
- [ ] Add configuration parameters for DuckDB and add to `config.yaml`
- [ ] Explore compression:
  - DuckDB has built-in compression with lightweight compression algorithms.
  - Validate compression is working.
- [ ] User defined functions, specific to o11ty.
- [ ] Create real world demo application (like HotROD).
  - [x] Logs
  - [x] Traces
  - [x] Metrics
  - [ ] Exemplars
  - [ ] Serve simple web UI
  - [ ] Move to `cmd/demo`
- [ ] Blue/green deployment example
  - Demo -> OTEL collector
  - OTEL collector -> Loki + Prometheus + Jaeger
  - OTEL collector -> Sweetcorn (+ Postgres, MinIO)
  - Visualize with Grafana
  - Consider comparing with Grafana stack (alloy+loki+mimir+tempo).

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
