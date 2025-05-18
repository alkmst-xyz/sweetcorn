# sweetcorn

A DuckDB backend for OpenTelemetry data.

## Features

- [x] Logs
  - [ ] Basic insert
  - [ ] Basic query
  - [ ] Create and query views
- [ ] Spans
- [ ] Metrics
- [ ] HTTP server
- [ ] Exporter for open telemetry collector
- [ ] Focus completly on DuckDB data types, OTEL -> database schema transformation, etc.
- [ ] TTL for rows (duck db does not provide it)
  - Table specific TTL configuration
- [ ] Refresh views periodically
  - This way the schemas will remain up to date
- [ ] Add configuration parameters for duckdb and add to `config.yaml`
- [ ] Compression:
  - DuckDB has built-in compression with lightweight compression algorithms.

### Development

```bash
make test
```

### DuckDB

```bash
curl https://install.duckdb.org | sh
```

## References

1. [Observability 3](https://charity.wtf/2025/03/24/another-observability-3-0-appears-on-the-horizon/)
2. [LogHouse](https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog#schema)
