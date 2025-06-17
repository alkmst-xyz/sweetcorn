# sweetcorn

A DuckDB backend for OpenTelemetry data.

## Features

- [x] Logs
  - [x] Basic insert
  - [x] Basic query
  - [x] Create and query views
- [x] Traces
- [ ] Metrics
- [ ] Basic HTTP server
  - [x] Handle protobuf payload
- [ ] Exporter for open telemetry collector
- [ ] Focus completely on DuckDB data types, OTEL -> database schema transformation, etc.
- [ ] TTL for rows (duck db does not provide it)
  - Table specific TTL configuration
- [ ] Refresh views periodically
  - This way the schemas will remain up to date
- [ ] Add configuration parameters for DuckDB and add to `config.yaml`
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

### Telemetry generation

Mock telemetry data can be generated using [`telemetrygen`](github.com/opentelemetry-collector-contrib/cmd/telemetrygen@latest).

`telemetrygen` is installed as a `go tool` (check the tools directive in [go.mod](./go.mod) for the exact version).

```bash
# example: generate logs for 5 seconds
go tool telemetrygen logs --otlp-http --otlp-insecure --otlp-endpoint localhost:8090 --duration 5s
```

## Query

### Logs

- [x] Get log severity count time series.

```sql
SELECT TimestampTime as time, SeverityText, count() as count
FROM otel_logs
WHERE time >= NOW() - INTERVAL 1 HOUR
GROUP BY SeverityText, time
ORDER BY time;
```

- [x] Find any log.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [x] Find log with specific service.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE ServiceName = 'telemetrygen'
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [ ] Find log with specific attribute.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE LogAttributes['container_name'] = '/example_flog_1'
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- Find log with body contain string token.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE 'message' IN Body
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- Find log with body contain string.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE Body LIKE '%mes%'
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [x] Find log with body regexp match string.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE BODY GLOB '*'
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [ ] Find log with body json extract.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE JSONExtractFloat(Body, 'bytes') > 1000
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

## References

1. [Observability 3](https://charity.wtf/2025/03/24/another-observability-3-0-appears-on-the-horizon/)
2. [LogHouse](https://clickhouse.com/blog/building-a-logging-platform-with-clickhouse-and-saving-millions-over-datadog#schema)
