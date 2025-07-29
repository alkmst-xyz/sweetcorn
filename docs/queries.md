# Queries

## Logs

- [x] Get log severity count time series.

```sql
SELECT
    timestamp_time as time,
    severity_text,
    count() as count
FROM
    otel_logs
WHERE
    CAST(time AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
GROUP BY
    severity_text,
    time
ORDER BY
    time;
```

- [x] Find any log.

```sql
SELECT
    timestamp,
    body
FROM
    otel_logs
WHERE
    CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find log with specific service.

```sql
SELECT
    timestamp,
    body
FROM
    otel_logs
WHERE
    service_name = 'telemetrygen'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find log with specific attribute.

[DuckDB - JSON functions](https://duckdb.org/docs/stable/data/json/json_functions)

```sql
SELECT
    timestamp,
    body
FROM
    otel_logs
WHERE
    (log_attributes->>'$.app') = 'server'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find log with body contain string token.

```sql
SELECT
    timestamp,
    body
FROM
    otel_logs
WHERE
    'message' IN body
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find log with body contain string.

```sql
SELECT
    timestamp,
    body
FROM
    otel_logs
WHERE
    body LIKE '%mes%'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find log with body regexp match string.

[DuckDB - Pattern matching](https://duckdb.org/docs/stable/sql/functions/pattern_matching)

```sql
SELECT
    timestamp,
    body
FROM
    otel_logs
WHERE
    BODY GLOB '*message*'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

## Traces

- [x] Find spans with specific attribute.

```sql
SELECT
    timestamp,
    trace_id,
    span_id,
    parent_span_id,
    trace_state,
    span_name,
    span_kind,
    service_name,
    resource_attributes,
    scope_name,
    scope_version,
    span_attributes,
    duration,
    status_code,
    status_message,
    events_timestamps,
    events_names,
    events_attributes,
    links_trace_ids,
    links_span_ids,
    links_trace_states,
    links_attributes
FROM
    otel_traces
WHERE
    service_name = 'telemetrygen'
    AND (span_attributes->>'$."peer.service"') = 'telemetrygen-server'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find spans is error.

```sql
SELECT
    timestamp,
    trace_id,
    span_id,
    parent_span_id,
    trace_state,
    span_name,
    span_kind,
    service_name,
    resource_attributes,
    scope_name,
    scope_version,
    span_attributes,
    duration,
    status_code,
    status_message,
    events_timestamps,
    events_names,
    events_attributes,
    links_trace_ids,
    links_span_ids,
    links_trace_states,
    links_attributes
FROM
    otel_traces
WHERE
    service_name = 'telemetrygen'
    AND status_code = 'Unset'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Find slow spans.

```sql
SELECT
    timestamp,
    trace_id,
    span_id,
    parent_span_id,
    trace_state,
    span_name,
    span_kind,
    service_name,
    resource_attributes,
    scope_name,
    scope_version,
    span_attributes,
    duration,
    status_code,
    status_message,
    events_timestamps,
    events_names,
    events_attributes,
    links_trace_ids,
    links_span_ids,
    links_trace_states,
    links_attributes
FROM
    otel_traces
WHERE
    service_name = 'telemetrygen'
    AND duration > 1 * 1e5
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Search all spans by operation.

```sql
SELECT
    timestamp,
    trace_id,
    span_id,
    parent_span_id,
    trace_state,
    span_name,
    span_kind,
    service_name,
    resource_attributes,
    scope_name,
    scope_version,
    span_attributes,
    duration,
    status_code,
    status_message,
    events_timestamps,
    events_names,
    events_attributes,
    links_trace_ids,
    links_span_ids,
    links_trace_states,
    links_attributes
FROM
    otel_traces
WHERE
    span_name = 'okey-dokey-0'
    AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
LIMIT
    100;
```

- [x] Retrieve all distinct services.

```sql
SELECT DISTINCT
    service_name
FROM
    otel_traces;
```

- [x] Retrieve all distinct operations.

```sql
SELECT DISTINCT
    span_name
FROM
    otel_traces;
```

- [ ] Find traces with traceID (using time primary index and TraceID skip index).

TODO: requires dedicated `otel_traces_trace_id_ts` table.

```sql
WITH
    trace_id AS '6562c33b75559ec1c7eca186d3cc1023',
    start AS (SELECT min(Start) FROM otel_traces_trace_id_ts WHERE TraceId = trace_id),
    end AS (SELECT max(End) + 1 FROM otel_traces_trace_id_ts WHERE TraceId = trace_id)
SELECT
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    SpanName,
    SpanKind,
    ServiceName,
    Duration,
    StatusCode,
    StatusMessage,
    SpanAttributes,
    ResourceAttributes,
    EventsName,
    LinksTraceId
FROM otel_traces
WHERE TraceId = trace_id
  AND Timestamp >= start
  AND Timestamp <= end
Limit 100;
```

## Indexes

Add indexes to improve query performance.

- Time range: "Show me logs from t1 to t2."
- By service: "Show me all logs for ServiceName=X in a time range."
- Trace/span correlation: "Show logs related to TraceId=abc123."
- Search by attribute: "Show logs where LogAttributes->'user_id' = '123'."
- Severity: "Show me ERROR logs only."

```sql
-- Time range
CREATE INDEX IF NOT EXISTS idx_otel_logs_timestamp_time		ON otel_logs (timestamp_time);

-- Trace/Span correlation
CREATE INDEX IF NOT EXISTS idx_otel_logs_trace_id_span_id	ON otel_logs (trace_id, span_id);

-- Severity
CREATE INDEX IF NOT EXISTS idx_otel_logs_severity_number	ON otel_logs (severity_number);
CREATE INDEX IF NOT EXISTS idx_otel_logs_service_name		ON otel_logs (service_name);
```

## TTL (time-to-live)

```sql
DELETE FROM "otel_logs"
WHERE
    CAST(timestamp_time AS TIMESTAMP) < NOW () - INTERVAL '30 days';
```

## References

1. [ClickHouse Exporter for OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
