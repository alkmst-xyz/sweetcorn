# Queries

## Logs

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

- [ ] Find log with body contain string token.

```sql
SELECT Timestamp as log_time, Body
FROM otel_logs
WHERE 'message' IN Body
  AND TimestampTime >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [ ] Find log with body contain string.

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

## Traces

- [x] Find spans with specific attribute.

```sql
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
WHERE ServiceName = 'telemetrygen'
  -- AND SpanAttributes['peer.service'] = 'telemetrygen-server'
  -- AND Timestamp >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [ ] Find traces with traceID (using time primary index and TraceID skip index).

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

- [x] Find spans is error.

```sql
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
WHERE ServiceName = 'telemetrygen'
  AND StatusCode = 'Unset'
  -- AND Timestamp >= NOW() - INTERVAL 1 HOUR
Limit 100;
```

- [x] Find slow spans.

```sql
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
WHERE ServiceName = 'telemetrygen'
  AND Duration > 1 * 1e9
  -- AND Timestamp >= NOW() - INTERVAL 1 HOUR
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
