package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	createTracesTableSQL = `
CREATE SEQUENCE IF NOT EXISTS otel_traces_id_seq;

CREATE TABLE IF NOT EXISTS %s (
	_id						BIGINT PRIMARY KEY DEFAULT nextval ('otel_traces_id_seq'),
	timestamp				TIMESTAMP_NS,
	timestamp_time			TIMESTAMP_S GENERATED ALWAYS AS (CAST(Timestamp AS TIMESTAMP)),
	trace_id				VARCHAR,
	span_id					VARCHAR,
	parent_span_id			VARCHAR,
	trace_state				VARCHAR,
	span_name				VARCHAR,
	span_kind				VARCHAR,
	service_name			VARCHAR,
	resource_attributes		JSON,
	scope_name				VARCHAR,
	scope_version			VARCHAR,
	span_attributes			JSON,
	duration				UBIGINT,
	status_code				VARCHAR,
	status_message			VARCHAR,
	events_timestamps		TIMESTAMP_NS[],
	events_names			VARCHAR[],
	events_attributes		JSON[],
	links_trace_ids			VARCHAR[],
	links_span_ids			VARCHAR[],
	links_trace_states		VARCHAR[],
	links_attributes		JSON[]
);`

	insertTracesSQLTemplate = `INSERT INTO %s (
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryTracesSQLTemplate = `SELECT
	timestamp_time,
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
	%s
ORDER BY
	timestamp_time DESC
LIMIT
	100;
`
)

type TraceRecord struct {
	TimestampTime      time.Time                          `json:"timestamp"`
	TraceId            string                             `json:"traceId"`
	SpanId             string                             `json:"spanId"`
	ParentSpanId       string                             `json:"parentSpanId"`
	TraceState         string                             `json:"traceState"`
	SpanName           string                             `json:"spanName"`
	SpanKind           string                             `json:"spanKind"`
	ServiceName        string                             `json:"serviceName"`
	ResourceAttributes map[string]any                     `json:"resourceAttributes"`
	ScopeName          string                             `json:"scopeName"`
	ScopeVersion       string                             `json:"scopeVersion"`
	SpanAttributes     map[string]any                     `json:"spanAttributes"`
	Duration           uint64                             `json:"duration"`
	StatusCode         string                             `json:"statusCode"`
	StatusMessage      string                             `json:"statusMessage"`
	EventsTimestamps   duckdb.Composite[[]uint64]         `json:"eventsTimestamps"`
	EventsNames        duckdb.Composite[[]string]         `json:"eventsNames"`
	EventsAttributes   duckdb.Composite[[]map[string]any] `json:"eventsAttributes"`
	LinksTraceIds      duckdb.Composite[[]string]         `json:"linksTraceIds"`
	LinksSpanIds       duckdb.Composite[[]string]         `json:"linksSpanIds"`
	LinksTraceStates   duckdb.Composite[[]string]         `json:"linksTraceStates"`
	LinksAttributes    duckdb.Composite[[]map[string]any] `json:"linksAttributes"`
}

func convertEvents(events ptrace.SpanEventSlice) (times []time.Time, names []string, attrs []byte) {
	var attrsRaw []pcommon.Map
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())
		attrsRaw = append(attrsRaw, event.Attributes())
	}
	attrs = attributesArrayToBytes(attrsRaw)
	return
}

func convertLinks(links ptrace.SpanLinkSlice) (traceIDs []string, spanIDs []string, states []string, attrs []byte) {
	var attrsRaw []pcommon.Map
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, traceIDToHexOrEmptyString(link.TraceID()))
		spanIDs = append(spanIDs, spanIDToHexOrEmptyString(link.SpanID()))
		states = append(states, link.TraceState().AsRaw())
		attrsRaw = append(attrsRaw, link.Attributes())
	}
	attrs = attributesArrayToBytes(attrsRaw)
	return
}

func renderCreateTracesTableSQL(cfg *Config) string {
	return fmt.Sprintf(createTracesTableSQL, cfg.TracesTableName)
}

func CreateTracesTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateTracesTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create traces table sql: %w", err)
	}
	return nil
}

func RenderInsertTracesSQL(cfg *Config) string {
	return fmt.Sprintf(insertTracesSQLTemplate, cfg.TracesTableName)
}

func RenderQueryTracesSQL(cfg *Config) string {
	return fmt.Sprintf(queryTracesSQLTemplate, cfg.TracesTableName)
}

func InsertTracesData(ctx context.Context, db *sql.DB, insertSQL string, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		spans := td.ResourceSpans().At(i)
		res := spans.Resource()
		resAttr := attributesToBytes(res.Attributes())
		serviceName := getServiceName(res.Attributes())

		for j := 0; j < spans.ScopeSpans().Len(); j++ {
			rs := spans.ScopeSpans().At(j).Spans()
			scopeName := spans.ScopeSpans().At(j).Scope().Name()
			scopeVersion := spans.ScopeSpans().At(j).Scope().Version()
			for k := 0; k < rs.Len(); k++ {
				r := rs.At(k)
				spanAttr := attributesToBytes(r.Attributes())
				status := r.Status()

				var eventTimes []time.Time
				var eventAttrs []map[string]any
				var linksAttrs []map[string]any

				_, eventNames, _ := convertEvents(r.Events())
				linksTraceIDs, linksSpanIDs, linksTraceStates, _ := convertLinks(r.Links())

				_, err := db.ExecContext(ctx, insertSQL,
					r.StartTimestamp().AsTime(),
					traceIDToHexOrEmptyString(r.TraceID()),
					spanIDToHexOrEmptyString(r.SpanID()),
					spanIDToHexOrEmptyString(r.ParentSpanID()),
					r.TraceState().AsRaw(),
					r.Name(),
					r.Kind().String(),
					serviceName,
					resAttr,
					scopeName,
					scopeVersion,
					spanAttr,
					r.EndTimestamp().AsTime().Sub(r.StartTimestamp().AsTime()).Nanoseconds(),
					status.Code().String(),
					status.Message(),
					eventTimes,
					eventNames,
					eventAttrs,
					linksTraceIDs,
					linksSpanIDs,
					linksTraceStates,
					linksAttrs,
				)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func QueryTraces(ctx context.Context, db *sql.DB, queryLogsSQL string) ([]TraceRecord, error) {
	rows, err := db.QueryContext(ctx, queryLogsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]TraceRecord, 0)

	for rows.Next() {
		var result TraceRecord

		err := rows.Scan(
			&result.TimestampTime,
			&result.TraceId,
			&result.SpanId,
			&result.ParentSpanId,
			&result.TraceState,
			&result.SpanName,
			&result.SpanKind,
			&result.ServiceName,
			&result.ResourceAttributes,
			&result.ScopeName,
			&result.ScopeVersion,
			&result.SpanAttributes,
			&result.Duration,
			&result.StatusCode,
			&result.StatusMessage,
			&result.EventsTimestamps,
			&result.EventsNames,
			&result.EventsAttributes,
			&result.LinksTraceIds,
			&result.LinksSpanIds,
			&result.LinksTraceStates,
			&result.LinksAttributes,
		)

		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}
