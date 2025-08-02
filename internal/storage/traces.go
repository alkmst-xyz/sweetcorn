package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
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
	%s
ORDER BY
	timestamp DESC
LIMIT
	100;
`
)

type TraceRecord struct {
	TimestampTime      time.Time        `json:"timestamp"`
	TraceId            string           `json:"traceId"`
	SpanId             string           `json:"spanId"`
	ParentSpanId       string           `json:"parentSpanId"`
	TraceState         string           `json:"traceState"`
	SpanName           string           `json:"spanName"`
	SpanKind           string           `json:"spanKind"`
	ServiceName        string           `json:"serviceName"`
	ResourceAttributes map[string]any   `json:"resourceAttributes"`
	ScopeName          string           `json:"scopeName"`
	ScopeVersion       string           `json:"scopeVersion"`
	SpanAttributes     map[string]any   `json:"spanAttributes"`
	Duration           uint64           `json:"duration"`
	StatusCode         string           `json:"statusCode"`
	StatusMessage      string           `json:"statusMessage"`
	EventsTimestamps   []time.Time      `json:"eventsTimestamps"`
	EventsNames        []string         `json:"eventsNames"`
	EventsAttributes   []map[string]any `json:"eventsAttributes"`
	LinksTraceIds      []string         `json:"linksTraceIds"`
	LinksSpanIds       []string         `json:"linksSpanIds"`
	LinksTraceStates   []string         `json:"linksTraceStates"`
	LinksAttributes    []map[string]any `json:"linksAttributes"`
}

func convertEvents(events ptrace.SpanEventSlice) (times []time.Time, names, attrs []string, err error) {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())

		eventAttrBytes, eventAttrErr := json.Marshal(event.Attributes().AsRaw())
		if eventAttrErr != nil {
			return nil, nil, nil, fmt.Errorf("failed to marshal json trace event attributes: %w", eventAttrErr)
		}
		attrs = append(attrs, string(eventAttrBytes))
	}

	return
}

func convertLinks(links ptrace.SpanLinkSlice) (traceIDs, spanIDs, states, attrs []string, err error) {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, link.TraceID().String())
		spanIDs = append(spanIDs, link.SpanID().String())
		states = append(states, link.TraceState().AsRaw())

		linkAttrBytes, linkAttrErr := json.Marshal(link.Attributes().AsRaw())
		if linkAttrErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to marshal json trace link attributes: %w", linkAttrErr)
		}
		attrs = append(attrs, string(linkAttrBytes))
	}

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
	rsSpans := td.ResourceSpans()

	for i := range rsSpans.Len() {
		spans := rsSpans.At(i)
		res := spans.Resource()

		resAttr := res.Attributes()
		serviceName := getServiceName(resAttr)
		resAttrBytes, resAttrErr := json.Marshal(resAttr.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json trace resource attributes: %w", resAttrErr)
		}

		for j := range spans.ScopeSpans().Len() {
			scopeSpanRoot := spans.ScopeSpans().At(j)
			scopeSpanScope := scopeSpanRoot.Scope()
			scopeName := scopeSpanScope.Name()
			scopeVersion := scopeSpanScope.Version()
			scopeSpans := scopeSpanRoot.Spans()

			for k := range scopeSpans.Len() {
				span := scopeSpans.At(k)
				spanStatus := span.Status()

				spanDurationNanos := span.EndTimestamp() - span.StartTimestamp()

				spanAttrBytes, spanAttrErr := json.Marshal(span.Attributes().AsRaw())
				if spanAttrErr != nil {
					return fmt.Errorf("failed to marshal json trace span attributes: %w", spanAttrErr)
				}

				eventTimes, eventNames, eventAttrs, eventsErr := convertEvents(span.Events())
				if eventsErr != nil {
					return fmt.Errorf("failed to convert json trace events: %w", eventsErr)
				}

				linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs, linksErr := convertLinks(span.Links())
				if linksErr != nil {
					return fmt.Errorf("failed to convert json trace links: %w", linksErr)
				}

				_, err := db.ExecContext(ctx, insertSQL,
					span.StartTimestamp().AsTime(),
					span.TraceID().String(),
					span.SpanID().String(),
					span.ParentSpanID().String(),
					span.TraceState().AsRaw(),
					span.Name(),
					span.Kind().String(),
					serviceName,
					resAttrBytes,
					scopeName,
					scopeVersion,
					spanAttrBytes,
					spanDurationNanos,
					spanStatus.Code().String(),
					spanStatus.Message(),
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

		var eventsTimestamps duckdb.Composite[[]time.Time]
		var eventsNames duckdb.Composite[[]string]
		var eventsAttributes duckdb.Composite[[]map[string]any]

		var linksTraceIds duckdb.Composite[[]string]
		var linksSpanIds duckdb.Composite[[]string]
		var linksTraceStates duckdb.Composite[[]string]
		var linksAttributes duckdb.Composite[[]map[string]any]

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
			&eventsTimestamps,
			&eventsNames,
			&eventsAttributes,
			&linksTraceIds,
			&linksSpanIds,
			&linksTraceStates,
			&linksAttributes,
		)

		if err != nil {
			return nil, err
		}

		result.EventsTimestamps = eventsTimestamps.Get()
		result.EventsNames = eventsNames.Get()
		result.EventsAttributes = eventsAttributes.Get()

		result.LinksTraceIds = linksTraceIds.Get()
		result.LinksSpanIds = linksSpanIds.Get()
		result.LinksTraceStates = linksTraceStates.Get()
		result.LinksAttributes = linksAttributes.Get()

		results = append(results, result)
	}

	return results, nil
}
