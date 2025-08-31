package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	createTracesTableSQL = `
CREATE TABLE IF NOT EXISTS
	otel_traces (
		ts						TIMESTAMP_NS,
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

	insertTracesSQL = `
INSERT INTO
	otel_traces (
		ts,
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
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryTracesSQL = `
SELECT
	ts,
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
ORDER BY
	ts DESC
LIMIT
	100;`

	queryServicesSQL = `
SELECT DISTINCT
    service_name
FROM
	otel_traces
LIMIT
	100;`

	traceOperationsSQL = `
SELECT DISTINCT
    span_name
FROM
	otel_traces
WHERE
	service_name = ?
	AND (? = '' OR span_kind = ?)
LIMIT
	100;`

	// Note:
	// When using `struct_pack`, match it to the name of the golang struct field.
	// This is useful when we deserialize during query. See `GetTraces()`.
	// TODO:
	// - AND CAST(ts AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
	// - AND (span_attributes->>'$."peer.service"') = 'telemetrygen-server'
	tracesSQL = `
SELECT
	trace_id,
	array_agg(
		struct_pack(
			"TraceID" := trace_id,
			"SpanID" := span_id,
			"OperationName" := span_name,
			"StartTime" := epoch_us(ts),
			"Duration" := duration // 1000,
			"ParentName" := parent_span_id,
			"SpanAttributes" := span_attributes,
			"ScopeName" := scope_name,
			"SpanKind" := span_kind
		)
	) as spans
FROM
	otel_traces
WHERE
	(? IS NULL OR service_name = ?)
GROUP BY
	trace_id
LIMIT
	100;`

	traceSQL = `
SELECT
	trace_id,
	array_agg(
		struct_pack(
			"TraceID" := trace_id,
			"SpanID" := span_id,
			"OperationName" := span_name,
			"StartTime" := epoch_us(ts),
			"Duration" := duration // 1000,
			"ParentName" := parent_span_id,
			"SpanAttributes" := span_attributes,
			"ScopeName" := scope_name,
			"SpanKind" := span_kind
		)
	) as spans
FROM
	otel_traces
WHERE
	trace_id = ?
GROUP BY
	trace_id;`

	dependenciesSQL = `
SELECT DISTINCT
    (
        SELECT
            p.service_name
        FROM
            otel_traces AS p
        WHERE
            p.span_id = c.parent_span_id
    ) AS parent_service_name,
    c.service_name AS child_service_name,
    COUNT(*) AS count
FROM
    otel_traces as c
WHERE
    c.parent_span_id != ''
GROUP BY
    parent_service_name,
    child_service_name;`
)

type TraceRecord struct {
	Timestamp          int64            `json:"timestamp"`
	TraceID            string           `json:"traceID"`
	SpanID             string           `json:"spanID"`
	ParentSpanID       string           `json:"parentSpanID"`
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
	LinksTraceIDs      []string         `json:"linksTraceIDs"`
	LinksSpanIDs       []string         `json:"linksSpanIDs"`
	LinksTraceStates   []string         `json:"linksTraceStates"`
	LinksAttributes    []map[string]any `json:"linksAttributes"`
}

// Jaeger Query

type TraceKeyValuePair struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type TraceProcess struct {
	ServiceName string              `json:"serviceName"`
	Tags        []TraceKeyValuePair `json:"tags"`
}

type TraceSpanReference struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
	TraceID string `json:"traceID"`
}

// Note: Millisecond epoch time
type TraceLog struct {
	Timestamp int64               `json:"timestamp"`
	Fields    []TraceKeyValuePair `json:"fields"`
	Name      string              `json:"name"`
}

// Note: Times are in microseconds
type Span struct {
	TraceID        string               `json:"traceID"`
	SpanID         string               `json:"spanID"`
	ProcessID      string               `json:"processID"`
	OperationName  string               `json:"operationName"`
	StartTime      int64                `json:"startTime"`
	Duration       int64                `json:"duration"`
	Logs           []TraceLog           `json:"logs"`
	References     []TraceSpanReference `json:"references"`
	Tags           []TraceKeyValuePair  `json:"tags"`
	Warnings       []string             `json:"warnings"`
	Flags          int                  `json:"flags"`
	StackTraces    []string             `json:"stackTraces"`
	ParentName     string               `json:"-"` // TODO: remove
	SpanAttributes map[string]any       `json:"-"` // TODO: remove
	ScopeName      string               `json:"-"` // TODO: remove
	SpanKind       string               `json:"-"` // TODO: remove

}

type TraceResponse struct {
	Processes map[string]TraceProcess `json:"processes"`
	TraceID   string                  `json:"traceID"`
	Warnings  []string                `json:"warnings"`
	Spans     []Span                  `json:"spans"`
}

type DependenciesData struct {
	ParentServiceName string `json:"parent"`
	ChildServiceName  string `json:"child"`
	Count             int    `json:"callCount"`
}

var ErrTraceNotFound = errors.New("trace not found")

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

func CreateTracesTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, createTracesTableSQL); err != nil {
		return fmt.Errorf("exec create traces table sql: %w", err)
	}
	return nil
}

func InsertTracesData(ctx context.Context, db *sql.DB, td ptrace.Traces) error {
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

				_, err := db.ExecContext(ctx, insertTracesSQL,
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

func QueryTraces(ctx context.Context, db *sql.DB) ([]TraceRecord, error) {
	rows, err := db.QueryContext(ctx, queryTracesSQL)
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

		var timestamp time.Time
		var duration uint64

		err := rows.Scan(
			&timestamp,
			&result.TraceID,
			&result.SpanID,
			&result.ParentSpanID,
			&result.TraceState,
			&result.SpanName,
			&result.SpanKind,
			&result.ServiceName,
			&result.ResourceAttributes,
			&result.ScopeName,
			&result.ScopeVersion,
			&result.SpanAttributes,
			&duration,
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

		result.LinksTraceIDs = linksTraceIds.Get()
		result.LinksSpanIDs = linksSpanIds.Get()
		result.LinksTraceStates = linksTraceStates.Get()
		result.LinksAttributes = linksAttributes.Get()

		// convert nanoseconds to milliseconds
		result.Duration = duration / 1000

		// convert timestamp to unix epoch in microseconds
		result.Timestamp = timestamp.UnixMicro()

		results = append(results, result)
	}

	return results, nil
}

func GetServices(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, queryServicesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]string, 0)
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			log.Fatal(err)
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return results, nil
}

type TraceOperationsParams struct {
	ServiceName string
	SpanKind    string
}

func TraceOperations(ctx context.Context, db *sql.DB, params TraceOperationsParams) ([]string, error) {
	rows, err := db.QueryContext(ctx, traceOperationsSQL,
		params.ServiceName,
		params.SpanKind,
		params.SpanKind,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]string, 0)
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			log.Fatal(err)
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return results, nil
}

type SearchTracesParams struct {
	ServiceName   *string
	OperationName *string
	Tags          *map[string]string
	StartTimeMin  *time.Time
	StartTimeMax  *time.Time
	DurationMin   *time.Duration
	DurationMax   *time.Duration
	NumTraces     *int
}

func SearchTraces(ctx context.Context, db *sql.DB, params SearchTracesParams) ([]TraceResponse, error) {
	rows, err := db.QueryContext(ctx, tracesSQL,
		params.ServiceName,
		params.ServiceName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]TraceResponse, 0)
	for rows.Next() {
		var result TraceResponse
		var spans duckdb.Composite[[]Span]

		err := rows.Scan(
			&result.TraceID,
			&spans,
		)

		if err != nil {
			log.Fatal(err)
		}

		// processes
		// TODO: hardcoded
		result.Processes = make(map[string]TraceProcess)
		result.Processes["p1"] = TraceProcess{
			ServiceName: "telemetrygen",
			Tags:        []TraceKeyValuePair{},
		}

		// spans
		spansRaw := spans.Get()
		for i := range len(spansRaw) {
			span := spansRaw[i]

			span.Logs = make([]TraceLog, 0)
			span.References = make([]TraceSpanReference, 0)
			span.Tags = make([]TraceKeyValuePair, 0)
			span.StackTraces = make([]string, 0)

			// TODO: hardcoded
			span.ProcessID = "p1"

			if span.ParentName != "" {
				reference := TraceSpanReference{
					RefType: "CHILD_OF",
					TraceID: result.TraceID,
					SpanID:  span.ParentName,
				}
				span.References = append(span.References, reference)
			}

			// tags
			span.Tags = make([]TraceKeyValuePair, 0)
			for key, value := range span.SpanAttributes {
				span.Tags = append(span.Tags, TraceKeyValuePair{
					Key:   key,
					Type:  "string",
					Value: value,
				})
			}
			span.Tags = append(span.Tags, TraceKeyValuePair{
				Key:   "otel.scope.name",
				Type:  "string",
				Value: span.ScopeName,
			})
			span.Tags = append(span.Tags, TraceKeyValuePair{
				Key:   "span.kind",
				Type:  "string",
				Value: span.SpanKind,
			})

			result.Spans = append(result.Spans, span)
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return results, nil
}

type TraceParams struct {
	TraceID   string
	StartTime *time.Time
	EndTime   *time.Time
}

func Trace(ctx context.Context, db *sql.DB, params TraceParams) (TraceResponse, error) {
	row := db.QueryRowContext(ctx, traceSQL, params.TraceID)

	var result TraceResponse
	var spans duckdb.Composite[[]Span]

	err := row.Scan(
		&result.TraceID,
		&spans,
	)
	if err == sql.ErrNoRows {
		return result, ErrTraceNotFound
	}
	if err != nil {
		return result, err
	}

	// processes
	// TODO: hardcoded
	result.Processes = make(map[string]TraceProcess)
	result.Processes["p1"] = TraceProcess{
		ServiceName: "telemetrygen",
		Tags:        []TraceKeyValuePair{},
	}

	spansRaw := spans.Get()
	for i := range len(spansRaw) {
		span := spansRaw[i]

		span.Logs = make([]TraceLog, 0)
		span.References = make([]TraceSpanReference, 0)
		span.Tags = make([]TraceKeyValuePair, 0)
		span.StackTraces = make([]string, 0)

		// TODO: hardcoded
		span.ProcessID = "p1"

		if span.ParentName != "" {
			reference := TraceSpanReference{
				RefType: "CHILD_OF",
				TraceID: result.TraceID,
				SpanID:  span.ParentName,
			}
			span.References = append(span.References, reference)
		}

		// tags
		span.Tags = make([]TraceKeyValuePair, 0)
		for key, value := range span.SpanAttributes {
			span.Tags = append(span.Tags, TraceKeyValuePair{
				Key:   key,
				Type:  "string",
				Value: value,
			})
		}
		span.Tags = append(span.Tags, TraceKeyValuePair{
			Key:   "otel.scope.name",
			Type:  "string",
			Value: span.ScopeName,
		})
		span.Tags = append(span.Tags, TraceKeyValuePair{
			Key:   "span.kind",
			Type:  "string",
			Value: span.SpanKind,
		})

		result.Spans = append(result.Spans, span)
	}

	return result, nil
}

type DependenciesParams struct {
	EndTime  *time.Time
	Lookback *time.Duration
}

func Dependencies(ctx context.Context, db *sql.DB, params DependenciesParams) ([]DependenciesData, error) {
	rows, err := db.QueryContext(ctx, dependenciesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]DependenciesData, 0)
	for rows.Next() {
		var result DependenciesData
		if err := rows.Scan(
			&result.ParentServiceName,
			&result.ChildServiceName,
			&result.Count,
		); err != nil {
			log.Fatal(err)
		}

		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return results, nil
}
