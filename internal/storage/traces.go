package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	DefaultTracesTableName = "otel_traces"

	createTracesTableSQL = `
CREATE TABLE IF NOT EXISTS
	%s (
		trace_id							VARCHAR,
		span_id								VARCHAR,
		trace_state							VARCHAR,
		parent_span_id						VARCHAR,
		flags								UINT32,
		name								VARCHAR,
		kind								INT32,
		start_time_unix_nano				TIMESTAMP_NS,
		duration_nano						UINT64,
		span_attributes						JSON,
		span_dropped_attributes_count		UINT32,
    	events_time_unix_nano				TIMESTAMP_NS[],
    	events_name							VARCHAR[],
    	events_attributes					JSON[],
    	events_dropped_attributes_count		UINT32[],
		dropped_events_count				UINT32,
		links_trace_id 						VARCHAR[],
		links_span_id 						VARCHAR[],
		links_trace_state 					VARCHAR[],
		links_attributes 					JSON[],
		links_dropped_attributes_count 		UINT32[],
		links_flags							UINT32[],
		dropped_links_count 				UINT32,
		status_code							INT32,
		status_message						VARCHAR,
		resource_attributes					JSON,
		resource_dropped_attributes_count	UINT32,
		resource_service_name				VARCHAR,
		resource_schema_url					VARCHAR,
		scope_name							VARCHAR,
		scope_version						VARCHAR,
		scope_attributes					JSON,
		scope_dropped_attributes_count		UINT32,
		scope_schema_url					VARCHAR
	);`

	insertTracesSQL = `
INSERT INTO
	%s (
		trace_id,
		span_id,
		trace_state,
		parent_span_id,
		flags,
		name,
		kind,
		start_time_unix_nano,
		duration_nano,
		span_attributes,
		span_dropped_attributes_count,
    	events_time_unix_nano,
    	events_name,
    	events_attributes,
    	events_dropped_attributes_count,
		dropped_events_count,
		links_trace_id,
		links_span_id,
		links_trace_state,
		links_attributes,
		links_dropped_attributes_count,
		links_flags,
		dropped_links_count,
		status_code,
		status_message,
		resource_attributes,
		resource_dropped_attributes_count,
		resource_service_name,
		resource_schema_url,
		scope_name,
		scope_version,
		scope_attributes,
		scope_dropped_attributes_count,
		scope_schema_url
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryTracesSQL = `
SELECT
	start_time_unix_nano,
	trace_id,
	span_id,
	parent_span_id,
	trace_state,
	name,
	kind,
	resource_service_name,
	resource_attributes,
	scope_name,
	scope_version,
	span_attributes,
	duration_nano,
	status_code,
	status_message,
	events_time_unix_nano,
	events_name,
	events_attributes,
	links_trace_id,
	links_span_id,
	links_trace_state,
	links_attributes
FROM
	%s
ORDER BY
	start_time_unix_nano DESC
LIMIT
	100;`

	traceServicesSQL = `
SELECT DISTINCT
    resource_service_name
FROM
	%s
LIMIT
	100;`

	traceOperationsSQL = `
SELECT DISTINCT
    name
FROM
	%s
WHERE
	resource_service_name = ?
	AND (? = '' OR kind = ?)
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
			"StartTime" := epoch_us(start_time_unix_nano),
			"Duration" := duration_nano // 1000,
			"ParentName" := parent_span_id,
			"SpanAttributes" := span_attributes,
			"ScopeName" := name,
			"SpanKind" := kind
		)
	) as spans
FROM
	%s
WHERE
	(? IS NULL OR resource_service_name = ?)
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
			"OperationName" := name,
			"StartTime" := epoch_us(start_time_unix_nano),
			"Duration" := duration_nano // 1000,
			"ParentName" := parent_span_id,
			"SpanAttributes" := span_attributes,
			"ScopeName" := name,
			"SpanKind" := kind
		)
	) as spans
FROM
	%s
WHERE
	trace_id = ?
	AND (? = 0 OR epoch_us(start_time_unix_nano) >= ?)
	AND (? = 0 OR epoch_us(start_time_unix_nano) <  ?)
GROUP BY
	trace_id;`

	dependenciesSQL = `
SELECT DISTINCT
    (
        SELECT
            p.resource_service_name
        FROM
			%s AS p
        WHERE
            p.span_id = c.parent_span_id
    ) AS parent_service_name,
    c.resource_service_name AS child_service_name,
    COUNT(*) AS count
FROM
    %s as c
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

func convertEvents(events ptrace.SpanEventSlice) (
	times []time.Time,
	names []string,
	attrs []string,
	droppedAttributesCounts []uint32,
	err error,
) {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())

		eventAttrBytes, eventAttrErr := json.Marshal(event.Attributes().AsRaw())
		if eventAttrErr != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to marshal json trace event attributes: %w", eventAttrErr)
		}
		attrs = append(attrs, string(eventAttrBytes))

		droppedAttributesCounts = append(droppedAttributesCounts, event.DroppedAttributesCount())
	}

	return
}

func convertLinks(links ptrace.SpanLinkSlice) (
	traceIDs []string,
	spanIDs []string,
	states []string,
	attrs []string,
	droppedAttributesCounts []uint32,
	flags []uint32,
	err error,
) {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, link.TraceID().String())
		spanIDs = append(spanIDs, link.SpanID().String())
		states = append(states, link.TraceState().AsRaw())

		linkAttrBytes, linkAttrErr := json.Marshal(link.Attributes().AsRaw())
		if linkAttrErr != nil {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("failed to marshal json trace link attributes: %w", linkAttrErr)
		}
		attrs = append(attrs, string(linkAttrBytes))

		droppedAttributesCounts = append(droppedAttributesCounts, link.DroppedAttributesCount())
		flags = append(flags, link.Flags())
	}

	return
}

func InsertTracesData(ctx context.Context, db *sql.DB, insertTracesSQL string, tracesData ptrace.Traces) error {
	for i := range tracesData.ResourceSpans().Len() {
		resSpans := tracesData.ResourceSpans().At(i)

		resAttrs := resSpans.Resource().Attributes()
		resServiceName := getServiceName(resAttrs)
		resAttrsBytes, resAttrErr := json.Marshal(resAttrs.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json trace resource attributes: %w", resAttrErr)
		}

		for j := range resSpans.ScopeSpans().Len() {
			scopeSpans := resSpans.ScopeSpans().At(j)

			scope := scopeSpans.Scope()
			scopeAttrs := scope.Attributes()
			scopeAttrsBytes, scopeAttrsErr := json.Marshal(scopeAttrs.AsRaw())
			if scopeAttrsErr != nil {
				return fmt.Errorf("failed to marshal json trace scope attributes: %w", scopeAttrsErr)
			}

			for k := range scopeSpans.Spans().Len() {
				span := scopeSpans.Spans().At(k)

				durationNanoseconds := span.EndTimestamp() - span.StartTimestamp()
				status := span.Status()

				spanAttrsBytes, spanAttrsErr := json.Marshal(span.Attributes().AsRaw())
				if spanAttrsErr != nil {
					return fmt.Errorf("failed to marshal json trace span attributes: %w", spanAttrsErr)
				}

				eventsTimeUnixNano, eventsName, eventsAttrs,
					eventsDroppedAttributesCount, eventsErr := convertEvents(span.Events())
				if eventsErr != nil {
					return fmt.Errorf("failed to convert json trace events: %w", eventsErr)
				}

				linksTraceID, linksSpanID, linksTraceState, linksAttrs,
					linksDroppedAttributesCount, linksFlags, linksErr := convertLinks(span.Links())
				if linksErr != nil {
					return fmt.Errorf("failed to convert json trace links: %w", linksErr)
				}

				_, err := db.ExecContext(ctx, insertTracesSQL,
					span.TraceID().String(),
					span.SpanID().String(),
					span.TraceState().AsRaw(),
					span.ParentSpanID().String(),
					span.Flags(),
					span.Name(),
					span.Kind(),
					span.StartTimestamp().AsTime(),
					durationNanoseconds,
					spanAttrsBytes,
					span.DroppedAttributesCount(),
					eventsTimeUnixNano,
					eventsName,
					eventsAttrs,
					eventsDroppedAttributesCount,
					span.DroppedEventsCount(),
					linksTraceID,
					linksSpanID,
					linksTraceState,
					linksAttrs,
					linksDroppedAttributesCount,
					linksFlags,
					span.DroppedLinksCount(),
					status.Code(),
					status.Message(),
					resAttrsBytes,
					resSpans.Resource().DroppedAttributesCount(),
					resServiceName,
					resSpans.SchemaUrl(),
					scope.Name(),
					scope.Version(),
					scopeAttrsBytes,
					scope.DroppedAttributesCount(),
					scopeSpans.SchemaUrl(),
				)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func QueryTraces(ctx context.Context, s *Storage) ([]TraceRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryTracesSQL, s.Config.TracesTable))
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

func TraceServices(ctx context.Context, s *Storage) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(traceServicesSQL, s.Config.TracesTable))
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

func TraceOperations(ctx context.Context, s *Storage, params TraceOperationsParams) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(traceOperationsSQL, s.Config.TracesTable),
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

func SearchTraces(ctx context.Context, s *Storage, params SearchTracesParams) ([]TraceResponse, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(tracesSQL, s.Config.TracesTable),
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
	StartTime time.Time
	EndTime   time.Time
}

func Trace(ctx context.Context, s *Storage, params TraceParams) (TraceResponse, error) {
	var startTime int64
	if !params.StartTime.IsZero() {
		startTime = params.StartTime.UnixMicro()
	}

	var endTime int64
	if !params.EndTime.IsZero() {
		endTime = params.EndTime.UnixMicro()
	}

	row := s.DB.QueryRowContext(ctx, renderQuery(traceSQL, s.Config.TracesTable),
		params.TraceID,
		startTime,
		startTime,
		endTime,
		endTime,
	)

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

func Dependencies(ctx context.Context, s *Storage, params DependenciesParams) ([]DependenciesData, error) {
	rows, err := s.DB.QueryContext(
		ctx,
		renderQuery(dependenciesSQL, s.Config.TracesTable, s.Config.TracesTable),
	)
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
