package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	createTracesTableSQL = `CREATE TABLE IF NOT EXISTS %s (
	Timestamp 			TIMESTAMP_NS,
	TraceId				TEXT,
	SpanId 				TEXT,
	ParentSpanId 		TEXT,
	TraceState 			TEXT,
	SpanName 			TEXT,
	SpanKind 			TEXT,
	ServiceName 		TEXT,
	ResourceAttributes 	BLOB,
	ScopeName 			TEXT,
	ScopeVersion 		TEXT,
	SpanAttributes 		BLOB,
	Duration 			UBIGINT,
	StatusCode 			TEXT,
	StatusMessage 		TEXT,
	EventsTimestamp 	TIMESTAMP_NS[],
	EventsName 			TEXT[],
	EventsAttributes 	BLOB,
	LinksTraceId 		TEXT[],
	LinksSpanId 		TEXT[],
	LinksTraceState 	TEXT[],
	LinksAttributes 	BLOB
);`

	insertTracesSQLTemplate = `INSERT INTO %s (
    Timestamp,
    TraceId,
    SpanId,
    ParentSpanId,
    TraceState,
    SpanName,
    SpanKind,
    ServiceName,
    ResourceAttributes,
    ScopeName,
    ScopeVersion,
    SpanAttributes,
    Duration,
    StatusCode,
    StatusMessage,
    EventsName,
    EventsAttributes,
    LinksTraceId,
    LinksSpanId,
    LinksTraceState,
    LinksAttributes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryTracesSQLTemplate = `SELECT
	TraceId,
	SpanId,
	ParentSpanId,
	TraceState,
	SpanName,
	SpanKind,
	ServiceName,
	ResourceAttributes,
	ScopeName,
	ScopeVersion,
	SpanAttributes,
	Duration,
	StatusCode,
	StatusMessage,
	EventsAttributes,
	LinksAttributes
FROM
	%s
ORDER BY
	Timestamp DESC
LIMIT
	100;
`
)

type TraceRecord struct {
	// Timestamp          uint64      `json:"timestamp"`
	TraceId            string         `json:"traceId"`
	SpanId             string         `json:"spanId"`
	ParentSpanId       string         `json:"parentSpanId"`
	TraceState         string         `json:"traceState"`
	SpanName           string         `json:"spanName"`
	SpanKind           string         `json:"spanKind"`
	ServiceName        string         `json:"serviceName"`
	ResourceAttributes map[string]any `json:"resourceAttributes"`
	ScopeName          string         `json:"scopeName"`
	ScopeVersion       string         `json:"scopeVersion"`
	SpanAttributes     map[string]any `json:"spanAttributes"`
	Duration           uint64         `json:"duration"`
	StatusCode         string         `json:"statusCode"`
	StatusMessage      string         `json:"statusMessage"`
	// EventsTimestamp    []uint64       `json:"eventsTimestamp"`
	// EventsName         []string         `json:"eventsName"`
	EventsAttributes map[string]any `json:"eventsAttributes"`
	// LinksTraceId     []string       `json:"linksTraceId"`
	// LinksSpanId      []string       `json:"linksSpanId"`
	// LinksTraceState  []string       `json:"linksTraceState"`
	LinksAttributes map[string]any `json:"linksAttributes"`
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
				_, eventNames, eventAttrs := convertEvents(r.Events())
				linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs := convertLinks(r.Links())

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
					// eventTimes,
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
		var resourceAttrs, spanAttrs, eventAttrs, linkAttrs []byte

		err := rows.Scan(
			&result.TraceId,
			&result.SpanId,
			&result.ParentSpanId,
			&result.TraceState,
			&result.SpanName,
			&result.SpanKind,
			&result.ServiceName,
			&resourceAttrs,
			&result.ScopeName,
			&result.ScopeVersion,
			&spanAttrs,
			&result.Duration,
			&result.StatusCode,
			&result.StatusMessage,
			&eventAttrs,
			&linkAttrs,
		)
		if err != nil {
			return nil, err
		}

		_ = json.NewDecoder(bytes.NewReader(resourceAttrs)).Decode(&result.ResourceAttributes)
		_ = json.NewDecoder(bytes.NewReader(spanAttrs)).Decode(&result.SpanAttributes)
		_ = json.NewDecoder(bytes.NewReader(eventAttrs)).Decode(&result.EventsAttributes)
		_ = json.NewDecoder(bytes.NewReader(linkAttrs)).Decode(&result.LinksAttributes)

		results = append(results, result)
	}

	return results, nil
}
