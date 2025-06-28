package sweetcorn

import (
	"context"
	"database/sql"
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
)

func convertEvents(events ptrace.SpanEventSlice) (times []time.Time, names []string, attrs []byte) {
	var attrsRaw []pcommon.Map
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		times = append(times, event.Timestamp().AsTime())
		names = append(names, event.Name())
		attrsRaw = append(attrsRaw, event.Attributes())
	}
	attrs = AttributesArrayToBytes(attrsRaw)
	return
}

func convertLinks(links ptrace.SpanLinkSlice) (traceIDs []string, spanIDs []string, states []string, attrs []byte) {
	var attrsRaw []pcommon.Map
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		traceIDs = append(traceIDs, TraceIDToHexOrEmptyString(link.TraceID()))
		spanIDs = append(spanIDs, SpanIDToHexOrEmptyString(link.SpanID()))
		states = append(states, link.TraceState().AsRaw())
		attrsRaw = append(attrsRaw, link.Attributes())
	}
	attrs = AttributesArrayToBytes(attrsRaw)
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

func InsertTracesData(ctx context.Context, db *sql.DB, insertSQL string, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		spans := td.ResourceSpans().At(i)
		res := spans.Resource()
		resAttr := AttributesToBytes(res.Attributes())
		serviceName := GetServiceName(res.Attributes())

		for j := 0; j < spans.ScopeSpans().Len(); j++ {
			rs := spans.ScopeSpans().At(j).Spans()
			scopeName := spans.ScopeSpans().At(j).Scope().Name()
			scopeVersion := spans.ScopeSpans().At(j).Scope().Version()
			for k := 0; k < rs.Len(); k++ {
				r := rs.At(k)
				spanAttr := AttributesToBytes(r.Attributes())
				status := r.Status()
				_, eventNames, eventAttrs := convertEvents(r.Events())
				linksTraceIDs, linksSpanIDs, linksTraceStates, linksAttrs := convertLinks(r.Links())

				_, err := db.ExecContext(ctx, insertSQL,
					r.StartTimestamp().AsTime(),
					TraceIDToHexOrEmptyString(r.TraceID()),
					SpanIDToHexOrEmptyString(r.SpanID()),
					SpanIDToHexOrEmptyString(r.ParentSpanID()),
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
