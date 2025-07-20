package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

const (
	createLogsTableSQL = `CREATE TABLE IF NOT EXISTS %s (
	Timestamp 			TIMESTAMP_NS,
	TimestampTime 		TIMESTAMP GENERATED ALWAYS AS (CAST(Timestamp AS TIMESTAMP)),
	TraceId				TEXT,
	SpanId 				TEXT,
	TraceFlags 			UTINYINT,
	SeverityText 		TEXT,
	SeverityNumber		UTINYINT,
	ServiceName 		TEXT,
	Body 				TEXT,
	ResourceSchemaUrl 	TEXT,
	ResourceAttributes	JSON,
	ScopeSchemaUrl 		TEXT,
	ScopeName 			TEXT,
	ScopeVersion 		TEXT,
	ScopeAttributes 	JSON,
	LogAttributes 		JSON,
	PRIMARY KEY (ServiceName, Timestamp)
);`

	insertLogsSQLTemplate = `INSERT INTO %s (
	Timestamp,
	TraceId,
	SpanId,
	TraceFlags,
	SeverityText,
	SeverityNumber,
	ServiceName,
	Body,
	ResourceSchemaUrl,
	ResourceAttributes,
	ScopeSchemaUrl,
	ScopeName,
	ScopeVersion,
	ScopeAttributes,
	LogAttributes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryLogsSQLTemplate = `SELECT
	TimestampTime,
	TraceId,
	SpanId,
	TraceFlags,
	SeverityText,
	SeverityNumber,
	ServiceName,
	Body,
	ResourceSchemaUrl,
	ResourceAttributes,
	ScopeSchemaUrl,
	ScopeName,
	ScopeVersion,
	ScopeAttributes,
	LogAttributes
FROM
	%s
ORDER BY
	Timestamp DESC
LIMIT
	100;
`
)

func renderCreateLogsTableSQL(cfg *Config) string {
	return fmt.Sprintf(createLogsTableSQL, cfg.LogsTableName)
}

func RenderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.LogsTableName)
}

func RenderQueryLogsSQL(cfg *Config) string {
	return fmt.Sprintf(queryLogsSQLTemplate, cfg.LogsTableName)
}

type LogRecord struct {
	TimestampTime      time.Time      `json:"timestamp"`
	TraceId            string         `json:"traceId"`
	SpanId             string         `json:"spanId"`
	TraceFlags         uint8          `json:"traceFlags"`
	SeverityText       string         `json:"severityText"`
	SeverityNumber     uint8          `json:"severityNumber"`
	ServiceName        string         `json:"serviceName"`
	Body               string         `json:"body"`
	ResourceSchemaUrl  string         `json:"resourceSchemaUrl"`
	ResourceAttributes map[string]any `json:"resourceAttributes"`
	ScopeSchemaUrl     string         `json:"scopeSchemaUrl"`
	ScopeName          string         `json:"scopeName"`
	ScopeVersion       string         `json:"scopeVersion"`
	ScopeAttributes    map[string]any `json:"scopeAttributes"`
	LogAttributes      map[string]any `json:"logAttributes"`
}

func CreateLogsTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateLogsTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create logs table sql: %w", err)
	}
	return nil
}

// Convert nanoseconds since epoch to time.Time
func toISO8601(ts pcommon.Timestamp) string {
	t := time.Unix(0, int64(ts)).UTC()
	return t.Format(time.RFC3339Nano)
}

func jsonBlob(m map[string]any) []byte {
	b, _ := json.Marshal(m)
	return b
}

// deprecated
func insertLog(ctx context.Context, cfg *Config, db *sql.DB, logRecord LogRecord) error {
	insertLogsSQL := RenderInsertLogsSQL(cfg)

	_, err := db.ExecContext(ctx, insertLogsSQL,
		logRecord.TimestampTime,
		logRecord.TraceId,
		logRecord.SpanId,
		logRecord.TraceFlags,
		logRecord.SeverityText,
		logRecord.SeverityNumber,
		logRecord.ServiceName,
		logRecord.Body,
		logRecord.ResourceSchemaUrl,
		jsonBlob(logRecord.ResourceAttributes),
		logRecord.ScopeSchemaUrl,
		logRecord.ScopeName,
		logRecord.ScopeVersion,
		jsonBlob(logRecord.ScopeAttributes),
		jsonBlob(logRecord.LogAttributes),
	)

	return err
}

func QueryLogs(ctx context.Context, db *sql.DB, queryLogsSQL string) ([]LogRecord, error) {
	rows, err := db.QueryContext(ctx, queryLogsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]LogRecord, 0)

	for rows.Next() {
		var result LogRecord

		err := rows.Scan(
			&result.TimestampTime,
			&result.TraceId,
			&result.SpanId,
			&result.TraceFlags,
			&result.SeverityText,
			&result.SeverityNumber,
			&result.ServiceName,
			&result.Body,
			&result.ResourceSchemaUrl,
			&result.ResourceAttributes,
			&result.ScopeSchemaUrl,
			&result.ScopeName,
			&result.ScopeVersion,
			&result.ScopeAttributes,
			&result.LogAttributes,
		)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, nil
}

func InsertLogsData(ctx context.Context, db *sql.DB, insertSQL string, ld plog.Logs) error {
	for i := range ld.ResourceLogs().Len() {
		logs := ld.ResourceLogs().At(i)
		res := logs.Resource()
		resURL := logs.SchemaUrl()
		resAttr := attributesToBytes(res.Attributes())
		serviceName := getServiceName(res.Attributes())

		for j := range logs.ScopeLogs().Len() {
			rs := logs.ScopeLogs().At(j).LogRecords()
			scopeURL := logs.ScopeLogs().At(j).SchemaUrl()
			scopeName := logs.ScopeLogs().At(j).Scope().Name()
			scopeVersion := logs.ScopeLogs().At(j).Scope().Version()
			scopeAttr := attributesToBytes(logs.ScopeLogs().At(j).Scope().Attributes())

			for k := range rs.Len() {
				r := rs.At(k)

				timestamp := r.Timestamp()
				if timestamp == 0 {
					timestamp = r.ObservedTimestamp()
				}

				logAttr := attributesToBytes(r.Attributes())

				_, err := db.ExecContext(ctx, insertSQL,
					toISO8601(timestamp),
					traceIDToHexOrEmptyString(r.TraceID()),
					spanIDToHexOrEmptyString(r.SpanID()),
					uint32(r.Flags()),
					r.SeverityText(),
					int32(r.SeverityNumber()),
					serviceName,
					r.Body().AsString(),
					resURL,
					resAttr,
					scopeURL,
					scopeName,
					scopeVersion,
					scopeAttr,
					logAttr,
				)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func SimpleLogs(count int) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.4.0")
	rl.Resource().Attributes().PutStr("service.name", "test-service2")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	sl.Scope().SetName("duckdb")
	sl.Scope().SetVersion("1.0.0")
	sl.Scope().Attributes().PutStr("lib", "duckdb")
	timestamp := time.Now()
	for i := range count {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
		r.SetObservedTimestamp(pcommon.NewTimestampFromTime(timestamp))
		r.SetSeverityNumber(plog.SeverityNumberError2)
		r.SetSeverityText("error")
		r.Body().SetStr("error message")
		r.Attributes().PutStr(string(semconv.ServiceNamespaceKey), "default")
		r.SetFlags(plog.DefaultLogRecordFlags)
		r.SetTraceID([16]byte{1, 2, 3, byte(i)})
		r.SetSpanID([8]byte{1, 2, 3, byte(i)})
	}
	return logs
}
