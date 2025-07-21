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
	createLogsTableSQL = `
CREATE SEQUENCE IF NOT EXISTS otel_logs_id_seq;

CREATE TABLE IF NOT EXISTS %s (
	_id						BIGINT PRIMARY KEY DEFAULT nextval ('otel_logs_id_seq'),
	timestamp				TIMESTAMP_NS,
	timestamp_time			TIMESTAMP_S GENERATED ALWAYS AS (CAST(Timestamp AS TIMESTAMP)),
	trace_id				VARCHAR,
	span_id					VARCHAR,
	trace_flags				UTINYINT,
	severity_text			VARCHAR,
	severity_number			UTINYINT,
	service_name			VARCHAR,
	body					VARCHAR,
	resource_schema_url		VARCHAR,
	resource_attributes		JSON,
	scope_schema_url 		VARCHAR,
	scope_name				VARCHAR,
	scope_version			VARCHAR,
	scope_attributes		JSON,
	log_attributes			JSON
);`

	insertLogsSQLTemplate = `INSERT INTO %s (
	timestamp,
	trace_id,
	span_id,
	trace_flags,
	severity_text,
	severity_number,
	service_name,
	body,
	resource_schema_url,
	resource_attributes,
	scope_schema_url,
	scope_name,
	scope_version,
	scope_attributes,
	log_attributes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryLogsSQLTemplate = `SELECT
	timestamp_time,
	trace_id,
	span_id,
	trace_flags,
	severity_text,
	severity_number,
	service_name,
	body,
	resource_schema_url,
	resource_attributes,
	scope_schema_url,
	scope_name,
	scope_version,
	scope_attributes,
	log_attributes
FROM
	%s
ORDER BY
	timestamp_time DESC
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
	if m == nil {
		return []byte("{}")
	}

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
	rsLogs := ld.ResourceLogs()

	for i := range rsLogs.Len() {
		logs := rsLogs.At(i)
		res := logs.Resource()
		resURL := logs.SchemaUrl()
		resAttr := res.Attributes()
		serviceName := getServiceName(resAttr)

		resAttrBytes, resAttrErr := json.Marshal(resAttr.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json log resource attributes: %w", resAttrErr)
		}

		for j := range logs.ScopeLogs().Len() {
			scopeLog := logs.ScopeLogs().At(j)
			scopeURL := scopeLog.SchemaUrl()
			scopeLogScope := scopeLog.Scope()
			scopeName := scopeLog.Scope().Name()
			scopeVersion := scopeLog.Scope().Version()
			scopeLogRecords := scopeLog.LogRecords()

			scopeAttrBytes, scopeAttrErr := json.Marshal(scopeLogScope.Attributes().AsRaw())
			if scopeAttrErr != nil {
				return fmt.Errorf("failed to marshal json log scope attributes: %w", scopeAttrErr)
			}

			for k := range scopeLogRecords.Len() {
				r := scopeLogRecords.At(k)

				logAttrBytes, logAttrErr := json.Marshal(r.Attributes().AsRaw())
				if logAttrErr != nil {
					return fmt.Errorf("failed to marshal json log attributes: %w", logAttrErr)
				}

				timestamp := r.Timestamp()
				if timestamp == 0 {
					timestamp = r.ObservedTimestamp()
				}

				_, err := db.ExecContext(ctx, insertSQL,
					timestamp.AsTime(),
					r.TraceID().String(),
					r.SpanID().String(),
					uint8(r.Flags()),
					r.SeverityText(),
					uint8(r.SeverityNumber()),
					serviceName,
					r.Body().AsString(),
					resURL,
					resAttrBytes,
					scopeURL,
					scopeName,
					scopeVersion,
					scopeAttrBytes,
					logAttrBytes,
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
