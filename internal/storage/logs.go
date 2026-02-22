package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	DefaultLogsTableName = "otel_logs"

	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS
	%s (
		ts						TIMESTAMP_NS,
		trace_id				VARCHAR,
		span_id					VARCHAR,
		trace_flags				UINTEGER,
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
		log_attributes			JSON,
		event_name				VARCHAR
	);`

	insertLogsSQL = `
INSERT INTO
	%s (
		ts,
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
		log_attributes,
		event_name
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryLogsSQL = `
SELECT
	ts,
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
	log_attributes,
	event_name
FROM
	%s
ORDER BY
	ts DESC
LIMIT
	100;`
)

type LogRecord struct {
	Timestamp          int64          `json:"timestamp"`
	TraceId            string         `json:"traceId"`
	SpanId             string         `json:"spanId"`
	TraceFlags         uint32         `json:"traceFlags"`
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
	EventName          string         `json:"eventName"`
}

// TODO: add storage as receiver
// TODO: move db, insertSQL to storage receiver
func InsertLogsData(ctx context.Context, db *sql.DB, insertLogsSQL string, logs plog.Logs) error {
	resLogsSlice := logs.ResourceLogs()
	for i := range resLogsSlice.Len() {
		resLogs := resLogsSlice.At(i)

		resSchemaUrl := resLogs.SchemaUrl()
		resAttrs := resLogs.Resource().Attributes()
		resServiceName := getServiceName(resAttrs)
		resAttrsBytes, resAttrsErr := json.Marshal(resAttrs.AsRaw())
		if resAttrsErr != nil {
			return fmt.Errorf("failed to marshal json log resource attributes: %w", resAttrsErr)
		}

		scopeLogsSlice := resLogs.ScopeLogs()
		for j := range scopeLogsSlice.Len() {
			scopeLogs := scopeLogsSlice.At(j)

			scope := scopeLogs.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeSchemaUrl := scopeLogs.SchemaUrl()

			scopeAttrBytes, scopeAttrErr := json.Marshal(scope.Attributes().AsRaw())
			if scopeAttrErr != nil {
				return fmt.Errorf("failed to marshal json log scope attributes: %w", scopeAttrErr)
			}

			logRecordsSlice := scopeLogs.LogRecords()
			for k := range logRecordsSlice.Len() {
				logRecord := logRecordsSlice.At(k)

				logAttrsBytes, logAttrsErr := json.Marshal(logRecord.Attributes().AsRaw())
				if logAttrsErr != nil {
					return fmt.Errorf("failed to marshal json log attributes: %w", logAttrsErr)
				}

				timestamp := logRecord.Timestamp()
				if timestamp == 0 {
					timestamp = logRecord.ObservedTimestamp()
				}

				_, err := db.ExecContext(ctx, insertLogsSQL,
					timestamp.AsTime(),
					logRecord.TraceID().String(),
					logRecord.SpanID().String(),
					logRecord.Flags(),
					logRecord.SeverityText(),
					uint8(logRecord.SeverityNumber()),
					resServiceName,
					logRecord.Body().AsString(),
					resSchemaUrl,
					resAttrsBytes,
					scopeSchemaUrl,
					scopeName,
					scopeVersion,
					scopeAttrBytes,
					logAttrsBytes,
					logRecord.EventName(),
				)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func QueryLogs(ctx context.Context, s *Storage) ([]LogRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryLogsSQL, s.Config.LogsTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]LogRecord, 0)

	for rows.Next() {
		var result LogRecord

		var timestamp time.Time

		err := rows.Scan(
			&timestamp,
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
			&result.EventName,
		)
		if err != nil {
			return nil, err
		}

		// convert timestamp to unix epoch in microseconds
		result.Timestamp = timestamp.UnixMicro()

		results = append(results, result)
	}

	return results, nil
}
