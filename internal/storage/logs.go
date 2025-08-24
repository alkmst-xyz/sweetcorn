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
	createLogsTableSQL = `
CREATE SEQUENCE IF NOT EXISTS otel_logs_id_seq;

CREATE TABLE IF NOT EXISTS otel_logs (
	id						BIGINT PRIMARY KEY DEFAULT nextval ('otel_logs_id_seq'),
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

	insertLogsSQL = `INSERT INTO otel_logs (
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

	queryLogsSQL = `SELECT
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
FROM
	otel_logs
ORDER BY
	timestamp DESC
LIMIT
	100;
`
)

type LogRecord struct {
	Timestamp          time.Time      `json:"timestamp"`
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
	if _, err := db.ExecContext(ctx, createLogsTableSQL); err != nil {
		return fmt.Errorf("exec create logs table sql: %w", err)
	}
	return nil
}

func QueryLogs(ctx context.Context, db *sql.DB) ([]LogRecord, error) {
	rows, err := db.QueryContext(ctx, queryLogsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]LogRecord, 0)

	for rows.Next() {
		var result LogRecord

		err := rows.Scan(
			&result.Timestamp,
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

func InsertLogsData(ctx context.Context, db *sql.DB, ld plog.Logs) error {
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

				_, err := db.ExecContext(ctx, insertLogsSQL,
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
