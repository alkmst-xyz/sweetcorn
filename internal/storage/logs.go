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
		time_unix_nano						TIMESTAMP_NS,
		observed_time_unix_nano				TIMESTAMP_NS,
		severity_number						INT32,
		severity_text						VARCHAR,
		body								VARCHAR,
		log_attributes						JSON,
		log_dropped_attributes_count		UINT32,
		flags								UINT32,
		trace_id							VARCHAR,
		span_id								VARCHAR,
		event_name							VARCHAR,
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

	insertLogsSQL = `
INSERT INTO
	%s (
		time_unix_nano,
		observed_time_unix_nano,
		severity_number,
		severity_text,
		body,
		log_attributes,
		log_dropped_attributes_count,
		flags,
		trace_id,
		span_id,
		event_name,
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
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryLogsSQL = `
SELECT
	time_unix_nano,
	observed_time_unix_nano,
	severity_number,
	severity_text,
	body,
	log_attributes,
	log_dropped_attributes_count,
	flags,
	trace_id,
	span_id,
	event_name,
	resource_attributes,
	resource_dropped_attributes_count,
	resource_service_name,
	resource_schema_url,
	scope_name,
	scope_version,
	scope_attributes,
	scope_dropped_attributes_count,
	scope_schema_url
FROM
	%s
ORDER BY
	time_unix_nano DESC
LIMIT
	100;`
)

type LogRecord struct {
	Timestamp                      int64          `json:"timestamp"`
	TimestampObserved              int64          `json:"timestampObserved"`
	SeverityNumber                 int32          `json:"severityNumber"`
	SeverityText                   string         `json:"severityText"`
	Body                           string         `json:"body"`
	LogAttributes                  map[string]any `json:"logAttributes"`
	LogDroppedAttributesCount      uint32         `json:"logDroppedAttributesCount"`
	Flags                          uint32         `json:"flags"`
	TraceId                        string         `json:"traceId"`
	SpanId                         string         `json:"spanId"`
	EventName                      string         `json:"eventName"`
	ResourceAttributes             map[string]any `json:"resourceAttributes"`
	ResourceDroppedAttributesCount uint32         `json:"resourceDroppedAttributesCount"`
	ResourceServiceName            string         `json:"resourceServiceName"`
	ResourceSchemaUrl              string         `json:"resourceSchemaUrl"`
	ScopeName                      string         `json:"scopeName"`
	ScopeVersion                   string         `json:"scopeVersion"`
	ScopeAttributes                map[string]any `json:"scopeAttributes"`
	ScopeDroppedAttributesCount    uint32         `json:"scopeDroppedAttributesCount"`
	ScopeSchemaUrl                 string         `json:"scopeSchemaUrl"`
}

// TODO: add storage as receiver
// TODO: move db, insertSQL to storage receiver
func InsertLogsData(ctx context.Context, db *sql.DB, insertLogsSQL string, logsData plog.Logs) error {
	for i := range logsData.ResourceLogs().Len() {
		resLogs := logsData.ResourceLogs().At(i)

		resSchemaUrl := resLogs.SchemaUrl()
		resAttrs := resLogs.Resource().Attributes()
		resDroppedAttributesCount := resLogs.Resource().DroppedAttributesCount()
		resServiceName := getServiceName(resAttrs)
		resAttrsBytes, resAttrsErr := json.Marshal(resAttrs.AsRaw())
		if resAttrsErr != nil {
			return fmt.Errorf("failed to marshal json log resource attributes: %w", resAttrsErr)
		}

		for j := range resLogs.ScopeLogs().Len() {
			scopeLogs := resLogs.ScopeLogs().At(j)

			scopeSchemaUrl := scopeLogs.SchemaUrl()
			scope := scopeLogs.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeAttrs := scope.Attributes()
			scopeDroppedAttributesCount := scope.DroppedAttributesCount()
			scopeAttrsBytes, scopeAttrsErr := json.Marshal(scopeAttrs.AsRaw())
			if scopeAttrsErr != nil {
				return fmt.Errorf("failed to marshal json log scope attributes: %w", scopeAttrsErr)
			}

			for k := range scopeLogs.LogRecords().Len() {
				logRecord := scopeLogs.LogRecords().At(k)

				logAttrs := logRecord.Attributes()
				logDroppedAttributesCount := logRecord.DroppedAttributesCount()
				logAttrsBytes, logAttrsErr := json.Marshal(logAttrs.AsRaw())
				if logAttrsErr != nil {
					return fmt.Errorf("failed to marshal json log attributes: %w", logAttrsErr)
				}

				_, err := db.ExecContext(ctx, insertLogsSQL,
					logRecord.Timestamp().AsTime(),
					logRecord.ObservedTimestamp().AsTime(),
					logRecord.SeverityNumber(),
					logRecord.SeverityText(),
					logRecord.Body().AsString(), // TODO: body type is lost here.
					logAttrsBytes,
					logDroppedAttributesCount,
					logRecord.Flags(),
					logRecord.TraceID().String(),
					logRecord.SpanID().String(),
					logRecord.EventName(),
					resAttrsBytes,
					resDroppedAttributesCount,
					resServiceName,
					resSchemaUrl,
					scopeName,
					scopeVersion,
					scopeAttrsBytes,
					scopeDroppedAttributesCount,
					scopeSchemaUrl,
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
		var timestampObserved time.Time

		err := rows.Scan(
			&timestamp,
			&timestampObserved,
			&result.SeverityNumber,
			&result.SeverityText,
			&result.Body,
			&result.LogAttributes,
			&result.LogDroppedAttributesCount,
			&result.Flags,
			&result.TraceId,
			&result.SpanId,
			&result.EventName,
			&result.ResourceAttributes,
			&result.ResourceDroppedAttributesCount,
			&result.ResourceServiceName,
			&result.ResourceSchemaUrl,
			&result.ScopeName,
			&result.ScopeVersion,
			&result.ScopeAttributes,
			&result.ScopeDroppedAttributesCount,
			&result.ScopeSchemaUrl,
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
