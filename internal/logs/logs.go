package logs

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.27.0"
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
	ResourceAttributes	BLOB,
	ScopeSchemaUrl 		TEXT,
	ScopeName 			TEXT,
	ScopeVersion 		TEXT,
	ScopeAttributes 	BLOB,
	LogAttributes 		BLOB,
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
FROM %s`
)

type Config struct {
	DataSourceName string

	LogsTableName string
}

func (cfg *Config) openDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", cfg.DataSourceName)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func renderCreateLogsTableSQL(cfg *Config) string {
	return fmt.Sprintf(createLogsTableSQL, cfg.LogsTableName)
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.LogsTableName)
}
func renderQueryLogsSQL(cfg *Config) string {
	return fmt.Sprintf(queryLogsSQLTemplate, cfg.LogsTableName)
}

type LogRecord struct {
	Timestamp          pcommon.Timestamp
	TraceId            string
	SpanId             string
	TraceFlags         uint8
	SeverityText       string
	SeverityNumber     uint8
	ServiceName        string
	Body               string
	ResourceSchemaUrl  string
	ResourceAttributes map[string]any
	ScopeSchemaUrl     string
	ScopeName          string
	ScopeVersion       string
	ScopeAttributes    map[string]any
	LogAttributes      map[string]any
}

func createLogsTable(ctx context.Context, cfg *Config, db *sql.DB) error {
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

func AttributesToBytes(attributes pcommon.Map) []byte {
	result := make(map[string]any)

	for k, v := range attributes.All() {
		result[k] = v.AsString()
	}

	b, _ := json.Marshal(result)
	return b
}

func insertLog(ctx context.Context, cfg *Config, db *sql.DB, logRecord LogRecord) error {

	insertLogsSQL := renderInsertLogsSQL(cfg)

	_, err := db.ExecContext(ctx, insertLogsSQL,
		toISO8601(logRecord.Timestamp),
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

func queryLogs(ctx context.Context, cfg *Config, db *sql.DB) ([]LogRecord, error) {
	queryLogsSQL := renderQueryLogsSQL(cfg)
	rows, err := db.QueryContext(ctx, queryLogsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []LogRecord

	for rows.Next() {
		var result LogRecord
		var timestamp time.Time
		var resourceAttrs, scopeAttrs, logAttrs []byte

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
			&resourceAttrs,
			&result.ScopeSchemaUrl,
			&result.ScopeName,
			&result.ScopeVersion,
			&scopeAttrs,
			&logAttrs,
		)
		if err != nil {
			return nil, err
		}

		result.Timestamp = pcommon.Timestamp(timestamp.UnixNano())

		_ = json.NewDecoder(bytes.NewReader(resourceAttrs)).Decode(&result.ResourceAttributes)
		_ = json.NewDecoder(bytes.NewReader(scopeAttrs)).Decode(&result.ScopeAttributes)
		_ = json.NewDecoder(bytes.NewReader(logAttrs)).Decode(&result.LogAttributes)

		results = append(results, result)
	}

	return results, nil
}

func InsertLogsData(ctx context.Context, db *sql.DB, insertSQL string, ld plog.Logs) error {
	for i := range ld.ResourceLogs().Len() {
		logs := ld.ResourceLogs().At(i)
		res := logs.Resource()
		resURL := logs.SchemaUrl()
		resAttr := AttributesToBytes(res.Attributes())
		serviceName := GetServiceName(res.Attributes())

		for j := range logs.ScopeLogs().Len() {
			rs := logs.ScopeLogs().At(j).LogRecords()
			scopeURL := logs.ScopeLogs().At(j).SchemaUrl()
			scopeName := logs.ScopeLogs().At(j).Scope().Name()
			scopeVersion := logs.ScopeLogs().At(j).Scope().Version()
			scopeAttr := AttributesToBytes(logs.ScopeLogs().At(j).Scope().Attributes())

			for k := range rs.Len() {
				r := rs.At(k)

				timestamp := r.Timestamp()
				if timestamp == 0 {
					timestamp = r.ObservedTimestamp()
				}

				logAttr := AttributesToBytes(r.Attributes())

				_, err := db.ExecContext(ctx, insertSQL,
					toISO8601(timestamp),
					TraceIDToHexOrEmptyString(r.TraceID()),
					SpanIDToHexOrEmptyString(r.SpanID()),
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
					return fmt.Errorf("ExecContext:%w", err)
				}
			}
		}
	}
	return nil

}

// escape single quotes for DuckDB literal syntax
func escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func AttributesToMap(attributes pcommon.Map) string {
	pairs := make([]string, 0, attributes.Len())
	for k, v := range attributes.All() {
		pairs = append(pairs, fmt.Sprintf("'%s': '%s'", escape(k), escape(v.AsString())))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ", "))
}

func GetServiceName(resAttr pcommon.Map) string {
	var serviceName string
	if v, ok := resAttr.Get(conventions.AttributeServiceName); ok {
		serviceName = v.AsString()
	}

	return serviceName
}

// yoinked from
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/coreinternal/traceutil/traceutil.go
//
// SpanIDToHexOrEmptyString returns a hex string from SpanID.
// An empty string is returned, if SpanID is empty.
func SpanIDToHexOrEmptyString(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

// yoinked from
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/coreinternal/traceutil/traceutil.go
//
// TraceIDToHexOrEmptyString returns a hex string from TraceID.
// An empty string is returned, if TraceID is empty.
func TraceIDToHexOrEmptyString(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}
