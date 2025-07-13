package storage

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

type Config struct {
	DataSourceName  string
	LogsTableName   string
	TracesTableName string
}

func (cfg *Config) OpenDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", cfg.DataSourceName)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func attributesToBytes(attributes pcommon.Map) []byte {
	result := make(map[string]any)

	for k, v := range attributes.All() {
		result[k] = v.AsString()
	}

	b, _ := json.Marshal(result)
	return b
}

func attributesArrayToBytes(attributesArray []pcommon.Map) []byte {
	result := make(map[string]any)

	for _, item := range attributesArray {
		for k, v := range item.All() {
			result[k] = v.AsString()
		}

	}

	b, _ := json.Marshal(result)
	return b
}

func getServiceName(resAttr pcommon.Map) string {
	var serviceName string
	if v, ok := resAttr.Get(string(semconv.ServiceNameKey)); ok {
		serviceName = v.AsString()
	}

	return serviceName
}

// yoinked from
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/coreinternal/traceutil/traceutil.go
//
// spanIDToHexOrEmptyString returns a hex string from SpanID.
// An empty string is returned, if SpanID is empty.
func spanIDToHexOrEmptyString(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

// yoinked from
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/internal/coreinternal/traceutil/traceutil.go
//
// traceIDToHexOrEmptyString returns a hex string from TraceID.
// An empty string is returned, if TraceID is empty.
func traceIDToHexOrEmptyString(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}
