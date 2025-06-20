package sweetcorn

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.27.0"
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

func AttributesToBytes(attributes pcommon.Map) []byte {
	result := make(map[string]any)

	for k, v := range attributes.All() {
		result[k] = v.AsString()
	}

	b, _ := json.Marshal(result)
	return b
}

func AttributesArrayToBytes(attributesArray []pcommon.Map) []byte {
	result := make(map[string]any)

	for _, item := range attributesArray {
		for k, v := range item.All() {
			result[k] = v.AsString()
		}

	}

	b, _ := json.Marshal(result)
	return b
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
