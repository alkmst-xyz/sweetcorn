package storage

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

// Render SQL query template with placeholders. These are usually
// SQL identifiers such as table, columns, etc.
func renderQuery(queryTemplate string, args ...any) string {
	return fmt.Sprintf(queryTemplate, args...)
}

// Get the service name of a resource according to OTEL semantic conventions.
func getServiceName(resAttrs pcommon.Map) string {
	if v, ok := resAttrs.Get(string(semconv.ServiceNameKey)); ok {
		return v.AsString()
	}

	return ""
}
