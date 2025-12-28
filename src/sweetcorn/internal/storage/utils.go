package storage

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

func getServiceName(resAttr pcommon.Map) string {
	if v, ok := resAttr.Get(string(semconv.ServiceNameKey)); ok {
		return v.AsString()
	}

	return ""
}
