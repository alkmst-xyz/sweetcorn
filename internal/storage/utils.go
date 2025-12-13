package storage

import (
	"database/sql"

	_ "github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

type Config struct {
	DataSourceName string
}

func (cfg *Config) OpenDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", cfg.DataSourceName)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func getServiceName(resAttr pcommon.Map) string {
	if v, ok := resAttr.Get(string(semconv.ServiceNameKey)); ok {
		return v.AsString()
	}

	return ""
}
