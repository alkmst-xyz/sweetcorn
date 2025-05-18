package logs

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestInsertAndQuery(t *testing.T) {
	cfg := &Config{
		DataSourceName: "",
		LogsTableName:  "otel_logs_test",
	}

	db, err := cfg.openDB()
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	createLogsTable(ctx, cfg, db)

	logRecord := LogRecord{
		Timestamp:         uint64(time.Now().UnixNano()),
		TraceId:           "abc123",
		SpanId:            "span456",
		TraceFlags:        1,
		SeverityText:      "INFO",
		SeverityNumber:    9,
		ServiceName:       "test-service",
		Body:              "This is a log message",
		ResourceSchemaUrl: "http://schema.resource",
		ResourceAttributes: map[string]any{
			"host": "localhost",
		},
		ScopeSchemaUrl: "http://schema.scope",
		ScopeName:      "logger",
		ScopeVersion:   "v1.0.0",
		ScopeAttributes: map[string]any{
			"lib": "loglib",
		},
		LogAttributes: map[string]any{
			"env": "test",
			"boi": 1,
		},
	}

	err = insertLog(ctx, cfg, db, logRecord)
	if err != nil {
		t.Fatalf("insertLog failed: %v", err)
	}

	results, err := queryLogs(ctx, cfg, db)
	if err != nil {
		t.Fatalf("queryLogs failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	got := results[0]
	if got.ServiceName != logRecord.ServiceName {
		t.Errorf("expected service name %s, got %s", logRecord.ServiceName, got.ServiceName)
	}

	if got.LogAttributes["env"] != "test" {
		t.Errorf("expected log attribute env=test, got %v", got.LogAttributes["env"])
	}
}
