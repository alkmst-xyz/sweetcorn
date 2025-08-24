package storage

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

func withTestDB(t *testing.T, fn func(ctx context.Context, cfg *Config, db *sql.DB)) {
	cfg := &Config{
		DataSourceName: "",
	}

	db, err := cfg.OpenDB()
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := CreateLogsTable(ctx, cfg, db); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	fn(ctx, cfg, db)
}

func generateSampleLogs(count int) plog.Logs {
	logs := plog.NewLogs()

	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.4.0")
	rl.Resource().Attributes().PutStr("service.name", "test-service2")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.7.0")
	sl.Scope().SetName("duckdb")
	sl.Scope().SetVersion("1.0.0")
	sl.Scope().Attributes().PutStr("lib", "duckdb")
	timestamp := time.Now()

	for i := range count {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
		r.SetObservedTimestamp(pcommon.NewTimestampFromTime(timestamp))
		r.SetSeverityNumber(plog.SeverityNumberError2)
		r.SetSeverityText("error")
		r.Body().SetStr("error message")
		r.Attributes().PutStr(string(semconv.ServiceNamespaceKey), "default")
		r.SetFlags(plog.DefaultLogRecordFlags)
		r.SetTraceID([16]byte{1, 2, 3, byte(i)})
		r.SetSpanID([8]byte{1, 2, 3, byte(i)})
	}

	return logs
}

func TestOpenDB_InvalidDSN(t *testing.T) {
	cfg := &Config{
		DataSourceName: "invalid:://path",
	}

	_, err := cfg.OpenDB()
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestQueryLogs_WithoutTable(t *testing.T) {
	cfg := &Config{
		DataSourceName: "",
	}

	db, err := cfg.OpenDB()
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	_, err = QueryLogs(context.Background(), db)
	if err == nil {
		t.Fatal("expected error querying non-existent table")
	}
}

func TestInsertLogsData(t *testing.T) {
	withTestDB(t, func(ctx context.Context, cfg *Config, db *sql.DB) {
		logs := generateSampleLogs(1)

		if err := InsertLogsData(ctx, db, logs); err != nil {
			t.Fatalf("insertLog failed: %v", err)
		}
	})
}

func TestInsertLogsDataAndQuery(t *testing.T) {
	withTestDB(t, func(ctx context.Context, cfg *Config, db *sql.DB) {
		numLogs := 10
		logs := generateSampleLogs(numLogs)

		if err := InsertLogsData(ctx, db, logs); err != nil {
			t.Fatalf("InsertLogsData failed: %v", err)
		}

		results, err := QueryLogs(ctx, db)
		if err != nil {
			t.Fatalf("QueryLogs failed: %v", err)
		}

		if len(results) != numLogs {
			t.Fatalf("Expected %d result, got %d", numLogs, len(results))
		}

		gotServiceName := results[0].ServiceName
		expectedServiceName := getServiceName(logs.ResourceLogs().At(0).Resource().Attributes())
		if gotServiceName != expectedServiceName {
			t.Errorf("Expected service name %s, got %s.", expectedServiceName, gotServiceName)
		}
	})
}

func InsertLogsDataWithEmptyAttributes(t *testing.T) {
	withTestDB(t, func(ctx context.Context, cfg *Config, db *sql.DB) {
		numLogs := 1
		logs := generateSampleLogs(numLogs)

		resLog := logs.ResourceLogs().At(0)
		scopeLog := resLog.ScopeLogs().At(0)
		scopeLogRecords := scopeLog.LogRecords().At(0)

		resLog.Resource().Attributes().Clear()
		scopeLog.Scope().Attributes().Clear()
		scopeLogRecords.Attributes().Clear()

		if err := InsertLogsData(ctx, db, logs); err != nil {
			t.Fatalf("InsertLogsData failed: %v", err)
		}

		results, err := QueryLogs(ctx, db)
		if err != nil {
			t.Fatalf("QueryLogs failed: %v", err)
		}

		if len(results) != numLogs {
			t.Fatalf("Expected %d result, got %d", numLogs, len(results))
		}

	})
}
