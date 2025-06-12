package logs

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func withTestDB(t *testing.T, tableName string, fn func(ctx context.Context, cfg *Config, db *sql.DB)) {
	cfg := &Config{
		DataSourceName: "",
		LogsTableName:  tableName,
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

func sampleLog(ts pcommon.Timestamp) LogRecord {
	return LogRecord{
		Timestamp:         ts,
		TraceId:           "trace",
		SpanId:            "span",
		TraceFlags:        1,
		SeverityText:      "INFO",
		SeverityNumber:    9,
		ServiceName:       "test-service",
		Body:              "body",
		ResourceSchemaUrl: "http://resource",
		ResourceAttributes: map[string]any{
			"k": "v",
		},
		ScopeSchemaUrl: "http://scope",
		ScopeName:      "scope",
		ScopeVersion:   "v1",
		ScopeAttributes: map[string]any{
			"scope": "attr",
		},
		LogAttributes: map[string]any{
			"env": "test",
		},
	}
}

func TestInsertAndQuery_ValidLog(t *testing.T) {
	withTestDB(t, "logs_valid_test", func(ctx context.Context, cfg *Config, db *sql.DB) {
		log := sampleLog(pcommon.Timestamp(time.Now().UnixNano()))

		if err := insertLog(ctx, cfg, db, log); err != nil {
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
		if got.ServiceName != log.ServiceName {
			t.Errorf("expected service name %s, got %s", log.ServiceName, got.ServiceName)
		}
	})
}

func TestInsertLog_EmptyAttributes(t *testing.T) {
	withTestDB(t, "logs_empty_attr_test", func(ctx context.Context, cfg *Config, db *sql.DB) {
		log := sampleLog(pcommon.Timestamp(time.Now().UnixNano()))
		log.ResourceAttributes = nil
		log.ScopeAttributes = nil
		log.LogAttributes = nil

		if err := insertLog(ctx, cfg, db, log); err != nil {
			t.Fatalf("insertLog failed: %v", err)
		}

		results, _ := queryLogs(ctx, cfg, db)
		if len(results) != 1 {
			t.Fatal("expected 1 result for empty attributes")
		}
	})
}

func TestOpenDB_InvalidDSN(t *testing.T) {
	cfg := &Config{
		DataSourceName: "invalid:://path",
		LogsTableName:  "bad_dsn_test",
	}

	_, err := cfg.OpenDB()
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestQueryLogs_WithoutTable(t *testing.T) {
	cfg := &Config{
		DataSourceName: "",
		LogsTableName:  "non_existent_table",
	}

	db, err := cfg.OpenDB()
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	_, err = queryLogs(context.Background(), cfg, db)
	if err == nil {
		t.Fatal("expected error querying non-existent table")
	}
}

func TestInsertOtelLogs(t *testing.T) {
	withTestDB(t, "insert_otel_logs", func(ctx context.Context, cfg *Config, db *sql.DB) {
		logs := SimpleLogs(1)
		insertLogsSQL := RenderInsertLogsSQL(cfg)

		if err := InsertLogsData(ctx, db, insertLogsSQL, logs); err != nil {
			t.Fatalf("insertLog failed: %v", err)
		}
	})
}
