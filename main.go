package main

import (
	"context"
	"log"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/alkmst-xyz/sweetcorn/internal/logs"
)

func main() {
	cfg := &logs.Config{
		DataSourceName: "sweetcorn.db",
		LogsTableName:  "otel_logs",
	}

	db, err := cfg.OpenDB()
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := logs.CreateLogsTable(ctx, cfg, db); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	new_logs := logs.SimpleLogs(1)
	insertLogsSQL := logs.RenderInsertLogsSQL(cfg)

	if err := logs.InsertLogsData(ctx, db, insertLogsSQL, new_logs); err != nil {
		log.Fatalf("insertLog failed: %v", err)
	}
}
