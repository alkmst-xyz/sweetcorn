package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"

	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/alkmst-xyz/sweetcorn/internal/logs"
)

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "OK\n")
}

type Service struct {
	ctx           context.Context
	db            *sql.DB
	insertLogsSQL string
}

func unmarshalLogsRequest(buf []byte) (plogotlp.ExportRequest, error) {
	req := plogotlp.NewExportRequest()
	err := req.UnmarshalProto(buf)
	return req, err
}

func (s Service) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Fatalf("Failed to read request body: %v", err)
		return
	}
	if err = r.Body.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Fatalf("Failed to close request body: %v", err)
		return
	}

	otlpReq, err := unmarshalLogsRequest(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Fatalf("Failed to unmarshal request: %v", err)
		return
	}

	newLogs := otlpReq.Logs()
	if err := logs.InsertLogsData(s.ctx, s.db, s.insertLogsSQL, newLogs); err != nil {
		log.Fatalf("Failed to write logs to db: %v", err)
		return
	}
}

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

	insertLogsSQL := logs.RenderInsertLogsSQL(cfg)

	svc := &Service{
		ctx:           ctx,
		db:            db,
		insertLogsSQL: insertLogsSQL,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", rootHandler)
	mux.HandleFunc("POST /v1/logs", svc.handleLogs)

	http.ListenAndServe("localhost:8090", mux)
}
