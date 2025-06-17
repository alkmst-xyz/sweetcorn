package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/alkmst-xyz/sweetcorn/internal/sweetcorn"
	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "OK\n")
}

type Service struct {
	ctx             context.Context
	db              *sql.DB
	insertLogsSQL   string
	insertTracesSQL string
}

func unmarshalLogsRequest(buf []byte) (plogotlp.ExportRequest, error) {
	req := plogotlp.NewExportRequest()
	err := req.UnmarshalProto(buf)
	return req, err
}

func unmarshalTracesRequest(buf []byte) (ptraceotlp.ExportRequest, error) {
	req := ptraceotlp.NewExportRequest()
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

	payload := otlpReq.Logs()
	if err := sweetcorn.InsertLogsData(s.ctx, s.db, s.insertLogsSQL, payload); err != nil {
		log.Fatalf("Failed to write logs to db: %v", err)
		return
	}
}

func (s Service) handleTraces(w http.ResponseWriter, r *http.Request) {
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

	otlpReq, err := unmarshalTracesRequest(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Fatalf("Failed to unmarshal request: %v", err)
		return
	}

	payload := otlpReq.Traces()
	if err := sweetcorn.InsertTracesData(s.ctx, s.db, s.insertTracesSQL, payload); err != nil {
		log.Fatalf("Failed to write logs to db: %v", err)
		return
	}
}

func main() {
	cfg := &sweetcorn.Config{
		DataSourceName:  "sweetcorn.db",
		LogsTableName:   "otel_logs",
		TracesTableName: "otel_traces",
	}

	db, err := cfg.OpenDB()
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := sweetcorn.CreateLogsTable(ctx, cfg, db); err != nil {
		log.Fatalf("failed to create logs table: %v", err)
	}

	if err := sweetcorn.CreateTracesTable(ctx, cfg, db); err != nil {
		log.Fatalf("failed to create traces table: %v", err)
	}

	insertLogsSQL := sweetcorn.RenderInsertLogsSQL(cfg)
	insertTracesSQL := sweetcorn.RenderInsertTracesSQL(cfg)

	svc := &Service{
		ctx:             ctx,
		db:              db,
		insertLogsSQL:   insertLogsSQL,
		insertTracesSQL: insertTracesSQL,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", rootHandler)
	mux.HandleFunc("POST /v1/logs", svc.handleLogs)
	mux.HandleFunc("POST /v1/traces", svc.handleTraces)

	http.ListenAndServe("localhost:8090", mux)
}
