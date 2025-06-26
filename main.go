package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/alkmst-xyz/sweetcorn/internal/sweetcorn"
	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// yoinked from "go.opentelemetry.io/collector/receiver/otlpreceiver/internal/errors"
func GetStatusFromError(err error) error {
	s, ok := status.FromError(err)
	if !ok {
		// Default to a retryable error
		// https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures
		code := codes.Unavailable
		if consumererror.IsPermanent(err) {
			// If an error is permanent but doesn't have an attached gRPC status, assume it is server-side.
			code = codes.Internal
		}
		s = status.New(code, err.Error())
	}
	return s.Err()
}

type LogsGRPCService struct {
	plogotlp.UnimplementedGRPCServer
	ctx           context.Context
	db            *sql.DB
	insertLogsSQL string
}

func (r *LogsGRPCService) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ld := req.Logs()
	numSpans := ld.LogRecordCount()
	if numSpans == 0 {
		return plogotlp.NewExportResponse(), nil
	}

	err := sweetcorn.InsertLogsData(r.ctx, r.db, r.insertLogsSQL, ld)

	if err != nil {
		log.Fatalf("Failed to write logs to db: %v", err)
		return plogotlp.NewExportResponse(), GetStatusFromError(err)
	}

	return plogotlp.NewExportResponse(), nil
}

type TracesGRPCService struct {
	ptraceotlp.UnimplementedGRPCServer
	ctx             context.Context
	db              *sql.DB
	insertTracesSQL string
}

func (r *TracesGRPCService) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	td := req.Traces()
	numSpans := td.SpanCount()
	if numSpans == 0 {
		return ptraceotlp.NewExportResponse(), nil
	}

	err := sweetcorn.InsertTracesData(r.ctx, r.db, r.insertTracesSQL, td)

	if err != nil {
		log.Fatalf("Failed to write traces to db: %v", err)
		return ptraceotlp.NewExportResponse(), GetStatusFromError(err)
	}

	return ptraceotlp.NewExportResponse(), nil
}

func main() {
	cfg := &sweetcorn.Config{
		DataSourceName:  ".sweetcorn/data/sweetcorn.db",
		LogsTableName:   "otel_logs",
		TracesTableName: "otel_traces",
	}

	// create data dir
	err := os.MkdirAll(".sweetcorn/data", 0755)
	if err != nil {
		log.Fatalf("Failed to create sweetcorn data dir: %s", err)
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

	// grpc
	const addr = "0.0.0.0:4317"
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()

	logsService := &LogsGRPCService{
		ctx:           ctx,
		db:            db,
		insertLogsSQL: insertLogsSQL,
	}
	tracesService := &TracesGRPCService{
		ctx:             ctx,
		db:              db,
		insertTracesSQL: insertTracesSQL,
	}

	plogotlp.RegisterGRPCServer(server, logsService)
	ptraceotlp.RegisterGRPCServer(server, tracesService)

	log.Printf("Starting GRPC server %v", listener.Addr())
	go func() {
		if errGrpc := server.Serve(listener); errGrpc != nil {
			log.Fatalf("failed to serve: %v", errGrpc)
		}
	}()

	// http
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

	log.Printf("Starting HTTP server")
	http.ListenAndServe("localhost:4318", mux)
}
