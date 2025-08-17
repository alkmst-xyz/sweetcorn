package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/rs/cors"

	"github.com/alkmst-xyz/sweetcorn/internal/storage"
	"github.com/alkmst-xyz/sweetcorn/web"
)

const webDefaultContentType = "application/json"

type WebService struct {
	ctx            context.Context
	db             *sql.DB
	queryLogsSQL   string
	queryTracesSQL string
}

func (s WebService) getHealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"status\": \"OK\"}"))
}

func (s WebService) getLogsHandler(w http.ResponseWriter, r *http.Request) {
	res, err := storage.QueryLogs(s.ctx, s.db)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func (s WebService) getTracesHandler(w http.ResponseWriter, r *http.Request) {
	res, err := storage.QueryTraces(s.ctx, s.db)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func (s WebService) getDistinctTraceServices(w http.ResponseWriter, r *http.Request) {
	data, err := storage.GetDistinctServices(s.ctx, s.db)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := &storage.ServicesResponse{
		Data:   data,
		Errors: nil,
		Limit:  0,
		Offset: 0,
		Total:  len(data),
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s WebService) getDistinctTraceOperations(w http.ResponseWriter, r *http.Request) {
	data, err := storage.GetDistinctOperations(s.ctx, s.db)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := &storage.ServicesResponse{
		Data:   data,
		Errors: nil,
		Limit:  0,
		Offset: 0,
		Total:  len(data),
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s WebService) getServiceTraceOperations(w http.ResponseWriter, r *http.Request) {
	serviceName := r.PathValue("service_name")

	data, err := storage.GetServiceOperations(s.ctx, s.db, serviceName)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := &storage.ServicesResponse{
		Data:   data,
		Errors: nil,
		Limit:  0,
		Offset: 0,
		Total:  len(data),
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s WebService) getTraces(w http.ResponseWriter, r *http.Request) {
	data, err := storage.GetTraces(s.ctx, s.db)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := &storage.TracesResponse{
		Data:   data,
		Errors: nil,
		Limit:  0,
		Offset: 0,
		Total:  len(data),
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s WebService) getTrace(w http.ResponseWriter, r *http.Request) {
	traceID := r.PathValue("trace_id")
	response, err := storage.GetTrace(s.ctx, s.db, traceID)

	// TODO: use proper error responses
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func StartWebApp(ctx context.Context, db *sql.DB, addr string) error {
	s := &WebService{
		ctx: ctx,
		db:  db,
	}

	mux := http.NewServeMux()

	// Web UI
	webAssets, webAssetsErr := web.AssetsFS()
	if webAssetsErr != nil {
		return webAssetsErr
	}

	mux.Handle("/", http.FileServer(http.FS(webAssets)))

	// API routes
	mux.HandleFunc("GET /api/v1/healthz", s.getHealthzHandler)
	mux.HandleFunc("GET /api/v1/logs", s.getLogsHandler)
	mux.HandleFunc("GET /api/v1/traces", s.getTracesHandler)

	// Jaeger Query Internal HTTP API
	// Ref: https://www.jaegertracing.io/docs/2.9/architecture/apis/#internal-http-json
	mux.HandleFunc("GET /jaeger/api/services", s.getDistinctTraceServices)
	mux.HandleFunc("GET /jaeger/api/services/{service_name}/operations", s.getServiceTraceOperations)
	mux.HandleFunc("GET /jaeger/api/operations", s.getDistinctTraceOperations)
	mux.HandleFunc("GET /jaeger/api/traces", s.getTraces)
	mux.HandleFunc("GET /jaeger/api/traces/{trace_id}", s.getTrace)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(mux),
	}
	log.Printf("Sweetcorn server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}
