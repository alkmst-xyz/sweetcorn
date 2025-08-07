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
	ctx                             context.Context
	db                              *sql.DB
	queryLogsSQL                    string
	queryTracesSQL                  string
	queryDistinctTraceServicesSQL   string
	queryDistinctTraceOperationsSQL string
}

func (s WebService) getHealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("{\"status\": \"OK\"}"))
}

func (s WebService) getLogsHandler(w http.ResponseWriter, r *http.Request) {
	res, err := storage.QueryLogs(s.ctx, s.db, s.queryLogsSQL)
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
	res, err := storage.QueryTraces(s.ctx, s.db, s.queryTracesSQL)
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
	res, err := storage.GetDistinctServices(s.ctx, s.db, s.queryDistinctTraceServicesSQL)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func (s WebService) getDistinctTraceOperations(w http.ResponseWriter, r *http.Request) {
	res, err := storage.GetDistinctOperations(s.ctx, s.db, s.queryDistinctTraceOperationsSQL)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func StartWebApp(ctx context.Context, db *sql.DB, addr string, queryLogsSQL string, queryTracesSQL string, queryDistinctTraceServicesSQL string, queryDistinctTraceOperationsSQL string) error {
	s := &WebService{
		ctx:                             ctx,
		db:                              db,
		queryLogsSQL:                    queryLogsSQL,
		queryTracesSQL:                  queryTracesSQL,
		queryDistinctTraceServicesSQL:   queryDistinctTraceServicesSQL,
		queryDistinctTraceOperationsSQL: queryDistinctTraceOperationsSQL,
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
	mux.HandleFunc("GET /api/v1/traces/services", s.getDistinctTraceServices)
	mux.HandleFunc("GET /api/v1/traces/operations", s.getDistinctTraceOperations)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(mux),
	}
	log.Printf("Sweetcorn server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}
