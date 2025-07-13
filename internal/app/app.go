package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/rs/cors"

	"github.com/alkmst-xyz/sweetcorn/internal/storage"
)

const webDefaultContentType = "application/json"

type WebService struct {
	ctx          context.Context
	db           *sql.DB
	queryLogsSQL string
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

func StartWebApp(ctx context.Context, db *sql.DB, queryLogsSQL string, addr string) error {
	s := &WebService{
		ctx:          ctx,
		db:           db,
		queryLogsSQL: queryLogsSQL,
	}

	mux := http.NewServeMux()

	// Web UI
	webAssetsDirPath := "./web/build"
	mux.Handle("/", newSPAFileServer(webAssetsDirPath))

	// API routes
	mux.HandleFunc("GET /api/v1/healthz", s.getHealthzHandler)
	mux.HandleFunc("GET /api/v1/logs", s.getLogsHandler)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(mux),
	}
	log.Printf("Sweetcorn server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}
