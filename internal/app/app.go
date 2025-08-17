package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/cors"

	"github.com/alkmst-xyz/sweetcorn/internal/storage"
	"github.com/alkmst-xyz/sweetcorn/web"
)

const webDefaultContentType = "application/json"

const (
	jaegerTraceIDParam   = "traceID"
	jaegerStartTimeParam = "start"
	jaegerEndTimeParam   = "end"
)

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

func (s WebService) jaegerServices(w http.ResponseWriter, r *http.Request) {
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

func (s WebService) jaegerOperations(w http.ResponseWriter, r *http.Request) {
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

func (s WebService) jaegerOperationsLegacy(w http.ResponseWriter, r *http.Request) {
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

func (s WebService) jaegerSearchTraces(w http.ResponseWriter, r *http.Request) {
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

type JaegerGetTraceParams struct {
	TraceID   string
	StartTime time.Time // optional
	EndTime   time.Time // optional
}

const (
	defaultJaegerQueryLookbackDuration = 1 * time.Hour
)

func defaultStartTime() time.Time {
	return time.Now().Add(-1 * defaultJaegerQueryLookbackDuration)
}

func defaultEndTime() time.Time {
	return time.Now()
}

func parseTimeParam(r *http.Request, param string, defaultTimeFn func() time.Time) (time.Time, bool) {
	val := r.FormValue(param)

	if val == "" {
		return defaultTimeFn(), true
	}

	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, false
	}

	t := time.Unix(0, 0).Add(time.Duration(i) * time.Microsecond)

	return t, true
}

func parseGetTraceParams(r *http.Request) (JaegerGetTraceParams, bool) {
	params := JaegerGetTraceParams{}

	traceID := r.PathValue(jaegerTraceIDParam)
	if traceID == "" {
		return params, false
	}

	startTime, ok := parseTimeParam(r, jaegerStartTimeParam, defaultStartTime)
	if !ok {
		return params, false
	}

	endTime, ok := parseTimeParam(r, jaegerEndTimeParam, defaultEndTime)
	if !ok {
		return params, false
	}

	params.TraceID = traceID
	params.StartTime = startTime
	params.EndTime = endTime

	return params, true
}

func (s WebService) jaegerGetTrace(w http.ResponseWriter, r *http.Request) {
	params, ok := parseGetTraceParams(r)
	if !ok {
		// TODO: return error
		return
	}

	result, err := storage.GetTrace(s.ctx, s.db, params.TraceID)

	// TODO: use proper error responses
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
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
	mux.HandleFunc("GET /jaeger/api/services", s.jaegerServices)
	mux.HandleFunc("GET /jaeger/api/operations", s.jaegerOperations)
	mux.HandleFunc("GET /jaeger/api/services/{service_name}/operations", s.jaegerOperationsLegacy)
	mux.HandleFunc("GET /jaeger/api/traces", s.jaegerSearchTraces)
	mux.HandleFunc("GET /jaeger/api/traces/{traceID}", s.jaegerGetTrace)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(mux),
	}
	log.Printf("Sweetcorn server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}
