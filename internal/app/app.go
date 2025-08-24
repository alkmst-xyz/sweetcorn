package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
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
	jaegerServiceParam   = "service"
	jaegerSpanKindParam  = "spanKind"
	jaegerOperationParam = "operation"
)

type WebService struct {
	ctx context.Context
	db  *sql.DB
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
	services, err := storage.GetDistinctServices(s.ctx, s.db)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := &storage.ServicesResponse{
		Data:   services,
		Errors: nil,
		Limit:  0,
		Offset: 0,
		Total:  len(services),
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func parseTraceOperationsParams(r *http.Request) storage.TraceOperationsParams {
	q := r.URL.Query()

	var p storage.TraceOperationsParams

	// ?service
	if vals, ok := q[jaegerServiceParam]; ok {
		p.ServiceName = &vals[0]
	}

	// ?spanKind
	if vals, ok := q[jaegerSpanKindParam]; ok {
		p.SpanKind = &vals[0]
	}

	return p
}

func (s WebService) jaegerOperations(w http.ResponseWriter, r *http.Request) {
	params := parseTraceOperationsParams(r)

	data, err := storage.TraceOperations(s.ctx, s.db, params)
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
	q := r.URL.Query()

	var params storage.TraceOperationsParams

	// ?service
	if vals, ok := q[jaegerServiceParam]; ok {
		params.ServiceName = &vals[0]
	}

	data, err := storage.TraceOperations(s.ctx, s.db, params)
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

func parseSearchTracesParams(r *http.Request) (storage.SearchTracesParams, bool) {
	q := r.URL.Query()

	var p storage.SearchTracesParams

	// ?service
	if vals, ok := q[jaegerServiceParam]; ok {
		p.ServiceName = &vals[0]
	}

	// ?operation
	if vals, ok := q[jaegerOperationParam]; ok {
		p.OperationName = &vals[0]
	}

	// ?start
	if vals, ok := q[jaegerStartTimeParam]; ok {
		t, err := parseTimeParam(vals[0], defaultStartTime)

		if err != nil {
			return p, false
		}

		p.StartTimeMin = t
	}

	// ?end
	if vals, ok := q[jaegerEndTimeParam]; ok {
		t, err := parseTimeParam(vals[0], defaultEndTime)

		if err != nil {
			return p, false
		}

		p.StartTimeMax = t
	}

	return p, true
}

func (s WebService) jaegerSearchTraces(w http.ResponseWriter, r *http.Request) {
	params, ok := parseSearchTracesParams(r)
	if !ok {
		// TODO: return error
		return
	}

	data, err := storage.SearchTraces(s.ctx, s.db, params)
	if err != nil {
		io.WriteString(w, err.Error())
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

const (
	defaultJaegerQueryLookbackDuration = 1 * time.Hour
)

func defaultStartTime() time.Time {
	return time.Now().Add(-1 * defaultJaegerQueryLookbackDuration)
}

func defaultEndTime() time.Time {
	return time.Now()
}

// parseUnixMicros parses a string containing a Unix timestamp in microseconds.
func parseUnixMicros(val string) (time.Time, error) {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, 0).Add(time.Duration(i) * time.Microsecond), nil
}

// parseTimeParam parses a sting into a Unix timestamp. If the provided
// string is empty, it uses the provided defaultTimeFn.
func parseTimeParam(raw string, defaultTimeFn func() time.Time) (*time.Time, error) {
	if raw == "" {
		t := defaultTimeFn()
		return &t, nil
	}

	t, err := parseUnixMicros(raw)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func parseTraceParams(r *http.Request) (storage.TraceParams, bool) {
	q := r.URL.Query()

	var p storage.TraceParams

	// {traceID}
	traceID := r.PathValue(jaegerTraceIDParam)
	if traceID == "" {
		return p, false
	}
	p.TraceID = traceID

	// ?start
	if vals, ok := q[jaegerStartTimeParam]; ok {
		t, err := parseTimeParam(vals[0], defaultStartTime)

		if err != nil {
			return p, false
		}

		p.StartTime = t
	}

	// ?end
	if vals, ok := q[jaegerEndTimeParam]; ok {
		t, err := parseTimeParam(vals[0], defaultEndTime)

		if err != nil {
			return p, false
		}

		p.EndTime = t
	}

	return p, true
}

func (s WebService) jaegerTrace(w http.ResponseWriter, r *http.Request) {
	params, ok := parseTraceParams(r)
	if !ok {
		// TODO: return error
		return
	}

	data, err := storage.Trace(s.ctx, s.db, params)

	// TODO: use proper error responses
	if err != nil {
		io.WriteString(w, err.Error())
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := storage.TracesResponse{
		Data:   []storage.TraceResponse{data},
		Errors: nil,
		Limit:  0,
		Offset: 0,
		Total:  1,
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&response)
}

func parseDependenciesParams(r *http.Request) (storage.DependenciesParams, bool) {
	q := r.URL.Query()

	var p storage.DependenciesParams

	// ?end
	if vals, ok := q[jaegerEndTimeParam]; ok {
		t, err := parseTimeParam(vals[0], defaultEndTime)

		if err != nil {
			return p, false
		}

		p.EndTime = t
	}

	// TODO: ?lookback

	return p, true
}

func (s WebService) jaegerDependencies(w http.ResponseWriter, r *http.Request) {
	params, ok := parseDependenciesParams(r)
	if !ok {
		// TODO: return error
		return
	}

	data, err := storage.Dependencies(s.ctx, s.db, params)

	// TODO: use proper error responses
	if err != nil {
		io.WriteString(w, err.Error())
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	response := storage.DependenciesResponse{
		Data:   data,
		Errors: nil,
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&response)
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
	// TODO: remove hard coded path match parameters
	mux.HandleFunc("GET /jaeger/api/services", s.jaegerServices)
	mux.HandleFunc("GET /jaeger/api/operations", s.jaegerOperations)
	mux.HandleFunc("GET /jaeger/api/services/{service}/operations", s.jaegerOperationsLegacy)
	mux.HandleFunc("GET /jaeger/api/traces", s.jaegerSearchTraces)
	mux.HandleFunc("GET /jaeger/api/traces/{traceID}", s.jaegerTrace)
	mux.HandleFunc("GET /jaeger/api/dependencies", s.jaegerDependencies)

	// grafana is hitting this endpoint somehow!!
	mux.HandleFunc("GET /api/traces", s.jaegerSearchTraces)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(loggingMiddleware(mux)),
	}
	log.Printf("Sweetcorn server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}

// TODO: remove later, use proper logging lol
// loggingMiddleware wraps an http.Handler and logs request info.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		query := r.URL.Query()

		log.Printf(
			"Request: %s %s | Query: %v",
			r.Method, path, query,
		)

		next.ServeHTTP(w, r)
	})
}
