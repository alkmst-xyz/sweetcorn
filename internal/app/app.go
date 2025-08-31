package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

var errServiceParameterRequired = fmt.Errorf("parameter '%s' is required", jaegerServiceParam)

type WebService struct {
	ctx context.Context
	db  *sql.DB
}

type jaegerResponse struct {
	Data   any           `json:"data"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
	Errors []jaegerError `json:"errors"`
}

type jaegerError struct {
	Code    int    `json:"code,omitempty"`
	Msg     string `json:"msg"`
	TraceID string `json:"traceID,omitempty"`
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
	data, err := storage.GetServices(s.ctx, s.db)
	if jaegerHandleError(w, err, http.StatusInternalServerError) {
		return
	}

	resp := jaegerResponse{
		Data:  data,
		Total: len(data),
	}

	jaegerWriteResponse(w, &resp)
}

func (s WebService) jaegerOperations(w http.ResponseWriter, r *http.Request) {
	service := r.FormValue(jaegerServiceParam)
	if service == "" {
		if jaegerHandleError(w, errServiceParameterRequired, http.StatusBadRequest) {
			return
		}
	}
	spanKind := r.FormValue(jaegerSpanKindParam)

	data, err := storage.TraceOperations(s.ctx, s.db, storage.TraceOperationsParams{
		ServiceName: service,
		SpanKind:    spanKind,
	})

	if jaegerHandleError(w, err, http.StatusInternalServerError) {
		return
	}

	resp := jaegerResponse{
		Data:  data,
		Total: len(data),
	}

	jaegerWriteResponse(w, &resp)
}

func (s WebService) jaegerOperationsLegacy(w http.ResponseWriter, r *http.Request) {
	// Here we expect service name to not be empty because it the result of a path match.
	service := r.PathValue(jaegerServiceParam)

	data, err := storage.TraceOperations(s.ctx, s.db, storage.TraceOperationsParams{
		ServiceName: service,
		SpanKind:    "",
	})
	if jaegerHandleError(w, err, http.StatusInternalServerError) {
		return
	}

	resp := jaegerResponse{
		Data:  data,
		Total: len(data),
	}

	jaegerWriteResponse(w, &resp)
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
	if jaegerHandleError(w, err, http.StatusInternalServerError) {
		return
	}

	resp := jaegerResponse{
		Data:  data,
		Total: len(data),
	}

	jaegerWriteResponse(w, &resp)
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

	if errors.Is(err, storage.ErrTraceNotFound) {
		jaegerHandleError(w, err, http.StatusNotFound)
		return
	}

	if jaegerHandleError(w, err, http.StatusInternalServerError) {
		return
	}

	resp := jaegerResponse{
		Data:  []storage.TraceResponse{data},
		Total: 1,
	}

	jaegerWriteResponse(w, &resp)
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
	if jaegerHandleError(w, err, http.StatusInternalServerError) {
		return
	}

	resp := jaegerResponse{
		Data: data,
	}

	jaegerWriteResponse(w, &resp)
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

func jaegerHandleError(w http.ResponseWriter, err error, code int) bool {
	if err == nil {
		return false
	}

	if code == http.StatusInternalServerError {
		log.Panicf("Error: HTTP handler, Internal Server Error: %v", err)
	}

	h := w.Header()
	h.Set("Content-Type", webDefaultContentType)
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)

	response := &jaegerResponse{
		Errors: []jaegerError{
			{
				Code: code,
				Msg:  err.Error(),
			},
		},
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Panicf("Error: failed writing HTTP error response: %v", err)
	}

	return true
}

func jaegerWriteResponse(w http.ResponseWriter, response any) {
	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		jaegerHandleError(w, fmt.Errorf("failed writing HTTP response: %w", err), http.StatusInternalServerError)
	}
}
