package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/rs/cors"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"golang.org/x/sync/errgroup"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/alkmst-xyz/sweetcorn/cmd/sweetcorn/internal/errors"
	"github.com/alkmst-xyz/sweetcorn/cmd/sweetcorn/internal/statusutil"
	"github.com/alkmst-xyz/sweetcorn/internal/sweetcorn"
)

// Pre-computed status with code=Internal to be used in case of a marshaling error.
var fallbackMsg = []byte(`{"code": 13, "message": "failed to marshal error message"}`)

const fallbackContentType = "application/json"

// yoinked from https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/otlpreceiver/encoder.go
const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"
)

var (
	pbEncoder       = &protoEncoder{}
	jsEncoder       = &jsonEncoder{}
	jsonPbMarshaler = &jsonpb.Marshaler{}
)

type encoder interface {
	unmarshalTracesRequest(buf []byte) (ptraceotlp.ExportRequest, error)
	unmarshalLogsRequest(buf []byte) (plogotlp.ExportRequest, error)

	marshalTracesResponse(ptraceotlp.ExportResponse) ([]byte, error)
	marshalLogsResponse(plogotlp.ExportResponse) ([]byte, error)

	marshalStatus(rsp *spb.Status) ([]byte, error)

	contentType() string
}

type protoEncoder struct{}

func (protoEncoder) unmarshalTracesRequest(buf []byte) (ptraceotlp.ExportRequest, error) {
	req := ptraceotlp.NewExportRequest()
	err := req.UnmarshalProto(buf)
	return req, err
}

func (protoEncoder) unmarshalLogsRequest(buf []byte) (plogotlp.ExportRequest, error) {
	req := plogotlp.NewExportRequest()
	err := req.UnmarshalProto(buf)
	return req, err
}

func (protoEncoder) marshalTracesResponse(resp ptraceotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalProto()
}

func (protoEncoder) marshalLogsResponse(resp plogotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalProto()
}

func (jsonEncoder) marshalTracesResponse(resp ptraceotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalJSON()
}

func (jsonEncoder) marshalLogsResponse(resp plogotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalJSON()
}

func (protoEncoder) marshalStatus(resp *spb.Status) ([]byte, error) {
	return proto.Marshal(resp)
}

func (protoEncoder) contentType() string {
	return pbContentType
}

type jsonEncoder struct{}

func (jsonEncoder) unmarshalTracesRequest(buf []byte) (ptraceotlp.ExportRequest, error) {
	req := ptraceotlp.NewExportRequest()
	err := req.UnmarshalJSON(buf)
	return req, err
}

func (jsonEncoder) unmarshalLogsRequest(buf []byte) (plogotlp.ExportRequest, error) {
	req := plogotlp.NewExportRequest()
	err := req.UnmarshalJSON(buf)
	return req, err
}

func (jsonEncoder) marshalStatus(resp *spb.Status) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := jsonPbMarshaler.Marshal(buf, resp)
	return buf.Bytes(), err
}

func (jsonEncoder) contentType() string {
	return jsonContentType
}

type HTTPService struct {
	ctx             context.Context
	db              *sql.DB
	insertLogsSQL   string
	insertTracesSQL string
}

// TODO: return appropriate status errors
// Ref: https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/otlpreceiver/internal/logs/otlp.go
func (s HTTPService) handleLogs(resp http.ResponseWriter, req *http.Request) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalLogsRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	err = sweetcorn.InsertLogsData(s.ctx, s.db, s.insertLogsSQL, otlpReq.Logs())
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalLogsResponse(plogotlp.NewExportResponse())
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func (s HTTPService) handleTraces(resp http.ResponseWriter, req *http.Request) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalTracesRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	err = sweetcorn.InsertTracesData(s.ctx, s.db, s.insertTracesSQL, otlpReq.Traces())
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalTracesResponse(ptraceotlp.NewExportResponse())
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
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

func startHTTPServer(ctx context.Context, db *sql.DB, insertLogsSQL string, insertTracesSQL string, addr string) error {
	svc := &HTTPService{
		ctx:             ctx,
		db:              db,
		insertLogsSQL:   insertLogsSQL,
		insertTracesSQL: insertTracesSQL,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", svc.handleLogs)
	mux.HandleFunc("POST /v1/traces", svc.handleTraces)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(mux),
	}

	log.Printf("HTTP server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}

func startGRPCServer(ctx context.Context, db *sql.DB, insertLogsSQL string, insertTracesSQL string, addr string) error {
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

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, logsService)
	ptraceotlp.RegisterGRPCServer(server, tracesService)
	reflection.Register(server)

	log.Printf("GRPC server listening on %s", lis.Addr())
	err = server.Serve(lis)

	return err
}

// Web
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
	res, err := sweetcorn.QueryLogs(s.ctx, s.db, s.queryLogsSQL)
	if err != nil {
		w.Header().Set("Content-Type", webDefaultContentType)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", webDefaultContentType)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func startApp(ctx context.Context, db *sql.DB, queryLogsSQL string, addr string) error {
	s := &WebService{
		ctx:          ctx,
		db:           db,
		queryLogsSQL: queryLogsSQL,
	}

	mux := http.NewServeMux()

	// Web UI
	// Note: The file server is explicitly configured to respond with the root index
	// when a 404 error is generated. This enables proper SPA functionality.
	// TODO: implement similar logic as `try_files` from nginx.
	webAssetsDirPath := "./web/build"
	webAssetsDir := http.Dir(webAssetsDirPath)
	webFileServer := http.FileServer(webAssetsDir)
	webServeIndex := serveFileContents("index.html", webAssetsDir)
	mux.Handle("/", intercept404(webFileServer, webServeIndex))

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

func main() {
	cfg := &sweetcorn.Config{
		DataSourceName:  ".sweetcorn_data/sweetcorn.db",
		LogsTableName:   "otel_logs",
		TracesTableName: "otel_traces",
	}

	// create data dir
	err := os.MkdirAll(".sweetcorn_data", 0755)
	if err != nil {
		log.Fatalf("Failed to create sweetcorn data dir: %s", err)
	}

	db, err := cfg.OpenDB()
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	if err := sweetcorn.CreateLogsTable(ctx, cfg, db); err != nil {
		log.Fatalf("Failed to create logs table: %v", err)
	}

	if err := sweetcorn.CreateTracesTable(ctx, cfg, db); err != nil {
		log.Fatalf("Failed to create traces table: %v", err)
	}

	insertLogsSQL := sweetcorn.RenderInsertLogsSQL(cfg)
	insertTracesSQL := sweetcorn.RenderInsertTracesSQL(cfg)
	queryLogsSQL := sweetcorn.RenderQueryLogsSQL(cfg)

	// start servers
	const httpAddr = ":4318"
	const grpcAddr = ":4317"
	const appAddr = ":3000"

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return startHTTPServer(ctx, db, insertLogsSQL, insertTracesSQL, httpAddr)
	})
	g.Go(func() error {
		return startGRPCServer(ctx, db, insertLogsSQL, insertTracesSQL, grpcAddr)
	})
	g.Go(func() error {
		return startApp(ctx, db, queryLogsSQL, appAddr)
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// Nothing we can do with the error if we cannot write to the response.
	_, _ = w.Write(msg)
}

func writeStatusResponse(w http.ResponseWriter, enc encoder, statusCode int, st *status.Status) {
	// https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#otlphttp-throttling
	if statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable {
		retryInfo := statusutil.GetRetryInfo(st)
		// Check if server returned throttling information.
		if retryInfo != nil {
			// We are throttled. Wait before retrying as requested by the server.
			// The value of Retry-After field can be either an HTTP-date or a number of
			// seconds to delay after the response is received. See https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.3
			//
			// Retry-After = HTTP-date / delay-seconds
			//
			// Use delay-seconds since is easier to format as well as does not require clock synchronization.
			w.Header().Set("Retry-After", strconv.FormatInt(int64(retryInfo.GetRetryDelay().AsDuration()/time.Second), 10))
		}
	}
	msg, err := enc.marshalStatus(st.Proto())
	if err != nil {
		writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
		return
	}

	writeResponse(w, enc.contentType(), statusCode, msg)
}

// writeError encodes the HTTP error inside a rpc.Status message as required by the OTLP protocol.
func writeError(w http.ResponseWriter, encoder encoder, err error, statusCode int) {
	s, ok := status.FromError(err)
	if ok {
		statusCode = errors.GetHTTPStatusCodeFromStatus(s)
	} else {
		s = statusutil.NewStatusFromMsgAndHTTPCode(err.Error(), statusCode)
	}
	writeStatusResponse(w, encoder, statusCode, s)
}

func handleUnmatchedMethod(resp http.ResponseWriter) {
	hst := http.StatusMethodNotAllowed
	writeResponse(resp, "text/plain", hst, []byte(fmt.Sprintf("%v method not allowed, supported: [POST]", hst)))
}

func getMimeTypeFromContentType(contentType string) string {
	mediatype, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return mediatype
}

func handleUnmatchedContentType(resp http.ResponseWriter) {
	hst := http.StatusUnsupportedMediaType
	writeResponse(resp, "text/plain", hst, []byte(fmt.Sprintf("%v unsupported media type, supported: [%s, %s]", hst, jsonContentType, pbContentType)))
}

func readContentType(w http.ResponseWriter, r *http.Request) (encoder, bool) {
	if r.Method != http.MethodPost {
		handleUnmatchedMethod(w)
		return nil, false
	}

	switch getMimeTypeFromContentType(r.Header.Get("Content-Type")) {
	case pbContentType:
		return pbEncoder, true
	case jsonContentType:
		return jsEncoder, true
	default:
		handleUnmatchedContentType(w)
		return nil, false
	}
}

func readAndCloseBody(resp http.ResponseWriter, req *http.Request, enc encoder) ([]byte, bool) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return nil, false
	}
	if err = req.Body.Close(); err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

//-----------------------------------------------------------------------------
// Magic used to enable SPA mode for sweetcorn web app.
// TODO: revise FS implementation used, inner working, etc.
// Ref: https://hackandsla.sh/posts/2021-11-06-serve-spa-from-go/
//-----------------------------------------------------------------------------

// See https://stackoverflow.com/questions/26141953/custom-404-with-gorilla-mux-and-std-http-fileserver
func intercept404(handler, on404 http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hookedWriter := &hookedResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(hookedWriter, r)

		if hookedWriter.got404 {
			on404.ServeHTTP(w, r)
		}
	})
}

type hookedResponseWriter struct {
	http.ResponseWriter
	got404 bool
}

func (hrw *hookedResponseWriter) WriteHeader(status int) {
	if status == http.StatusNotFound {
		// Don't actually write the 404 header, just set a flag.
		hrw.got404 = true
	} else {
		hrw.ResponseWriter.WriteHeader(status)
	}
}

func (hrw *hookedResponseWriter) Write(p []byte) (int, error) {
	if hrw.got404 {
		// No-op, but pretend that we wrote len(p) bytes to the writer.
		return len(p), nil
	}

	return hrw.ResponseWriter.Write(p)
}

func serveFileContents(file string, files http.FileSystem) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Restrict only to instances where the browser is looking for an HTML file
		if !strings.Contains(r.Header.Get("Accept"), "text/html") {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "404 not found")

			return
		}

		// Open the file and return its contents using http.ServeContent
		index, err := files.Open(file)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}

		fi, err := index.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, fi.Name(), fi.ModTime(), index)
	}
}
