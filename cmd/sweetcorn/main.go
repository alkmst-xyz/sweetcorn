package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	_ "github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

func rootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "OK\n")
}

type Service struct {
	ctx             context.Context
	db              *sql.DB
	insertLogsSQL   string
	insertTracesSQL string
}

// TODO: return appropriate status errors
// Ref: https://github.com/open-telemetry/opentelemetry-collector/blob/main/receiver/otlpreceiver/internal/logs/otlp.go
func (s Service) handleLogs(resp http.ResponseWriter, req *http.Request) {
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

func (s Service) handleTraces(resp http.ResponseWriter, req *http.Request) {
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

	// grpc
	const grpcAddr = ":4317"
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("Failed to create listener: %v", err)
	}

	grpcServer := grpc.NewServer()

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

	plogotlp.RegisterGRPCServer(grpcServer, logsService)
	ptraceotlp.RegisterGRPCServer(grpcServer, tracesService)

	log.Printf("Starting GRPC server %v", listener.Addr())
	go func() {
		if grpcErr := grpcServer.Serve(listener); grpcErr != nil {
			log.Fatalf("Failed to start gRPC server: %v", grpcErr)
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

	const httpAddr = ":4318"
	httpServer := &http.Server{Addr: httpAddr, Handler: mux}

	log.Printf("Starting HTTP server %v", httpServer.Addr)
	if httpErr := httpServer.ListenAndServe(); httpErr != nil {
		log.Fatalf("Failed to start HTTP server: %v", httpErr)
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
