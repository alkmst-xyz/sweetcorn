package otlphttp

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/rs/cors"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/alkmst-xyz/sweetcorn/sweetcorn/internal/storage"
)

type HTTPService struct {
	ctx context.Context
	db  *sql.DB
}

//
// Encoder
//

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
	unmarshalMetricsRequest(buf []byte) (pmetricotlp.ExportRequest, error)

	marshalTracesResponse(ptraceotlp.ExportResponse) ([]byte, error)
	marshalLogsResponse(plogotlp.ExportResponse) ([]byte, error)
	marshalMetricsResponse(pmetricotlp.ExportResponse) ([]byte, error)

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

func (protoEncoder) unmarshalMetricsRequest(buf []byte) (pmetricotlp.ExportRequest, error) {
	req := pmetricotlp.NewExportRequest()
	err := req.UnmarshalProto(buf)
	return req, err
}

func (protoEncoder) marshalTracesResponse(resp ptraceotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalProto()
}

func (protoEncoder) marshalLogsResponse(resp plogotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalProto()
}

func (protoEncoder) marshalMetricsResponse(resp pmetricotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalProto()
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

func (jsonEncoder) unmarshalMetricsRequest(buf []byte) (pmetricotlp.ExportRequest, error) {
	req := pmetricotlp.NewExportRequest()
	err := req.UnmarshalJSON(buf)
	return req, err
}

func (jsonEncoder) marshalTracesResponse(resp ptraceotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalJSON()
}

func (jsonEncoder) marshalLogsResponse(resp plogotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalJSON()
}

func (jsonEncoder) marshalMetricsResponse(resp pmetricotlp.ExportResponse) ([]byte, error) {
	return resp.MarshalJSON()
}

func (jsonEncoder) marshalStatus(resp *spb.Status) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := jsonPbMarshaler.Marshal(buf, resp)
	return buf.Bytes(), err
}

func (jsonEncoder) contentType() string {
	return jsonContentType
}

//
// Utils
//

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// Nothing we can do with the error if we cannot write to the response.
	_, _ = w.Write(msg)
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

func writeStatusResponse(w http.ResponseWriter, enc encoder, statusCode int, st *status.Status) {
	// https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#otlphttp-throttling
	if statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable {
		retryInfo := GetRetryInfo(st)
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
		statusCode = GetHTTPStatusCodeFromStatus(s)
	} else {
		s = NewStatusFromMsgAndHTTPCode(err.Error(), statusCode)
	}
	writeStatusResponse(w, encoder, statusCode, s)
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

//
// Logs
//

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

	err = storage.InsertLogsData(s.ctx, s.db, otlpReq.Logs())
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

//
// Traces
//

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

	err = storage.InsertTracesData(s.ctx, s.db, otlpReq.Traces())
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

//
// Metrics
//

func (s HTTPService) handleMetrics(resp http.ResponseWriter, req *http.Request) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalMetricsRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	err = storage.IngestMetricsData(s.ctx, s.db, otlpReq.Metrics())
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalMetricsResponse(pmetricotlp.NewExportResponse())
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

//
// Main
//

func StartHTTPServer(ctx context.Context, db *sql.DB, addr string) error {
	svc := &HTTPService{
		ctx: ctx,
		db:  db,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", svc.handleLogs)
	mux.HandleFunc("POST /v1/traces", svc.handleTraces)
	mux.HandleFunc("POST /v1/metrics", svc.handleMetrics)

	server := &http.Server{
		Addr:    addr,
		Handler: cors.Default().Handler(mux),
	}

	log.Printf("HTTP server listening on %s", addr)
	err := server.ListenAndServe()

	return err
}
