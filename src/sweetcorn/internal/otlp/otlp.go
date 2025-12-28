package otlp

import (
	"context"
	"log"
	"net"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/alkmst-xyz/sweetcorn/sweetcorn/internal/storage"
)

//
// Utils
//

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

//
// Logs
//

type LogsGRPCService struct {
	plogotlp.UnimplementedGRPCServer
	ctx     context.Context
	storage *storage.Storage
}

func (r *LogsGRPCService) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	ld := req.Logs()
	numSpans := ld.LogRecordCount()
	if numSpans == 0 {
		return plogotlp.NewExportResponse(), nil
	}

	err := storage.InsertLogsData(r.ctx, r.storage.DB, ld)
	if err != nil {
		log.Fatalf("Failed to write logs to db: %v", err)
		return plogotlp.NewExportResponse(), GetStatusFromError(err)
	}

	return plogotlp.NewExportResponse(), nil
}

//
// Traces
//

type TracesGRPCService struct {
	ptraceotlp.UnimplementedGRPCServer
	ctx     context.Context
	storage *storage.Storage
}

func (r *TracesGRPCService) Export(ctx context.Context, req ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	td := req.Traces()
	numSpans := td.SpanCount()
	if numSpans == 0 {
		return ptraceotlp.NewExportResponse(), nil
	}

	err := storage.InsertTracesData(r.ctx, r.storage.DB, td)
	if err != nil {
		log.Fatalf("Failed to write traces to db: %v", err)
		return ptraceotlp.NewExportResponse(), GetStatusFromError(err)
	}

	return ptraceotlp.NewExportResponse(), nil
}

//
// Metrics
//

type MetricsGRPCService struct {
	pmetricotlp.UnimplementedGRPCServer
	ctx     context.Context
	storage *storage.Storage
}

func (r *MetricsGRPCService) Export(ctx context.Context, req pmetricotlp.ExportRequest) (pmetricotlp.ExportResponse, error) {
	md := req.Metrics()
	dataPointCount := md.DataPointCount()
	if dataPointCount == 0 {
		return pmetricotlp.NewExportResponse(), nil
	}

	err := storage.IngestMetricsData(r.ctx, r.storage.DB, md)
	if err != nil {
		log.Fatalf("Failed to write metrics to db: %v", err)
		return pmetricotlp.NewExportResponse(), GetStatusFromError(err)
	}

	return pmetricotlp.NewExportResponse(), nil
}

//
// Main
//

func StartGRPCServer(ctx context.Context, storage *storage.Storage, addr string) error {
	logsService := &LogsGRPCService{
		ctx:     ctx,
		storage: storage,
	}
	tracesService := &TracesGRPCService{
		ctx:     ctx,
		storage: storage,
	}
	metricsService := &MetricsGRPCService{
		ctx:     ctx,
		storage: storage,
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	plogotlp.RegisterGRPCServer(server, logsService)
	ptraceotlp.RegisterGRPCServer(server, tracesService)
	pmetricotlp.RegisterGRPCServer(server, metricsService)
	reflection.Register(server)

	log.Printf("GRPC server listening on %s", lis.Addr())
	err = server.Serve(lis)

	return err
}
