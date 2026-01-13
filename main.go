package main

import (
	"context"
	"flag"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/alkmst-xyz/sweetcorn/internal/app"
	"github.com/alkmst-xyz/sweetcorn/internal/otlp"
	"github.com/alkmst-xyz/sweetcorn/internal/otlphttp"
	"github.com/alkmst-xyz/sweetcorn/internal/storage"
)

func main() {
	dataDir := flag.String("data-dir", ".sweetcorn_data", "Data directory.")
	dbName := flag.String("db-name", "main.db", "Main DuckDB file name.")
	storageType := flag.String("storage-type", "duckdb", "Storage type.")
	flag.Parse()

	ctx := context.Background()

	// create storage
	storageConfig := storage.StorageConfig{
		StorageType:                      storage.StorageType(*storageType),
		DataDir:                          *dataDir,
		DBName:                           *dbName,
		LogsTable:                        storage.DefaultLogsTableName,
		TracesTable:                      storage.DefaultTracesTableName,
		MetricsSumTable:                  storage.DefaultMetricsSumTableName,
		MetricsGaugeTable:                storage.DefaultMetricsGaugeTableName,
		MetricsHistogramTable:            storage.DefaultMetricsHistogramTableName,
		MetricsExponentialHistogramTable: storage.DefaultMetricsExponentialHistogramTableName,
		MetricsSummaryTable:              storage.DefaultMetricsSummaryTableName,
	}
	storage, err := storage.NewStorage(ctx, storageConfig)
	if err != nil {
		log.Fatalf("failed to initialize storage: %v", err)
	}
	defer storage.Close()

	// start servers
	const httpAddr = ":4318"
	const grpcAddr = ":4317"
	const appAddr = ":13579"

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return otlphttp.StartHTTPServer(ctx, storage, httpAddr)
	})
	g.Go(func() error {
		return otlp.StartGRPCServer(ctx, storage, grpcAddr)
	})
	g.Go(func() error {
		return app.StartWebApp(ctx, storage, appAddr)
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
