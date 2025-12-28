package main

import (
	"context"
	"flag"
	"log"

	_ "github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/alkmst-xyz/sweetcorn/sweetcorn/internal/app"
	"github.com/alkmst-xyz/sweetcorn/sweetcorn/internal/otlp"
	"github.com/alkmst-xyz/sweetcorn/sweetcorn/internal/otlphttp"
	"github.com/alkmst-xyz/sweetcorn/sweetcorn/internal/storage"
)

func main() {
	storageType := flag.String("storage-type", "duckdb", "storage type")
	dsn := flag.String("dsn", ".sweetcorn_data/main.db", "data source name")
	flag.Parse()

	cfg := &storage.Config{
		StorageType:    storage.StorageType(*storageType),
		DataSourceName: *dsn,
	}

	ctx := context.Background()

	// create storage
	db, err := cfg.NewStorage(ctx)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer db.Close()

	// start servers
	const httpAddr = ":4318"
	const grpcAddr = ":4317"
	const appAddr = ":13579"

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return otlphttp.StartHTTPServer(ctx, db, httpAddr)
	})
	g.Go(func() error {
		return otlp.StartGRPCServer(ctx, db, grpcAddr)
	})
	g.Go(func() error {
		return app.StartWebApp(ctx, db, appAddr)
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
