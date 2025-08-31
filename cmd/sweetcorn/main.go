package main

import (
	"context"
	"log"
	"os"

	_ "github.com/marcboeker/go-duckdb/v2"
	"golang.org/x/sync/errgroup"

	"github.com/alkmst-xyz/sweetcorn/internal/app"
	"github.com/alkmst-xyz/sweetcorn/internal/otlp"
	"github.com/alkmst-xyz/sweetcorn/internal/otlphttp"
	"github.com/alkmst-xyz/sweetcorn/internal/storage"
)

func main() {
	cfg := &storage.Config{
		DataSourceName: ".sweetcorn_data/main.db",
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

	if err := storage.CreateLogsTable(ctx, cfg, db); err != nil {
		log.Fatalf("Failed to create logs table: %v", err)
	}

	if err := storage.CreateTracesTable(ctx, cfg, db); err != nil {
		log.Fatalf("Failed to create traces table: %v", err)
	}

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
