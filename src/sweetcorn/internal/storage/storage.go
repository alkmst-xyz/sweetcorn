package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
)

type StorageType string

const (
	DuckDB   StorageType = "duckdb"
	DuckLake StorageType = "ducklake"
)

type Config struct {
	StorageType    StorageType
	DataSourceName string
}

func openDuckDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createDataDir(dataDir string) error {
	err := os.MkdirAll(dataDir, 0754)
	if err != nil {
		return fmt.Errorf("failed to create data dir: %s", err)
	}
	return nil
}

func (cfg *Config) initDB(ctx context.Context) (*sql.DB, error) {
	switch cfg.StorageType {
	case DuckDB:
		err := createDataDir(".sweetcorn_data") // TODO: get path from config
		if err != nil {
			return nil, err
		}

		db, err := openDuckDB(cfg.DataSourceName)
		if err != nil {
			return nil, err
		}

		log.Printf("Connected to DuckDB at %s", cfg.DataSourceName)

		return db, nil

	case DuckLake:
		db, err := openDuckDB("") // create in-memory instance
		if err != nil {
			return nil, err
		}

		err = SetupDuckLake(ctx, cfg, db)
		if err != nil {
			return nil, fmt.Errorf("failed to setup ducklake: %w", err)
		}

		log.Printf("Connected to DuckLake")

		return db, nil

	default:
		return nil, fmt.Errorf("unknown storage type: %s", cfg.StorageType)
	}

}

// Create all tables used by sweetcorn
// TODO: make table names configurable
//
// otel_logs
// otel_traces
// otel_metrics_gauge
// otel_metrics_sum
// otel_metrics_histogram
// otel_metrics_exponential_histogram
// otel_metrics_summary
func createTables(cfg *Config, ctx context.Context, db *sql.DB) error {
	tablesConfig := []string{
		createLogsTableSQL,
		createTracesTableSQL,
		createMetricsGaugeTable,
		createMetricsSumTable,
		createMetricsHistogramTable,
		createMetricsExponentialHistogramTable,
		createMetricsSummaryTable,
	}

	for _, tblDDL := range tablesConfig {
		if _, err := db.ExecContext(ctx, tblDDL); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// TODO: re-consider accepting ctx from parent
func (cfg *Config) NewStorage(ctx context.Context) (*sql.DB, error) {
	db, err := cfg.initDB(ctx)
	if err != nil {
		return nil, err
	}

	err = createTables(cfg, ctx, db)
	if err != nil {
		return nil, err
	}

	return db, nil
}
