package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
)

type StorageType string

const (
	DuckDB   StorageType = "duckdb"
	DuckLake StorageType = "ducklake"
)

type Storage struct {
	Type           StorageType
	DataSourceName string
	DB             *sql.DB
}

type StorageConfig struct {
	StorageType StorageType
	DataDir     string
	DBName      string
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

type StorageBackend interface {
	init(ctx context.Context, dsn string) (*sql.DB, error)
}

type DuckDBBackend struct{}

func (cfg DuckDBBackend) init(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}

	createTables(ctx, db)

	return db, nil
}

type DuckLakeBackend struct{}

func (cfg DuckLakeBackend) init(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, err
	}

	setupDuckLake(ctx, db)
	createTables(ctx, db)

	return db, nil
}

func getStorageBackend(storageType StorageType) (StorageBackend, error) {
	switch storageType {
	case DuckDB:
		return DuckDBBackend{}, nil

	case DuckLake:
		return DuckLakeBackend{}, nil

	default:
		return nil, fmt.Errorf("unknown storage type: %s", storageType)
	}

}

func NewStorage(ctx context.Context, cfg StorageConfig) (*Storage, error) {
	err := createDataDir(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	backend, err := getStorageBackend(cfg.StorageType)
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("%s/%s", cfg.DataDir, cfg.DBName)
	db, err := backend.init(ctx, dsn)
	if err != nil {
		return nil, err
	}

	log.Printf("Connected to DuckDB at %s", dsn)
	log.Printf("Storage initialized with storageType=%s", cfg.StorageType)

	return &Storage{DB: db}, nil
}

// Close storage connection.
func (s *Storage) Close() error {
	log.Printf("Closing storage connection.")

	if err := s.DB.Close(); err != nil {
		return err
	}

	return nil
}

func execQueries(ctx context.Context, db *sql.DB, queries []string) error {
	for _, query := range queries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return nil
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
func createTables(ctx context.Context, db *sql.DB) error {
	var createTableQueries = []string{
		createLogsTableSQL,
		createTracesTableSQL,
		createMetricsGaugeTable,
		createMetricsSumTable,
		createMetricsHistogramTable,
		createMetricsExponentialHistogramTable,
		createMetricsSummaryTable,
	}

	return execQueries(ctx, db, createTableQueries)
}

func setupDuckLake(ctx context.Context, db *sql.DB) error {
	var duckLakeSetupQueries = []string{
		installDuckLakeSQL,
		installPostgresSQL,
		createS3SecretSQL,
		createPostgresSecretSQL,
		attachDuckLakeSQL,
		useDuckLakeSQL,
	}

	return execQueries(ctx, db, duckLakeSetupQueries)
}
