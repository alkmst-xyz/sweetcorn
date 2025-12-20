package storage

import (
	"context"
	"database/sql"
	"fmt"
)

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
func InitStorage(ctx context.Context, cfg *Config, db *sql.DB) error {
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
