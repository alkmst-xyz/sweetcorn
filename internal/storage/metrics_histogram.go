package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	DefaultMetricsHistogramTableName = "otel_metrics_histogram"

	// Note: Ducklake uses `FLOAT64` for `DOUBLE`.
	createMetricsHistogramTable = `
CREATE TABLE IF NOT EXISTS
	%s (
		resource_service_name				VARCHAR,
		resource_attributes					JSON,
		resource_dropped_attributes_count	UINT32,
		resource_schema_url					VARCHAR,
		scope_name							VARCHAR,
		scope_version						VARCHAR,
		scope_attributes					JSON,
		scope_dropped_attributes_count		UINT32,
		scope_schema_url					VARCHAR,
		metric_name							VARCHAR,
		metric_description					VARCHAR,
		metric_unit							VARCHAR,
		metric_metadata						JSON,
		data_dp_attributes					JSON,
		data_dp_start_time_unix_nano		TIMESTAMP_NS,
		data_dp_time_unix_nano				TIMESTAMP_NS,
		data_dp_count						UINT64,
		data_dp_sum							DOUBLE,
		data_dp_bucket_counts				UINT64[],
		data_dp_explicit_bounds				DOUBLE[],
		data_dp_flags						UINT32,
		data_dp_min							DOUBLE,
		data_dp_max							DOUBLE,
		data_aggregation_temporality		INT32
	);`

	insertMetricsHistogramSQL = `
INSERT INTO
	%s (
		resource_service_name,
		resource_attributes,
		resource_dropped_attributes_count,
		resource_schema_url,
		scope_name,
		scope_version,
		scope_attributes,
		scope_dropped_attributes_count,
		scope_schema_url,
		metric_name,
		metric_description,
		metric_unit,
		metric_metadata,
		data_dp_attributes,
		data_dp_start_time_unix_nano,
		data_dp_time_unix_nano,
		data_dp_count,
		data_dp_sum,
		data_dp_bucket_counts,
		data_dp_explicit_bounds,
		data_dp_flags,
		data_dp_min,
		data_dp_max,
		data_aggregation_temporality
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsHistogramSQL = `
SELECT
	data_dp_time_unix_nano,
	resource_service_name,
	metric_name,
	metric_description,
	metric_unit,
	resource_attributes,
	scope_name,
	scope_version,
	data_dp_attributes,
	data_dp_count,
	data_dp_sum,
	data_dp_bucket_counts,
	data_dp_explicit_bounds,
	data_dp_min,
	data_dp_max
FROM
	%s	
ORDER BY
	data_dp_time_unix_nano DESC
LIMIT
	100;`
)

type histogramModel struct {
	baseMetricsModel
	histogram pmetric.Histogram
}

type histogramMetrics struct {
	data      []*histogramModel
	count     int
	insertSQL string
}

func (m *histogramMetrics) add(
	resAttrsBytes []byte,
	resDroppedAttrsCount uint32,
	resServiceName string,
	resSchemaUrl string,
	scopeName string,
	scopeVersion string,
	scopeAttrsBytes []byte,
	scopeDroppedAttrsCount uint32,
	scopeSchemaUrl string,
	metric pmetric.Metric,

) {
	histogram := metric.Histogram()
	m.count += histogram.DataPoints().Len()
	m.data = append(m.data,
		&histogramModel{
			baseMetricsModel: baseMetricsModel{
				resAttrsBytes:          resAttrsBytes,
				resDroppedAttrsCount:   resDroppedAttrsCount,
				resServiceName:         resServiceName,
				resSchemaUrl:           resSchemaUrl,
				scopeName:              scopeName,
				scopeVersion:           scopeVersion,
				scopeAttrsBytes:        scopeAttrsBytes,
				scopeDroppedAttrsCount: scopeDroppedAttrsCount,
				scopeSchemaUrl:         scopeSchemaUrl,
				metric:                 metric,
			},
			histogram: histogram,
		})
}

func (m *histogramMetrics) insert(ctx context.Context, db *sql.DB) error {
	if m.count == 0 {
		return nil
	}

	for _, model := range m.data {
		metricName := model.metric.Name()
		metricDescription := model.metric.Description()
		metricUnit := model.metric.Unit()
		metricMetadataBytes, metricMetadataErr := json.Marshal(model.metric.Metadata())
		if metricMetadataErr != nil {
			return fmt.Errorf("failed to marshal json metric metadata: %w", metricMetadataErr)
		}

		aggTemp := model.histogram.AggregationTemporality()
		dataPoints := model.histogram.DataPoints()
		for i := range dataPoints.Len() {
			dp := dataPoints.At(i)

			dpAttrsBytes, dpAttrsErr := json.Marshal(dp.Attributes())
			if dpAttrsErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", dpAttrsErr)
			}

			_, err := db.ExecContext(ctx, m.insertSQL,
				model.resServiceName,
				model.resAttrsBytes,
				model.resDroppedAttrsCount,
				model.resSchemaUrl,
				model.scopeName,
				model.scopeVersion,
				model.scopeAttrsBytes,
				model.scopeDroppedAttrsCount,
				model.scopeSchemaUrl,
				metricName,
				metricDescription,
				metricUnit,
				metricMetadataBytes,
				dpAttrsBytes,
				dp.StartTimestamp().AsTime(),
				dp.Timestamp().AsTime(),
				dp.Count(),
				dp.Sum(),
				dp.BucketCounts().AsRaw(),
				dp.ExplicitBounds().AsRaw(),
				// exemplars
				dp.Flags(),
				dp.Min(),
				dp.Max(),
				aggTemp,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type MetricsHistogramRecord struct {
	MetricsRecordBase
	Count          uint64    `json:"count"`
	Sum            float64   `json:"sum"`
	BucketCounts   []uint64  `json:"bucketCounts"`
	ExplicitBounds []float64 `json:"explicitBounds"`
	Min            float64   `json:"min"`
	Max            float64   `json:"max"`
}

func QueryMetricsHistogram(ctx context.Context, s *Storage) ([]MetricsHistogramRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryMetricsHistogramSQL, s.Config.MetricsHistogramTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]MetricsHistogramRecord, 0)

	for rows.Next() {
		var result MetricsHistogramRecord

		var timestamp time.Time

		var bucketCounts duckdb.Composite[[]uint64]
		var explicitBounds duckdb.Composite[[]float64]

		err := rows.Scan(
			&timestamp,
			&result.ServiceName,
			&result.MetricName,
			&result.MetricDescription,
			&result.MetricUnit,
			&result.ResourceAttributes,
			&result.ScopeName,
			&result.ScopeVersion,
			&result.Attributes,
			&result.Count,
			&result.Sum,
			&bucketCounts,
			&explicitBounds,
			&result.Min,
			&result.Max,
		)
		if err != nil {
			return nil, err
		}

		// convert timestamp to unix epoch in microseconds
		result.Timestamp = timestamp.UnixMicro()

		result.BucketCounts = bucketCounts.Get()
		result.ExplicitBounds = explicitBounds.Get()

		results = append(results, result)
	}

	return results, nil
}
