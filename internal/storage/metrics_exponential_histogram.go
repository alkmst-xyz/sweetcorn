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
	DefaultMetricsExponentialHistogramTableName = "otel_metrics_exponential_histogram"

	createMetricsExponentialHistogramTable = `
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
		data_dp_scale						INT32,
		data_dp_zero_count					UINT64,
		data_dp_positive_offset				INT32,
		data_dp_positive_bucket_counts		UINT64[],
		data_dp_negative_offset				INT32,
		data_dp_negative_bucket_counts		UINT64[],
		data_dp_flags						UINT32,
		data_dp_min							DOUBLE,
		data_dp_max							DOUBLE,
		data_dp_zero_threshold				DOUBLE,
		data_aggregation_temporality		INT32
	);`

	insertMetricsExponentialHistogramSQL = `
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
		data_dp_scale,
		data_dp_zero_count,
		data_dp_positive_offset,
		data_dp_positive_bucket_counts,
		data_dp_negative_offset,
		data_dp_negative_bucket_counts,
		data_dp_flags,
		data_dp_min,
		data_dp_max,
		data_dp_zero_threshold,
		data_aggregation_temporality
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsExponentialHistogramSQL = `
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
	data_dp_scale,
	data_dp_zero_count,
	data_dp_positive_offset,
	data_dp_positive_bucket_counts,
	data_dp_negative_offset,
	data_dp_negative_bucket_counts,
	data_dp_min,
	data_dp_max
FROM
	%s
ORDER BY
	data_dp_time_unix_nano DESC
LIMIT
	100;`
)

type expHistogramModel struct {
	baseMetricsModel
	expHistogram pmetric.ExponentialHistogram
}

type expHistogramMetrics struct {
	data      []*expHistogramModel
	count     int
	insertSQL string
}

func (m *expHistogramMetrics) add(
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
	expHistogram := metric.ExponentialHistogram()
	m.count += expHistogram.DataPoints().Len()
	m.data = append(m.data,
		&expHistogramModel{
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
			expHistogram: expHistogram,
		})
}

func (m *expHistogramMetrics) insert(ctx context.Context, db *sql.DB) error {
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

		aggTemp := model.expHistogram.AggregationTemporality()
		dataPoints := model.expHistogram.DataPoints()
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
				dp.Scale(),
				dp.ZeroCount(),
				dp.Positive().Offset(),
				dp.Positive().BucketCounts().AsRaw(),
				dp.Negative().Offset(),
				dp.Negative().BucketCounts().AsRaw(),
				// exemplars
				dp.Flags(),
				dp.Min(),
				dp.Max(),
				dp.ZeroThreshold(),
				aggTemp,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type MetricsExponentialHistogramRecord struct {
	MetricsRecordBase
	Count                uint64   `json:"count"`
	Sum                  float64  `json:"sum"`
	Scale                int32    `json:"scale"`
	ZeroCount            uint64   `json:"zeroCount"`
	PositiveOffset       int32    `json:"positiveOffset"`
	PositiveBucketCounts []uint64 `json:"positiveBucketCounts"`
	NegativeOffset       int32    `json:"negativeOffset"`
	NegativeBucketCounts []uint64 `json:"negativeBucketCounts"`
	Min                  float64  `json:"min"`
	Max                  float64  `json:"max"`
}

func QueryMetricsExponentialHistogram(ctx context.Context, s *Storage) ([]MetricsExponentialHistogramRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryMetricsExponentialHistogramSQL, s.Config.MetricsExponentialHistogramTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]MetricsExponentialHistogramRecord, 0)

	for rows.Next() {
		var result MetricsExponentialHistogramRecord

		var timestamp time.Time

		var positiveBucketCounts duckdb.Composite[[]uint64]
		var negativeBucketCounts duckdb.Composite[[]uint64]

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
			&result.Scale,
			&result.ZeroCount,
			&result.PositiveOffset,
			&positiveBucketCounts,
			&result.NegativeOffset,
			&negativeBucketCounts,
			&result.Min,
			&result.Max,
		)
		if err != nil {
			return nil, err
		}

		// convert timestamp to unix epoch in microseconds
		result.Timestamp = timestamp.UnixMicro()

		result.PositiveBucketCounts = positiveBucketCounts.Get()
		result.NegativeBucketCounts = negativeBucketCounts.Get()

		results = append(results, result)
	}

	return results, nil
}
