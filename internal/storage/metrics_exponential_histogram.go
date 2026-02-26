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
		timestamp				TIMESTAMP_NS,
		service_name			VARCHAR,
		metric_name				VARCHAR,
		metric_description		VARCHAR,
		metric_unit				VARCHAR,
		resource_attributes		JSON,
		scope_name				VARCHAR,
		scope_version			VARCHAR,
		attributes				JSON,
		count					BIGINT,
		sum						DOUBLE,
		scale					INTEGER,
		zero_count				BIGINT,
		positive_offset			INTEGER,
		positive_bucket_counts	UBIGINT[],
		negative_offset			INTEGER,
		negative_bucket_counts 	UBIGINT[],
		min						DOUBLE,
		max						DOUBLE
	);`

	insertMetricsExponentialHistogramSQL = `
INSERT INTO
	%s (
		timestamp,
		service_name,
		metric_name,
		metric_description,
		metric_unit,
		resource_attributes,
		scope_name,
		scope_version,
		attributes,
		count,
		sum,
		scale,
		zero_count,
		positive_offset,
		positive_bucket_counts,
		negative_offset,
		negative_bucket_counts,
		min,
		max
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsExponentialHistogramSQL = `
SELECT
	timestamp,
	service_name,
	metric_name,
	metric_description,
	metric_unit,
	resource_attributes,
	scope_name,
	scope_version,
	attributes,
	count,
	sum,
	scale,
	zero_count,
	positive_offset,
	positive_bucket_counts,
	negative_offset,
	negative_bucket_counts,
	min,
	max
FROM
	%s
ORDER BY
	timestamp DESC
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
				name:                   metric.Name(),
				description:            metric.Description(),
				unit:                   metric.Unit(),
			},
			expHistogram: expHistogram,
		})
}

func (m *expHistogramMetrics) insert(ctx context.Context, db *sql.DB) error {
	if m.count == 0 {
		return nil
	}

	for _, model := range m.data {
		dataPoints := model.expHistogram.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)

			dpAttrsBytes, dpAttrsErr := json.Marshal(dp.Attributes())
			if dpAttrsErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", dpAttrsErr)
			}

			_, err := db.ExecContext(ctx, m.insertSQL,
				dp.Timestamp().AsTime(),
				model.resServiceName,
				model.name,
				model.description,
				model.unit,
				model.resAttrsBytes,
				model.scopeName,
				model.scopeVersion,
				dpAttrsBytes,
				dp.Count(),
				dp.Sum(),
				dp.Scale(),
				dp.ZeroCount(),
				dp.Positive().Offset(),
				dp.Positive().BucketCounts().AsRaw(),
				dp.Negative().Offset(),
				dp.Negative().BucketCounts().AsRaw(),
				dp.Min(),
				dp.Max(),
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
