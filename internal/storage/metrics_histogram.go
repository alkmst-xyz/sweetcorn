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

	createMetricsHistogramTable = `
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
		bucket_counts			UBIGINT[],
		explicit_bounds			DOUBLE[],
		min						DOUBLE,
		max						DOUBLE
	);`

	insertMetricsHistogramSQL = `
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
		bucket_counts,
		explicit_bounds,
		min,
		max
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsHistogramSQL = `
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
	bucket_counts,
	explicit_bounds,
	min,
	max
FROM
	%s	
ORDER BY
	timestamp DESC
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
				name:                   metric.Name(),
				description:            metric.Description(),
				unit:                   metric.Unit(),
			},
			histogram: histogram,
		})
}

func (m *histogramMetrics) insert(ctx context.Context, db *sql.DB) error {
	if m.count == 0 {
		return nil
	}

	for _, model := range m.data {

		dataPoints := model.histogram.DataPoints()
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
				dp.BucketCounts().AsRaw(),
				dp.ExplicitBounds().AsRaw(),
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
