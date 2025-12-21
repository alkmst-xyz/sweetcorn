package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	createMetricsExponentialHistogramTable = `
CREATE TABLE IF NOT EXISTS
	otel_metrics_exponential_histogram (
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
	otel_metrics_exponential_histogram (
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
	otel_metrics_exponential_histogram
ORDER BY
	timestamp DESC
LIMIT
	100;`
)

type expHistogramModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	expHistogram      pmetric.ExponentialHistogram
}

type expHistogramMetrics struct {
	expHistogramModels []*expHistogramModel
	insertSQL          string
	count              int
}

func (e *expHistogramMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics pmetric.Metric) {
	expHistogram := metrics.ExponentialHistogram()
	e.count += expHistogram.DataPoints().Len()
	e.expHistogramModels = append(e.expHistogramModels, &expHistogramModel{
		metricName:        metrics.Name(),
		metricDescription: metrics.Description(),
		metricUnit:        metrics.Unit(),
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		expHistogram: expHistogram,
	})
}

func (e *expHistogramMetrics) insert(ctx context.Context, db *sql.DB) error {
	if e.count == 0 {
		return nil
	}

	for _, model := range e.expHistogramModels {
		resAttr := model.metadata.ResAttr
		serviceName := getServiceName(resAttr)

		resAttrBytes, resAttrErr := json.Marshal(resAttr.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json metric resource attributes: %w", resAttrErr)
		}

		for i := 0; i < model.expHistogram.DataPoints().Len(); i++ {
			dp := model.expHistogram.DataPoints().At(i)

			attrBytes, attrErr := json.Marshal(dp.Attributes())
			if attrErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", attrErr)
			}

			_, err := db.ExecContext(ctx, e.insertSQL,
				dp.Timestamp().AsTime(),
				serviceName,
				model.metricName,
				model.metricDescription,
				model.metricUnit,
				resAttrBytes,
				model.metadata.ScopeInstr.Name(),
				model.metadata.ScopeInstr.Version(),
				attrBytes,
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

func QueryMetricsExponentialHistogram(ctx context.Context, db *sql.DB) ([]MetricsExponentialHistogramRecord, error) {
	rows, err := db.QueryContext(ctx, queryMetricsExponentialHistogramSQL)
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
