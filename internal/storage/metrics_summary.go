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
	DefaultMetricsSummaryTableName = "otel_metrics_summary"

	createMetricsSummaryTable = `
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
		data_dp_quantile_values_quantiles	DOUBLE[],
		data_dp_quantile_values_values		DOUBLE[]
	);`

	insertMetricsSummarySQL = `
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
		data_dp_quantile_values_quantiles,
		data_dp_quantile_values_values
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsSummarySQL = `
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
	data_dp_quantile_values_quantiles,
	data_dp_quantile_values_values
FROM
	%s
ORDER BY
	data_dp_time_unix_nano DESC
LIMIT
	100;`
)

type summaryModel struct {
	baseMetricsModel
	summary pmetric.Summary
}

type summaryMetrics struct {
	data      []*summaryModel
	count     int
	insertSQL string
}

func (m *summaryMetrics) add(
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
	summary := metric.Summary()
	m.count += summary.DataPoints().Len()
	m.data = append(m.data,
		&summaryModel{
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
			summary: summary,
		})
}

func (m *summaryMetrics) insert(ctx context.Context, db *sql.DB) error {
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

		dataPoints := model.summary.DataPoints()
		for i := range dataPoints.Len() {
			dp := dataPoints.At(i)

			dpAttrsBytes, dpAttrsErr := json.Marshal(dp.Attributes())
			if dpAttrsErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", dpAttrsErr)
			}

			quantiles, values := convertValueAtQuantile(dp.QuantileValues())

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
				quantiles,
				values,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type MetricsSummaryRecord struct {
	MetricsRecordBase
	Count             uint64    `json:"count"`
	Sum               float64   `json:"sum"`
	QuantileQuantiles []float64 `json:"quantileQuantiles"`
	QuantileValues    []float64 `json:"quantileValues"`
}

func QueryMetricsSummary(ctx context.Context, s *Storage) ([]MetricsSummaryRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryMetricsSummarySQL, s.Config.MetricsSummaryTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]MetricsSummaryRecord, 0)

	for rows.Next() {
		var result MetricsSummaryRecord

		var timestamp time.Time

		var quantileQuantiles duckdb.Composite[[]float64]
		var quantileValues duckdb.Composite[[]float64]

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
			&result.Sum,
			&result.Count,
			&quantileQuantiles,
			&quantileValues,
		)
		if err != nil {
			return nil, err
		}

		// convert timestamp to unix epoch in microseconds
		result.Timestamp = timestamp.UnixMicro()

		result.QuantileQuantiles = quantileQuantiles.Get()
		result.QuantileValues = quantileValues.Get()

		results = append(results, result)
	}

	return results, nil
}

func convertValueAtQuantile(valueAtQuantile pmetric.SummaryDataPointValueAtQuantileSlice) (quantiles []float64, values []float64) {
	for i := 0; i < valueAtQuantile.Len(); i++ {
		value := valueAtQuantile.At(i)
		quantiles = append(quantiles, value.Quantile())
		values = append(values, value.Value())
	}

	return quantiles, values
}
