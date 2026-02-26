package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	DefaultMetricsSumTableName = "otel_metrics_sum"

	// Note: Ducklake uses `FLOAT64` for `DOUBLE`.
	createMetricsSumTable = `
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
		data_dp_value_double				DOUBLE,
		data_dp_value_int					INT64,
		data_dp_flags						UINT32,
		data_aggregation_temporality		INT32,
		data_is_monotonic					BOOLEAN
	);`

	insertMetricsSumSQL = `
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
		data_dp_value_double,
		data_dp_value_int,
		data_dp_flags,
		data_aggregation_temporality,
		data_is_monotonic
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsSumSQL = `
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
	data_dp_value_double,
	data_aggregation_temporality,
	data_is_monotonic
FROM
	%s
ORDER BY
	data_dp_time_unix_nano DESC
LIMIT
	100;`
)

type sumMetricModel struct {
	baseMetricsModel
	sum pmetric.Sum
}

type sumMetrics struct {
	data      []*sumMetricModel
	count     int
	insertSQL string
}

func (m *sumMetrics) add(
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
	sum := metric.Sum()
	m.count += sum.DataPoints().Len()
	m.data = append(m.data,
		&sumMetricModel{
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
			sum: sum,
		})
}

func (m *sumMetrics) insert(ctx context.Context, db *sql.DB) error {
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

		aggTemp := model.sum.AggregationTemporality()
		isMonotonic := model.sum.IsMonotonic()
		dataPoints := model.sum.DataPoints()
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
				dp.DoubleValue(),
				dp.IntValue(),
				// exemplars
				dp.Flags(),
				aggTemp,
				isMonotonic,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type MetricsSumRecord struct {
	MetricsRecordBase
	Value                  float64 `json:"value"`
	AggregationTemporality int32   `json:"aggregationTemporality"`
	IsMonotonic            bool    `json:"isMonotonic"`
}

func QueryMetricsSum(ctx context.Context, s *Storage) ([]MetricsSumRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryMetricsSumSQL, s.Config.MetricsSumTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]MetricsSumRecord, 0)

	for rows.Next() {
		var result MetricsSumRecord

		var timestamp time.Time

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
			&result.Value,
			&result.AggregationTemporality,
			&result.IsMonotonic,
		)
		if err != nil {
			return nil, err
		}

		// convert timestamp to unix epoch in microseconds
		result.Timestamp = timestamp.UnixMicro()

		results = append(results, result)
	}

	return results, nil
}
