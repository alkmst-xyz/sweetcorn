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

	createMetricsSumTable = `
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
		value					DOUBLE,
		aggregation_temporality	INTEGER,
		is_monotonic			BOOLEAN
	);`

	insertMetricsSumSQL = `
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
		value,
		aggregation_temporality,
		is_monotonic
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsSumSQL = `
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
	value,
	aggregation_temporality,
	is_monotonic
FROM
	%s
ORDER BY
	timestamp DESC
LIMIT
	100;`
)

type sumModel struct {
	baseMetricsModel
	sum pmetric.Sum
}

type sumMetrics struct {
	data      []*sumModel
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
		&sumModel{
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
			sum: sum,
		})
}

func (m *sumMetrics) insert(ctx context.Context, db *sql.DB) error {
	if m.count == 0 {
		return nil
	}

	for _, model := range m.data {
		dataPoints := model.sum.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)

			value := getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType())
			aggTemp := model.sum.AggregationTemporality()
			isMonotonic := model.sum.IsMonotonic()

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
				value,
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
