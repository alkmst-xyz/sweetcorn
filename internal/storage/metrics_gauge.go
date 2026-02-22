package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	DefaultMetricsGaugeTableName = "otel_metrics_gauge"

	createMetricsGaugeTable = `
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
		value					DOUBLE
	);`

	insertMetricsGaugeSQL = `
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
		value
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsGaugeSQL = `
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
	value
FROM
	%s	
ORDER BY
	timestamp DESC
LIMIT
	100;`
)

type gaugeModel struct {
	baseMetricsModel
	gauge pmetric.Gauge
}

type gaugeMetrics struct {
	data      []*gaugeModel
	count     int
	insertSQL string
}

func (g *gaugeMetrics) Add(
	resSchemaUrl string,
	resAttrs pcommon.Map,
	scopeSchemaUrl string,
	scope pcommon.InstrumentationScope,
	metric pmetric.Metric,
) {
	gauge := metric.Gauge()
	g.count += gauge.DataPoints().Len()
	g.data = append(g.data,
		&gaugeModel{
			baseMetricsModel: baseMetricsModel{
				resSchemaUrl:   resSchemaUrl,
				resAttrs:       resAttrs,
				scopeSchemaUrl: scopeSchemaUrl,
				scope:          scope,
				name:           metric.Name(),
				description:    metric.Description(),
				unit:           metric.Unit(),
			},
			gauge: gauge,
		})
}

func (g *gaugeMetrics) insert(ctx context.Context, db *sql.DB) error {
	if g.count == 0 {
		return nil
	}

	for _, model := range g.data {
		resAttrs := model.resAttrs
		resServiceName := getServiceName(resAttrs)
		resAttrsBytes, resAttrsErr := json.Marshal(resAttrs.AsRaw())
		if resAttrsErr != nil {
			return fmt.Errorf("failed to marshal json metric resource attributes: %w", resAttrsErr)
		}

		dataPoints := model.gauge.DataPoints()
		for i := 0; i < dataPoints.Len(); i++ {
			dp := dataPoints.At(i)

			dpAttrsBytes, dpAttrsErr := json.Marshal(dp.Attributes())
			if dpAttrsErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", dpAttrsErr)
			}

			_, err := db.ExecContext(ctx, g.insertSQL,
				dp.Timestamp().AsTime(),
				resServiceName,
				model.name,
				model.description,
				model.unit,
				resAttrsBytes,
				model.scope.Name(),
				model.scope.Version(),
				dpAttrsBytes,
				getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type MetricsGaugeRecord struct {
	MetricsRecordBase
	Value float64 `json:"value"`
}

func QueryMetricsGauge(ctx context.Context, s *Storage) ([]MetricsGaugeRecord, error) {
	rows, err := s.DB.QueryContext(ctx, renderQuery(queryMetricsGaugeSQL, s.Config.MetricsGaugeTable))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]MetricsGaugeRecord, 0)

	for rows.Next() {
		var result MetricsGaugeRecord

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
