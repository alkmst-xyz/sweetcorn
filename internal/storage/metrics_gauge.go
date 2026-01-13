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
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	gauge             pmetric.Gauge
}

type gaugeMetrics struct {
	gaugeModels []*gaugeModel
	insertSQL   string
	count       int
}

func (g *gaugeMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics pmetric.Metric) {
	gauge := metrics.Gauge()
	g.count += gauge.DataPoints().Len()
	g.gaugeModels = append(g.gaugeModels, &gaugeModel{
		metricName:        metrics.Name(),
		metricDescription: metrics.Description(),
		metricUnit:        metrics.Unit(),
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		gauge: gauge,
	})
}

func (g *gaugeMetrics) insert(ctx context.Context, db *sql.DB) error {
	if g.count == 0 {
		return nil
	}

	for _, model := range g.gaugeModels {
		resAttr := model.metadata.ResAttr
		serviceName := getServiceName(resAttr)

		resAttrBytes, resAttrErr := json.Marshal(resAttr.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json metric resource attributes: %w", resAttrErr)
		}

		for i := 0; i < model.gauge.DataPoints().Len(); i++ {
			dp := model.gauge.DataPoints().At(i)

			attrBytes, attrErr := json.Marshal(dp.Attributes())
			if attrErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", attrErr)
			}

			_, err := db.ExecContext(ctx, g.insertSQL,
				dp.Timestamp().AsTime(),
				serviceName,
				model.metricName,
				model.metricDescription,
				model.metricUnit,
				resAttrBytes,
				model.metadata.ScopeInstr.Name(),
				model.metadata.ScopeInstr.Version(),
				attrBytes,
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
