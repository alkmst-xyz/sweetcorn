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
	DefaultMetricsGaugeTableName = "otel_metrics_gauge"

	createMetricsGaugeTable = `
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
		data_dp_flags						UINT32
	);`

	insertMetricsGaugeSQL = `
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
		data_dp_flags
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsGaugeSQL = `
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
	data_dp_value_double
FROM
	%s	
ORDER BY
	data_dp_time_unix_nano DESC
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

func (m *gaugeMetrics) add(
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
	gauge := metric.Gauge()
	m.count += gauge.DataPoints().Len()
	m.data = append(m.data,
		&gaugeModel{
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
			gauge: gauge,
		})
}

func (m *gaugeMetrics) insert(ctx context.Context, db *sql.DB) error {
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

		dataPoints := model.gauge.DataPoints()
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
