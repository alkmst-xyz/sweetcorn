package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	createMetricsSummaryTable = `
CREATE TABLE IF NOT EXISTS
	otel_metrics_summary (
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
		quantile_quantiles		DOUBLE[],
		quantile_values			DOUBLE[]
	);`

	insertMetricsSummarySQL = `
INSERT INTO
	otel_metrics_summary (
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
		quantile_quantiles,
		quantile_values
	)
VALUES
	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	queryMetricsSummarySQL = `
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
	quantile_quantiles,
	quantile_values
FROM
	otel_metrics_summary
ORDER BY
	timestamp DESC
LIMIT
	100;`
)

type summaryModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	summary           pmetric.Summary
}

type summaryMetrics struct {
	summaryModel []*summaryModel
	insertSQL    string
	count        int
}

func (s *summaryMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics pmetric.Metric) {
	summary := metrics.Summary()
	s.count += summary.DataPoints().Len()
	s.summaryModel = append(s.summaryModel, &summaryModel{
		metricName:        metrics.Name(),
		metricDescription: metrics.Description(),
		metricUnit:        metrics.Unit(),
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		summary: summary,
	})
}

func (s *summaryMetrics) insert(ctx context.Context, db *sql.DB) error {
	if s.count == 0 {
		return nil
	}

	for _, model := range s.summaryModel {
		resAttr := model.metadata.ResAttr
		serviceName := getServiceName(resAttr)

		resAttrBytes, resAttrErr := json.Marshal(resAttr.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json metric resource attributes: %w", resAttrErr)
		}
		for i := 0; i < model.summary.DataPoints().Len(); i++ {
			dp := model.summary.DataPoints().At(i)

			attrBytes, attrErr := json.Marshal(dp.Attributes())
			if attrErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", attrErr)
			}

			quantiles, values := convertValueAtQuantile(dp.QuantileValues())

			_, err := db.ExecContext(ctx, s.insertSQL,
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

func convertValueAtQuantile(valueAtQuantile pmetric.SummaryDataPointValueAtQuantileSlice) (quantiles []float64, values []float64) {
	for i := 0; i < valueAtQuantile.Len(); i++ {
		value := valueAtQuantile.At(i)
		quantiles = append(quantiles, value.Quantile())
		values = append(values, value.Value())
	}

	return quantiles, values
}
