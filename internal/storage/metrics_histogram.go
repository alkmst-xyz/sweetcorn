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
	createMetricsHistogramTable = `
CREATE TABLE IF NOT EXISTS
	otel_metrics_histogram (
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
	otel_metrics_histogram (
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
	otel_metrics_histogram
ORDER BY
	timestamp DESC
LIMIT
	100;`
)

type histogramModel struct {
	metricName        string
	metricDescription string
	metricUnit        string
	metadata          *MetricsMetaData
	histogram         pmetric.Histogram
}

type histogramMetrics struct {
	histogramModel []*histogramModel
	insertSQL      string
	count          int
}

func (h *histogramMetrics) Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics pmetric.Metric) {
	histogram := metrics.Histogram()
	h.count += histogram.DataPoints().Len()
	h.histogramModel = append(h.histogramModel, &histogramModel{
		metricName:        metrics.Name(),
		metricDescription: metrics.Description(),
		metricUnit:        metrics.Unit(),
		metadata: &MetricsMetaData{
			ResAttr:    resAttr,
			ResURL:     resURL,
			ScopeURL:   scopeURL,
			ScopeInstr: scopeInstr,
		},
		histogram: histogram,
	})
}

func (h *histogramMetrics) insert(ctx context.Context, db *sql.DB) error {
	if h.count == 0 {
		return nil
	}

	for _, model := range h.histogramModel {
		resAttr := model.metadata.ResAttr
		serviceName := getServiceName(resAttr)

		resAttrBytes, resAttrErr := json.Marshal(resAttr.AsRaw())
		if resAttrErr != nil {
			return fmt.Errorf("failed to marshal json metric resource attributes: %w", resAttrErr)
		}

		for i := 0; i < model.histogram.DataPoints().Len(); i++ {
			dp := model.histogram.DataPoints().At(i)

			attrBytes, attrErr := json.Marshal(dp.Attributes())
			if attrErr != nil {
				return fmt.Errorf("failed to marshal json metric attributes: %w", attrErr)
			}

			_, err := db.ExecContext(ctx, h.insertSQL,
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
