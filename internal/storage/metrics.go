package storage

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

var supportedMetricTypes = map[pmetric.MetricType]string{
	pmetric.MetricTypeGauge:                createMetricsGaugeTable,
	pmetric.MetricTypeSum:                  createMetricsSumTable,
	pmetric.MetricTypeHistogram:            createMetricsHistogramTable,
	pmetric.MetricTypeExponentialHistogram: createMetricsExponentialHistogramTable,
	pmetric.MetricTypeSummary:              createMetricsSummaryTable,
}

// MetricsModel is used to group metric data and insert into duckdb
// any type of metrics need implement it.
type MetricsModel interface {
	// Add used to bind MetricsMetaData to a specific metric then put them into a slice
	Add(resAttr pcommon.Map, resURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, metrics pmetric.Metric)

	// insert is used to insert metric data to duckdb
	insert(ctx context.Context, db *sql.DB) error
}

// MetricsMetaData contain specific metric data
type MetricsMetaData struct {
	ResAttr    pcommon.Map
	ResURL     string
	ScopeURL   string
	ScopeInstr pcommon.InstrumentationScope
}

// Query base DTO
type MetricsRecordBase struct {
	Timestamp          int64          `json:"timestamp"`
	ServiceName        string         `json:"serviceName"`
	MetricName         string         `json:"metricName"`
	MetricDescription  string         `json:"metricDescription"`
	MetricUnit         string         `json:"metricUnit"`
	ResourceAttributes map[string]any `json:"resourceAttributes"`
	ScopeName          string         `json:"scopeName"`
	ScopeVersion       string         `json:"scopeVersion"`
	Attributes         map[string]any `json:"attributes"`
}

// InsertMetrics insert metric data into duckdb concurrently
func InsertMetrics(ctx context.Context, db *sql.DB, metricsMap map[pmetric.MetricType]MetricsModel) error {
	errsChan := make(chan error, len(supportedMetricTypes))
	wg := &sync.WaitGroup{}
	for _, m := range metricsMap {
		wg.Add(1)
		go func(m MetricsModel, wg *sync.WaitGroup) {
			errsChan <- m.insert(ctx, db)
			wg.Done()
		}(m, wg)
	}
	wg.Wait()
	close(errsChan)
	var errs error
	for err := range errsChan {
		errs = errors.Join(errs, err)
	}
	return errs
}

// NewMetricsModel create a model for contain different metric data
func NewMetricsModel() map[pmetric.MetricType]MetricsModel {
	return map[pmetric.MetricType]MetricsModel{
		pmetric.MetricTypeGauge: &gaugeMetrics{
			insertSQL: insertMetricsGaugeSQL,
		},
		pmetric.MetricTypeSum: &sumMetrics{
			insertSQL: insertMetricsSumSQL,
		},
		pmetric.MetricTypeHistogram: &histogramMetrics{
			insertSQL: insertMetricsHistogramSQL,
		},
		pmetric.MetricTypeExponentialHistogram: &expHistogramMetrics{
			insertSQL: insertMetricsExponentialHistogramSQL,
		},
		pmetric.MetricTypeSummary: &summaryMetrics{
			insertSQL: insertMetricsSummarySQL,
		},
	}
}

func IngestMetricsData(ctx context.Context, db *sql.DB, md pmetric.Metrics) error {
	metricsMap := NewMetricsModel()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		metrics := md.ResourceMetrics().At(i)
		resAttr := metrics.Resource().Attributes()
		for j := 0; j < metrics.ScopeMetrics().Len(); j++ {
			rs := metrics.ScopeMetrics().At(j).Metrics()
			scopeInstr := metrics.ScopeMetrics().At(j).Scope()
			scopeURL := metrics.ScopeMetrics().At(j).SchemaUrl()
			for k := 0; k < rs.Len(); k++ {
				r := rs.At(k)
				if r.Type() == pmetric.MetricTypeEmpty {
					return errors.New("metrics type is unset")
				}
				m, ok := metricsMap[r.Type()]
				if !ok {
					return errors.New("unsupported metrics type")
				}
				m.Add(resAttr, metrics.SchemaUrl(), scopeInstr, scopeURL, r)
			}
		}
	}

	return InsertMetrics(ctx, db, metricsMap)
}

// TODO: modify to use in DuckDB
// https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto#L358
// define two types for one datapoint value, clickhouse only use one value of float64 to store them
func getValue(intValue int64, floatValue float64, dataType any) float64 {
	switch t := dataType.(type) {
	case pmetric.ExemplarValueType:
		switch t {
		case pmetric.ExemplarValueTypeDouble:
			return floatValue
		case pmetric.ExemplarValueTypeInt:
			return float64(intValue)
		case pmetric.ExemplarValueTypeEmpty:
			return 0.0
		default:
			// logger.Warn("Can't find a suitable value for ExemplarValueType, use 0.0 as default")
			return 0.0
		}
	case pmetric.NumberDataPointValueType:
		switch t {
		case pmetric.NumberDataPointValueTypeDouble:
			return floatValue
		case pmetric.NumberDataPointValueTypeInt:
			return float64(intValue)
		case pmetric.NumberDataPointValueTypeEmpty:
			return 0.0
		default:
			// logger.Warn("Can't find a suitable value for NumberDataPointValueType, use 0.0 as default")
			return 0.0
		}
	default:
		// logger.Warn("unsupported ValueType, current support: ExemplarValueType, NumberDataPointValueType, ues 0.0 as default")
		return 0.0
	}
}
