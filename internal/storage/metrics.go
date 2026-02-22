package storage

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricsModel is used to group metric data and insert into duckdb
// any type of metrics need implement it.
type MetricsModel interface {
	// Add used to bind MetricsMetaData to a specific metric then put them into a slice
	Add(resSchemaUrl string, resAttrs pcommon.Map, scopeSchemaUrl string, scope pcommon.InstrumentationScope, metric pmetric.Metric)

	// insert is used to insert metric data to duckdb
	insert(ctx context.Context, db *sql.DB) error
}

type baseMetricsModel struct {
	resSchemaUrl   string
	resAttrs       pcommon.Map
	scopeSchemaUrl string
	scope          pcommon.InstrumentationScope
	name           string
	description    string
	unit           string
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
func InsertMetrics(ctx context.Context, s *Storage, metricsMap map[pmetric.MetricType]MetricsModel) error {
	errsChan := make(chan error, len(metricsMap))
	wg := &sync.WaitGroup{}
	for _, metricsModel := range metricsMap {
		wg.Add(1)
		go func(metricsModel MetricsModel, wg *sync.WaitGroup) {
			errsChan <- metricsModel.insert(ctx, s.DB)
			wg.Done()
		}(metricsModel, wg)
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
func NewMetricsModel(s *Storage) map[pmetric.MetricType]MetricsModel {
	return map[pmetric.MetricType]MetricsModel{
		pmetric.MetricTypeGauge: &gaugeMetrics{
			insertSQL: s.InsertMetricsGaugeSQL,
		},
		pmetric.MetricTypeSum: &sumMetrics{
			insertSQL: s.InsertMetricsSumSQL,
		},
		pmetric.MetricTypeHistogram: &histogramMetrics{
			insertSQL: s.InsertMetricsHistogramSQL,
		},
		pmetric.MetricTypeExponentialHistogram: &expHistogramMetrics{
			insertSQL: s.InsertMetricsExponentialHistogramSQL,
		},
		pmetric.MetricTypeSummary: &summaryMetrics{
			insertSQL: s.InsertMetricsSummarySQL,
		},
	}
}

func IngestMetricsData(ctx context.Context, s *Storage, metrics pmetric.Metrics) error {
	metricsMap := NewMetricsModel(s)

	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		resMetrics := metrics.ResourceMetrics().At(i)

		resSchemaUrl := resMetrics.SchemaUrl()
		resAttrs := resMetrics.Resource().Attributes()

		for j := 0; j < resMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resMetrics.ScopeMetrics().At(j)

			scopeSchemaUrl := scopeMetrics.SchemaUrl()
			scope := scopeMetrics.Scope()

			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)

				if metric.Type() == pmetric.MetricTypeEmpty {
					return errors.New("metrics type is unset")
				}

				metricsModel, ok := metricsMap[metric.Type()]
				if !ok {
					return errors.New("unsupported metrics type")
				}

				metricsModel.Add(resSchemaUrl, resAttrs, scopeSchemaUrl, scope, metric)
			}
		}
	}

	return InsertMetrics(ctx, s, metricsMap)
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
