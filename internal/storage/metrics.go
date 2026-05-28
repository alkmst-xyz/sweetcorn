package storage

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricsModel is used to group metric data and insert into duckdb
// any type of metrics need implement it.
type MetricsModel interface {
	// add used to bind MetricsMetaData to a specific metric then put them into a slice
	add(
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
	)

	// insert is used to insert metric data to duckdb
	insert(ctx context.Context, db *sql.DB) error
}

type baseMetricsModel struct {
	resAttrsBytes          []byte
	resDroppedAttrsCount   uint32
	resServiceName         string
	resSchemaUrl           string
	scopeName              string
	scopeVersion           string
	scopeAttrsBytes        []byte
	scopeDroppedAttrsCount uint32
	scopeSchemaUrl         string
	metric                 pmetric.Metric
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

// runIngestMetrics insert metric data concurrently.
func runIngestMetrics(ctx context.Context, s *Storage, metricsMap map[pmetric.MetricType]MetricsModel) error {
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

// newMetricsModel create a model for contain different metric data
func newMetricsModel(s *Storage) map[pmetric.MetricType]MetricsModel {
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

// IngestMetricsData ingests metrics data to storage.
//
// Metrics are composed of metadata and data:
//   - Metadata part contains a name, description, unit.
//   - Data is one of the possible types (Sum, Gauge, Histogram, ExponentialHistogram, Summary).
func IngestMetricsData(ctx context.Context, s *Storage, metricsData pmetric.Metrics) error {
	metricsMap := newMetricsModel(s)

	for i := range metricsData.ResourceMetrics().Len() {
		resMetrics := metricsData.ResourceMetrics().At(i)

		resAttrs := resMetrics.Resource().Attributes()
		resServiceName := getServiceName(resAttrs)
		resAttrsBytes, resAttrsErr := json.Marshal(resAttrs.AsRaw())
		if resAttrsErr != nil {
			return fmt.Errorf("failed to marshal json metric resource attributes: %w", resAttrsErr)
		}
		resDroppedAttrsCount := resMetrics.Resource().DroppedAttributesCount()
		resSchemaUrl := resMetrics.SchemaUrl()

		for j := range resMetrics.ScopeMetrics().Len() {
			scopeMetrics := resMetrics.ScopeMetrics().At(j)

			scope := scopeMetrics.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeAttrs := scope.Attributes()
			scopeDroppedAttrsCount := scope.DroppedAttributesCount()
			scopeAttrsBytes, scopeAttrsErr := json.Marshal(scopeAttrs.AsRaw())
			if scopeAttrsErr != nil {
				return fmt.Errorf("failed to marshal json metric scope attributes: %w", scopeAttrsErr)
			}
			scopeSchemaUrl := scopeMetrics.SchemaUrl()

			for k := range scopeMetrics.Metrics().Len() {
				metric := scopeMetrics.Metrics().At(k)

				if metric.Type() == pmetric.MetricTypeEmpty {
					return errors.New("metrics type is unset")
				}

				metricsModel, ok := metricsMap[metric.Type()]
				if !ok {
					return errors.New("unsupported metrics type")
				}

				metricsModel.add(
					resAttrsBytes, resDroppedAttrsCount, resServiceName, resSchemaUrl,
					scopeName, scopeVersion, scopeAttrsBytes, scopeDroppedAttrsCount, scopeSchemaUrl,
					metric,
				)
			}
		}
	}

	return runIngestMetrics(ctx, s, metricsMap)
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

func convertExemplars(exemplars pmetric.ExemplarSlice) ([]pcommon.Map, []time.Time, []int64, []float64, []string, []string) {
	var (
		attrs        []pcommon.Map
		times        []time.Time
		intValues    []int64
		doubleValues []float64
		traceIDs     []string
		spanIDs      []string
	)
	for i := 0; i < exemplars.Len(); i++ {
		exemplar := exemplars.At(i)

		attrs = append(attrs, exemplar.FilteredAttributes())
		times = append(times, exemplar.Timestamp().AsTime())
		intValues = append(intValues, exemplar.IntValue())
		doubleValues = append(doubleValues, exemplar.DoubleValue())

		spanID := exemplar.SpanID()
		spanIDs = append(spanIDs, hex.EncodeToString(spanID[:]))

		traceID := exemplar.TraceID()
		traceIDs = append(traceIDs, hex.EncodeToString(traceID[:]))
	}
	return attrs, times, intValues, doubleValues, spanIDs, traceIDs
}
