import {
	getMetricsExponentialHistogram,
	getMetricsGauge,
	getMetricsHistogram,
	getMetricsSum,
	getMetricsSummary
} from '$lib/api';
import type { PageLoad } from './$types';

export const load: PageLoad = async ({ fetch }) => {
	return {
		gaugeMetrics: await getMetricsGauge(fetch),
		sumMetrics: await getMetricsSum(fetch),
		histogramMetrics: await getMetricsHistogram(fetch),
		exponentialHistogramMetrics: await getMetricsExponentialHistogram(fetch),
		summaryMetrics: await getMetricsSummary(fetch)
	};
};
