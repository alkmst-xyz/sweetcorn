import type {
	FetchType,
	MetricsRecordExponentialHistogram,
	MetricsRecordGauge,
	MetricsRecordHistogram,
	MetricsRecordSum,
	MetricsRecordSummary
} from './types';
import { API_BASE } from './utils';

export async function getMetricsGauge(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/metrics/gauge`);
	return (await res.json()) as Array<MetricsRecordGauge>;
}

export async function getMetricsSum(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/metrics/sum`);
	return (await res.json()) as Array<MetricsRecordSum>;
}

export async function getMetricsHistogram(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/metrics/histogram`);
	return (await res.json()) as Array<MetricsRecordHistogram>;
}

export async function getMetricsExponentialHistogram(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/metrics/exponential-histogram`);
	return (await res.json()) as Array<MetricsRecordExponentialHistogram>;
}

export async function getMetricsSummary(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/metrics/summary`);
	return (await res.json()) as Array<MetricsRecordSummary>;
}
