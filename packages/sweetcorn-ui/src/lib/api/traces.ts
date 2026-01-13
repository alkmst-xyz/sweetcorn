import type { FetchType, ServicesResponse, TraceRecord } from './types';
import { API_BASE } from './utils';

export async function getTraces(fetch: FetchType) {
	const res = await fetch(`${API_BASE}api/v1/traces`);
	return (await res.json()) as Array<TraceRecord>;
}

export async function getDistinctTraceServices(fetch: FetchType) {
	const res = await fetch(`${API_BASE}jaeger/api/services`);
	return (await res.json()) as ServicesResponse;
}

export async function getDistinctTraceOperations(fetch: FetchType) {
	const res = await fetch(`${API_BASE}jaeger/api/operations`);
	return (await res.json()) as ServicesResponse;
}
