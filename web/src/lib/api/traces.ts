import type { FetchType } from './types';
import { API_BASE } from './utils';

export type TraceRecord = {
	timestamp: string;
	traceId: string;
	spanId: string;
	parentSpanId: string;
	traceState: string;
	spanName: string;
	spanKind: string;
	serviceName: string;
	resourceAttributes: { [key: string]: unknown };
	scopeName: string;
	scopeVersion: string;
	spanAttributes: { [key: string]: unknown };
	duration: number;
	statusCode: string;
	statusMessage: string;
	eventsTimestamps: number[];
	eventsNames: string[];
	eventsAttributes: { [key: string]: unknown }[];
	linksTraceIds: string[];
	linksSpanIds: string[];
	linksTraceStates: string[];
	linksAttributes: { [key: string]: unknown }[];
};

export type ServicesResponse = {
	data: string;
	errors?: unknown;
	limit: number;
	offset: number;
	total: number;
};

export async function getTraces(fetch: FetchType) {
	const res = await fetch(`${API_BASE}/v1/traces`);
	return (await res.json()) as Array<TraceRecord>;
}
export async function getDistinctTraceServices(fetch: FetchType) {
	const res = await fetch(`${API_BASE}/v1/traces/services`);
	return (await res.json()) as ServicesResponse;
}

export async function getDistinctTraceOperations(fetch: FetchType) {
	const res = await fetch(`${API_BASE}/v1/traces/operations`);
	return (await res.json()) as ServicesResponse;
}
