import type { FetchType } from './types';
import { API_BASE } from './utils';

export interface TraceRecord {
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
}

export async function getTraces(fetch: FetchType) {
	const res = await fetch(`${API_BASE}/v1/traces`);
	return { traces: (await res.json()) as Array<TraceRecord> };
}
