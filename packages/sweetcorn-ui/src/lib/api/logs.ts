import type { FetchType } from './types';
import { API_BASE } from './utils';

export type LogRecord = {
	timestamp: number;
	timestampObserved: number;
	severityNumber: number;
	severityText: string;
	body: string;
	logAttributes: { [key: string]: unknown };
	logDroppedAttributesCount: number;
	flags: number;
	traceId: string;
	spanId: string;
	eventName: string;
	resourceAttributes: { [key: string]: unknown };
	resourceDroppedAttributesCount: number;
	resourceServiceName: string;
	resourceSchemaUrl: string;
	scopeName: string;
	scopeVersion: string;
	scopeAttributes: { [key: string]: unknown };
	scopeDroppedAttributesCount: number;
	scopeSchemaUrl: string;
};

export async function getLogs(fetch: FetchType) {
	const response = await fetch(`${API_BASE}api/v1/logs`);
	return (await response.json()) as Array<LogRecord>;
}
