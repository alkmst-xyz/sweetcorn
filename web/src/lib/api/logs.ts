import type { FetchType } from './types';
import { API_BASE } from './utils';

export type LogRecord = {
	timestamp: string;
	traceId: string;
	spanId: string;
	traceFlags: number;
	severityText: string;
	severityNumber: number;
	serviceName: string;
	body: string;
	resourceSchemaUrl: string;
	resourceAttributes: { [key: string]: unknown };
	scopeSchemaUrl: string;
	scopeName: string;
	scopeVersion: string;
	scopeAttributes: { [key: string]: unknown };
	logAttributes: { [key: string]: unknown };
};

export async function getLogs(fetch: FetchType) {
	const response = await fetch(`${API_BASE}/v1/logs`);
	return (await response.json()) as Array<LogRecord>;
}
