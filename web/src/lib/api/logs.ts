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

export async function getLogs() {
	const res = await fetch(`${API_BASE}/v1/logs`);
	return { logs: (await res.json()) as Array<LogRecord> };
}
