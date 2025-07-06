export type StatusResponse = {
	status: string;
};

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

export const API_BASE = 'http://127.0.0.1:3000';
