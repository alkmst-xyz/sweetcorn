export type FetchType = typeof fetch;

export type TraceRecord = {
	timestamp: number;
	traceID: string;
	spanID: string;
	parentSpanID: string;
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
	linksTraceIDs: string[];
	linksSpanIDs: string[];
	linksTraceStates: string[];
	linksAttributes: { [key: string]: unknown }[];
};

// Jaeger Query API
export type ServicesResponse = {
	data: string[];
	errors: any;
	limit: number;
	offset: number;
	total: number;
};

export type TraceKeyValuePair = {
	key: string;
	type: string;
	value: any;
};

export type TraceProcess = {
	serviceName: string;
	tags: TraceKeyValuePair[];
};

export type TraceSpanReference = {
	refType: string;
	spanID: string;
	traceID: string;
};

export type TraceLog = {
	timestamp: number;
	fields: TraceKeyValuePair[];
	name: string;
};

export type Span = {
	traceID: string;
	spanID: string;
	processID: string;
	operationName: string;
	startTime: number;
	duration: number;
	logs: TraceLog[];
	references: TraceSpanReference[];
	tags: TraceKeyValuePair[];
	warnings: string[];
	flags: number;
	stackTraces: string[];
};

export type TraceResponse = {
	processes: { [key: string]: TraceProcess };
	traceID: string;
	warnings: string[];
	spans: Span[];
};

export type TracesResponse = {
	data: TraceResponse[];
	errors: any;
	limit: number;
	offset: number;
	total: number;
};

export type MetricsRecordBase = {
	timestamp: number;
	serviceName: string;
	metricName: string;
	metricDescription: string;
	metricUnit: string;
	resourceAttributes: { [key: string]: unknown };
	scopeName: string;
	scopeVersion: string;
	attributes: { [key: string]: unknown };
};

export type MetricsRecordGauge = MetricsRecordBase & {
	value: number;
};

export type MetricsRecordSum = MetricsRecordBase & {
	value: number;
	aggregationTemporality: number;
	isMonotonic: boolean;
};

export type MetricsRecordHistogram = MetricsRecordBase & {
	count: number;
	sum: number;
	bucketCounts: number[];
	explicitBounds: number[];
	min: number;
	max: number;
};

export type MetricsRecordExponentialHistogram = MetricsRecordBase & {
	count: number;
	sum: number;
	scale: number;
	zeroCount: number;
	positiveOffset: number;
	positiveBucketCounts: number[];
	negativeOffset: number;
	negativeBucketCounts: number[];
	min: number;
	max: number;
};

export type MetricsRecordSummary = MetricsRecordBase & {
	count: number;
	sum: number;
	quantileQuantiles: number[];
	quantileValues: number[];
};
