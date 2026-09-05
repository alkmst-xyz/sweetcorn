import type {
  LogRecord,
  ServicesResponse,
  StatusResponse,
  TraceRecord,
  MetricsRecordExponentialHistogram,
  MetricsRecordGauge,
  MetricsRecordHistogram,
  MetricsRecordSum,
  MetricsRecordSummary,
} from './types'

export async function getHealthz() {
  const res = await fetch('/api/v1/healthz')
  return (await res.json()) as StatusResponse
}

export async function getLogs(): Promise<LogRecord[]> {
  const response = await fetch('/api/v1/logs')
  return (await response.json()) as Array<LogRecord>
}

export async function getTraces(): Promise<TraceRecord[]> {
  const res = await fetch('/api/v1/traces')
  return (await res.json()) as Array<TraceRecord>
}

export async function getDistinctTraceServices(): Promise<ServicesResponse> {
  const res = await fetch('/jaeger/api/services')
  return (await res.json()) as ServicesResponse
}

export async function getDistinctTraceOperations(): Promise<ServicesResponse> {
  const res = await fetch('/jaeger/api/operations')
  return (await res.json()) as ServicesResponse
}

export async function getMetricsGauge(): Promise<MetricsRecordGauge[]> {
  const res = await fetch('/api/v1/metrics/gauge')
  return (await res.json()) as Array<MetricsRecordGauge>
}

export async function getMetricsSum(): Promise<MetricsRecordSum[]> {
  const res = await fetch('/api/v1/metrics/sum')
  return (await res.json()) as Array<MetricsRecordSum>
}

export async function getMetricsHistogram(): Promise<MetricsRecordHistogram[]> {
  const res = await fetch('/api/v1/metrics/histogram')
  return (await res.json()) as Array<MetricsRecordHistogram>
}

export async function getMetricsExponentialHistogram(): Promise<
  MetricsRecordExponentialHistogram[]
> {
  const res = await fetch('/api/v1/metrics/exponential-histogram')
  return (await res.json()) as Array<MetricsRecordExponentialHistogram>
}

export async function getMetricsSummary(): Promise<MetricsRecordSummary[]> {
  const res = await fetch('/api/v1/metrics/summary')
  return (await res.json()) as Array<MetricsRecordSummary>
}
