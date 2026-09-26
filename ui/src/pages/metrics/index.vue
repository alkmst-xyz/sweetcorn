<script setup lang="ts">
import {
  getMetricsExponentialHistogram,
  getMetricsGauge,
  getMetricsHistogram,
  getMetricsSum,
  getMetricsSummary,
  type MetricsRecordExponentialHistogram,
  type MetricsRecordGauge,
  type MetricsRecordHistogram,
  type MetricsRecordSum,
  type MetricsRecordSummary,
} from '@/components/api'
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'
import { onMounted, ref } from 'vue'
import GaugeMetrics from './GaugeMetrics.vue'
import SumMetrics from './SumMetrics.vue'
import HistogramMetrics from './HistogramMetrics.vue'
import ExponentialHistogramMetrics from './ExponentialHistogramMetrics.vue'
import SummaryMetrics from './SummaryMetrics.vue'

const metrics = ref<null | {
  gaugeMetrics: MetricsRecordGauge[]
  sumMetrics: MetricsRecordSum[]
  histogramMetrics: MetricsRecordHistogram[]
  exponentialHistogramMetrics: MetricsRecordExponentialHistogram[]
  summaryMetrics: MetricsRecordSummary[]
}>(null)
onMounted(async () => {
  const result = await Promise.all([
    getMetricsGauge(),
    getMetricsSum(),
    getMetricsHistogram(),
    getMetricsExponentialHistogram(),
    getMetricsSummary(),
  ])
  metrics.value = {
    gaugeMetrics: result[0],
    sumMetrics: result[1],
    histogramMetrics: result[2],
    exponentialHistogramMetrics: result[3],
    summaryMetrics: result[4],
  }
})

useHead({
  title: 'About | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'Sweetcorn app dashboard',
    },
  ],
})
</script>

<template>
  <BaseLayout>
    <div>
      <h1 class="mb-4 font-semibold">Metrics</h1>

      <div>Len gaugeMetrics: {{ metrics?.gaugeMetrics.length }}</div>
      <div>Len sumMetrics: {{ metrics?.sumMetrics.length }}</div>
      <div>Len histogramMetrics: {{ metrics?.histogramMetrics.length }}</div>
      <div>Len exponentialHistogramMetrics: {{ metrics?.exponentialHistogramMetrics.length }}</div>
      <div>Len summaryMetrics: {{ metrics?.summaryMetrics.length }}</div>

      <div v-if="metrics" class="grid grid-cols-1 gap-4">
        <GaugeMetrics :data="metrics.gaugeMetrics" />
        <SumMetrics :data="metrics.sumMetrics" />
        <HistogramMetrics :data="metrics.histogramMetrics" />
        <ExponentialHistogramMetrics :data="metrics.exponentialHistogramMetrics" />
        <SummaryMetrics :data="metrics.summaryMetrics" />
      </div>
    </div>
  </BaseLayout>
</template>
