<script lang="ts">
	import ExponentialHistogramMetrics from './ExponentialHistogramMetrics.svelte';
	import GaugeMetrics from './GaugeMetrics.svelte';
	import HistogramMetrics from './HistogramMetrics.svelte';
	import SummaryMetrics from './SummaryMetrics.svelte';
	import SumMetrics from './SumMetrics.svelte';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();
</script>

<svelte:head>
	<title>Metrics | Sweetcorn</title>
	<meta name="description" content="Sweetcorn app dashboard" />
</svelte:head>

<section>
	<h1 class="mb-4 font-semibold">Metrics</h1>

	<div class="grid grid-cols-1 gap-4">
		<GaugeMetrics data={data.gaugeMetrics} />
		<SumMetrics data={data.sumMetrics} />
		<HistogramMetrics data={data.histogramMetrics} />
		<ExponentialHistogramMetrics data={data.exponentialHistogramMetrics} />
		<SummaryMetrics data={data.summaryMetrics} />
	</div>

	<div></div>
</section>
