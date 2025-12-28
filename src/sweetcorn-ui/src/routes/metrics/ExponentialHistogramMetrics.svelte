<script lang="ts">
	import type { MetricsRecordExponentialHistogram } from '$lib/api/types';
	import {
		Datatable,
		TableHandler,
		ThFilter,
		ThSort
	} from '@vincjo/datatables';

	let { data }: { data: MetricsRecordExponentialHistogram[] } = $props();

	const table = new TableHandler(data, { rowsPerPage: 10 });
</script>

<div class="border">
	<h2>Exponential Histogram Metrics</h2>

	<div>
		<Datatable basic {table}>
			<table>
				<thead>
					<tr>
						<ThSort {table} field="timestamp">Timestamp</ThSort>
						<ThSort {table} field="serviceName">Service Name</ThSort>
						<ThSort {table} field="metricName">Metric Name</ThSort>
						<ThSort {table} field="metricDescription">Metric Description</ThSort
						>
						<ThSort {table} field="metricUnit">Metric Unit</ThSort>
						<ThSort {table} field="resourceAttributes">
							Resource Attributes
						</ThSort>
						<ThSort {table} field="scopeName">Scope Name</ThSort>
						<ThSort {table} field="scopeVersion">Scope Version</ThSort>
						<ThSort {table} field="attributes">Attributes</ThSort>
						<ThSort {table} field="count">Count</ThSort>
						<ThSort {table} field="sum">Sum</ThSort>
						<ThSort {table} field="scale">Scale</ThSort>
						<ThSort {table} field="zeroCount">Zero Count</ThSort>
						<ThSort {table} field="positiveOffset">Positive Offset</ThSort>
						<ThSort {table} field="positiveBucketCounts">
							Positive Bucket Counts
						</ThSort>
						<ThSort {table} field="negativeOffset">Negative Offset</ThSort>
						<ThSort {table} field="negativeBucketCounts">
							Negative Bucket Counts
						</ThSort>
						<ThSort {table} field="min">Min</ThSort>
						<ThSort {table} field="max">Max</ThSort>
					</tr>
					<tr>
						<ThFilter {table} field="timestamp" />
						<ThFilter {table} field="serviceName" />
						<ThFilter {table} field="metricName" />
						<ThFilter {table} field="metricDescription" />
						<ThFilter {table} field="metricUnit" />
						<ThFilter {table} field="resourceAttributes" />
						<ThFilter {table} field="scopeName" />
						<ThFilter {table} field="scopeVersion" />
						<ThFilter {table} field="attributes" />
						<ThFilter {table} field="count" />
						<ThFilter {table} field="sum" />
						<ThFilter {table} field="scale" />
						<ThFilter {table} field="zeroCount" />
						<ThFilter {table} field="positiveOffset" />
						<ThFilter {table} field="positiveBucketCounts" />
						<ThFilter {table} field="negativeOffset" />
						<ThFilter {table} field="negativeBucketCounts" />
						<ThFilter {table} field="min" />
						<ThFilter {table} field="max" />
					</tr>
				</thead>
				<tbody>
					{#each table.rows as row}
						<tr>
							<td>{row.timestamp}</td>
							<td>{row.serviceName}</td>
							<td>{row.metricName}</td>
							<td>{row.metricDescription}</td>
							<td>{row.metricUnit}</td>
							<td>
								{JSON.stringify(row.resourceAttributes)}
							</td>
							<td>{row.scopeName}</td>
							<td>{row.scopeVersion}</td>
							<td>{JSON.stringify(row.attributes)}</td>
							<td>{row.count}</td>
							<td>{row.sum}</td>
							<td>{row.scale}</td>
							<td>{row.zeroCount}</td>
							<td>{row.positiveOffset}</td>
							<td>{row.positiveBucketCounts}</td>
							<td>{row.negativeOffset}</td>
							<td>{row.negativeBucketCounts}</td>
							<td>{row.min}</td>
							<td>{row.max}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Datatable>
	</div>
</div>
