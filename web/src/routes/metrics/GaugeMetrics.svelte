<script lang="ts">
	import type { MetricsRecordGauge } from '$lib/api/types';
	import {
		Datatable,
		TableHandler,
		ThFilter,
		ThSort
	} from '@vincjo/datatables';

	let { data }: { data: MetricsRecordGauge[] } = $props();

	const table = new TableHandler(data, { rowsPerPage: 10 });
</script>

<div class="border">
	<h2>Gauge Metrics</h2>

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
						<ThSort {table} field="value">Value</ThSort>
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
						<ThFilter {table} field="value" />
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
							<td>{row.value}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Datatable>
	</div>
</div>
