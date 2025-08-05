<script lang="ts">
	import {
		Datatable,
		TableHandler,
		ThFilter,
		ThSort
	} from '@vincjo/datatables';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const table = new TableHandler(data.traces, { rowsPerPage: 10 });
</script>

<svelte:head>
	<title>Traces | Sweetcorn</title>
	<meta name="description" content="Sweetcorn app dashboard" />
</svelte:head>

<section>
	<h1 class="mb-4 font-semibold">Traces</h1>

	<div class="mb-4 overflow-auto border">
		<Datatable basic {table}>
			<table>
				<thead>
					<tr>
						<ThSort {table} field="timestamp">Timestamp</ThSort>
						<ThSort {table} field="traceId">Trace ID</ThSort>
						<ThSort {table} field="spanId">Span ID</ThSort>
						<ThSort {table} field="parentSpanId">Parent Span ID</ThSort>
						<ThSort {table} field="traceState">Trace State</ThSort>
						<ThSort {table} field="spanName">Span Name</ThSort>
						<ThSort {table} field="spanKind">Span Kind</ThSort>
						<ThSort {table} field="serviceName">Service Name</ThSort>
						<ThSort {table} field="resourceAttributes"
							>Resource Attributes</ThSort
						>
						<ThSort {table} field="scopeName">Scope Name</ThSort>
						<ThSort {table} field="scopeVersion">Scope Version</ThSort>
						<ThSort {table} field="spanAttributes">Span Attributes</ThSort>
						<ThSort {table} field="duration">Duration</ThSort>
						<ThSort {table} field="statusCode">Status Code</ThSort>
						<ThSort {table} field="statusMessage">Status Message</ThSort>
						<ThSort {table} field="eventsTimestamps">Events Timestamps</ThSort>
						<ThSort {table} field="eventsNames">Events Names</ThSort>
						<ThSort {table} field="eventsAttributes">Events Attributes</ThSort>
						<ThSort {table} field="linksTraceIds">Links Trace IDs</ThSort>
						<ThSort {table} field="linksSpanIds">Links Span IDs</ThSort>
						<ThSort {table} field="linksTraceStates">Links Trace States</ThSort>
						<ThSort {table} field="linksAttributes">Links Attributes</ThSort>
					</tr>
					<tr>
						<ThFilter {table} field="timestamp" />
						<ThFilter {table} field="traceId" />
						<ThFilter {table} field="spanId" />
						<ThFilter {table} field="parentSpanId" />
						<ThFilter {table} field="traceState" />
						<ThFilter {table} field="spanName" />
						<ThFilter {table} field="spanKind" />
						<ThFilter {table} field="serviceName" />
						<ThFilter {table} field="resourceAttributes" />
						<ThFilter {table} field="scopeName" />
						<ThFilter {table} field="scopeVersion" />
						<ThFilter {table} field="spanAttributes" />
						<ThFilter {table} field="duration" />
						<ThFilter {table} field="statusCode" />
						<ThFilter {table} field="statusMessage" />
						<ThFilter {table} field="eventsTimestamps" />
						<ThFilter {table} field="eventsNames" />
						<ThFilter {table} field="eventsAttributes" />
						<ThFilter {table} field="linksTraceIds" />
						<ThFilter {table} field="linksSpanIds" />
						<ThFilter {table} field="linksTraceStates" />
						<ThFilter {table} field="linksAttributes" />
					</tr>
				</thead>
				<tbody>
					{#each table.rows as row}
						<tr>
							<td>{row.timestamp}</td>
							<td>{row.traceId}</td>
							<td>{row.spanId}</td>
							<td>{row.parentSpanId}</td>
							<td>{row.traceState}</td>
							<td>{row.spanName}</td>
							<td>{row.spanKind}</td>
							<td>{row.serviceName}</td>
							<td>
								{JSON.stringify(row.resourceAttributes)}
							</td>
							<td>{row.scopeName}</td>
							<td>{row.scopeVersion}</td>
							<td>
								{JSON.stringify(row.spanAttributes)}
							</td>
							<td>{row.duration}</td>
							<td>{row.statusCode}</td>
							<td>{row.statusMessage}</td>
							<td>
								{JSON.stringify(row.eventsTimestamps)}
							</td>
							<td>
								{JSON.stringify(row.eventsNames)}
							</td>
							<td>
								{JSON.stringify(row.eventsAttributes)}
							</td>
							<td>
								{JSON.stringify(row.linksTraceIds)}
							</td>
							<td>
								{JSON.stringify(row.linksSpanIds)}
							</td>
							<td>
								{JSON.stringify(row.linksTraceStates)}
							</td>
							<td>
								{JSON.stringify(row.linksAttributes)}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Datatable>
	</div>
</section>
