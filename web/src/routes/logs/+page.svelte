<script lang="ts">
	import {
		Datatable,
		TableHandler,
		ThFilter,
		ThSort
	} from '@vincjo/datatables';
	import type { PageProps } from './$types';

	let { data }: PageProps = $props();

	const table = new TableHandler(data.logs, { rowsPerPage: 10 });
</script>

<svelte:head>
	<title>Logs | Sweetcorn</title>
	<meta name="description" content="Sweetcorn app dashboard" />
</svelte:head>

<section>
	<h1 class="mb-4 font-semibold">Logs</h1>

	<div class="mb-4 overflow-auto border">
		<Datatable basic {table}>
			<table>
				<thead>
					<tr>
						<ThSort {table} field="timestamp">Timestamp</ThSort>
						<ThSort {table} field="traceId">Trace ID</ThSort>
						<ThSort {table} field="spanId">Span ID</ThSort>
						<ThSort {table} field="traceFlags">Trace Flags</ThSort>
						<ThSort {table} field="severityText">Severity Text</ThSort>
						<ThSort {table} field="severityNumber">Severity Number</ThSort>
						<ThSort {table} field="serviceName">Service Name</ThSort>
						<ThSort {table} field="body">Body</ThSort>
						<ThSort {table} field="resourceSchemaUrl">
							Resource Schema URL
						</ThSort>
						<ThSort {table} field="resourceAttributes">
							Resource Attributes
						</ThSort>
						<ThSort {table} field="scopeSchemaUrl">Scope Schema URL</ThSort>
						<ThSort {table} field="scopeName">Scope Name</ThSort>
						<ThSort {table} field="scopeVersion">Scope Version</ThSort>
						<ThSort {table} field="scopeAttributes">Scope Attributes</ThSort>
						<ThSort {table} field="logAttributes">Log Attributes</ThSort>
					</tr>
					<tr>
						<ThFilter {table} field="timestamp" />
						<ThFilter {table} field="traceId" />
						<ThFilter {table} field="spanId" />
						<ThFilter {table} field="traceFlags" />
						<ThFilter {table} field="severityText" />
						<ThFilter {table} field="severityNumber" />
						<ThFilter {table} field="serviceName" />
						<ThFilter {table} field="body" />
						<ThFilter {table} field="resourceSchemaUrl" />
						<ThFilter {table} field="resourceAttributes" />
						<ThFilter {table} field="scopeSchemaUrl" />
						<ThFilter {table} field="scopeName" />
						<ThFilter {table} field="scopeVersion" />
						<ThFilter {table} field="scopeAttributes" />
						<ThFilter {table} field="logAttributes" />
					</tr>
				</thead>
				<tbody>
					{#each table.rows as row}
						<tr>
							<td>{row.timestamp}</td>
							<td>{row.traceId}</td>
							<td>{row.spanId}</td>
							<td>{row.traceFlags}</td>
							<td>{row.severityText}</td>
							<td>{row.severityNumber}</td>
							<td>{row.serviceName}</td>
							<td>{row.body}</td>
							<td>{row.resourceSchemaUrl}</td>
							<td>
								{JSON.stringify(row.resourceAttributes)}
							</td>
							<td>{row.scopeSchemaUrl}</td>
							<td>{row.scopeName}</td>
							<td>{row.scopeVersion}</td>
							<td>{JSON.stringify(row.scopeAttributes)}</td>
							<td>{JSON.stringify(row.logAttributes)}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Datatable>
	</div>
</section>
