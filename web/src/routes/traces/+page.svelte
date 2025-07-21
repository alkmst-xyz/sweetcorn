<script lang="ts">
	import type { TraceRecord } from '$lib/api';
	import type { PageProps } from './$types';
	import type { TableOptions } from '@tanstack/svelte-table';
	import {
		createSvelteTable,
		flexRender,
		getCoreRowModel,
		type ColumnDef
	} from '@tanstack/svelte-table';
	import { writable } from 'svelte/store';

	let { data }: PageProps = $props();

	const defaultColumns: ColumnDef<TraceRecord>[] = [
		{
			accessorKey: 'timestamp',
			header: () => 'Timestamp'
		},
		{
			accessorKey: 'traceId',
			header: () => 'Trace Id'
		},
		{
			accessorKey: 'spanId',
			header: () => 'Span Id'
		},
		{
			accessorKey: 'parentSpanId',
			header: () => 'Parent Span ID'
		},
		{
			accessorKey: 'traceState',
			header: () => 'Trace State'
		},
		{
			accessorKey: 'spanName',
			header: () => 'Span Name'
		},
		{
			accessorKey: 'spanKind',
			header: () => 'Span Kind'
		},
		{
			accessorKey: 'serviceName',
			header: () => 'Service Name'
		},
		{
			accessorKey: 'resourceAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Resource Attributes'
		},
		{
			accessorKey: 'scopeName',
			header: () => 'Scope Name'
		},
		{
			accessorKey: 'scopeVersion',
			header: () => 'Scope Version'
		},
		{
			accessorKey: 'spanAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Span Attributes'
		},
		{
			accessorKey: 'duration',
			header: () => 'Duration'
		},
		{
			accessorKey: 'statusCode',
			header: () => 'Status Code'
		},
		{
			accessorKey: 'statusMessage',
			header: () => 'Status Message'
		},
		{
			accessorKey: 'eventsTimestamps',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Events Timestamps'
		},
		{
			accessorKey: 'eventsNames',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Events Names'
		},
		{
			accessorKey: 'eventsAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Events Attributes'
		},
		{
			accessorKey: 'linksTraceIds',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Links Trace IDs'
		},
		{
			accessorKey: 'linksSpanIds',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Links Span IDs'
		},
		{
			accessorKey: 'linksTraceStates',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Links Trace States'
		},
		{
			accessorKey: 'linksAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Links Attributes'
		}
	];

	const options = writable<TableOptions<TraceRecord>>({
		data: data.traces,
		columns: defaultColumns,
		getCoreRowModel: getCoreRowModel()
	});

	const table = createSvelteTable(options);
</script>

<svelte:head>
	<title>Traces | Sweetcorn</title>
	<meta name="description" content="Sweetcorn app dashboard" />
</svelte:head>

<section>
	<h1 class="mb-4 font-semibold">Traces</h1>

	<div class="mb-4 text-sm">
		Showing results: {data.traces.length}
	</div>

	<div class="mb-4 overflow-auto border">
		<table>
			<thead class="border-b bg-violet-400">
				{#each $table.getHeaderGroups() as headerGroup}
					<tr>
						{#each headerGroup.headers as header}
							<th class="px-4">
								{#if !header.isPlaceholder}
									<svelte:component
										this={flexRender(
											header.column.columnDef.header,
											header.getContext()
										)}
									/>
								{/if}
							</th>
						{/each}
					</tr>
				{/each}
			</thead>

			<tbody class="text-sm">
				{#each $table.getRowModel().rows as row}
					<tr class=" bg-violet-300 even:bg-violet-50">
						{#each row.getVisibleCells() as cell}
							<td class="px-4">
								<svelte:component
									this={flexRender(
										cell.column.columnDef.cell,
										cell.getContext()
									)}
								/>
							</td>
						{/each}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
</section>
