<script lang="ts">
	import type { LogRecord } from '$lib/api';
	import type { PageProps } from './$types';
	import type { TableOptions } from '@tanstack/svelte-table';
	import {
		createSvelteTable,
		flexRender,
		getCoreRowModel,
		type ColumnDef
	} from '@tanstack/svelte-table';
	import { writable } from 'svelte/store';

	// logs data
	let { data }: PageProps = $props();

	// // New in V9! Tell the table which features and row models we want to use.
	// // In this case, this will be a basic table with no additional features.
	// const _features = tableFeatures({});

	// Define the columns for your table. This uses the new `ColumnDef` type to
	// define columns. Alternatively, check out the
	// createTableHelper/createColumnHelper util for an even more type-safe way
	// to define columns.
	//
	// V9:
	// const defaultColumns: ColumnDef<typeof _features, Person>[] = [
	//
	// V8:
	const defaultColumns: ColumnDef<LogRecord>[] = [
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
			accessorKey: 'traceFlags',
			header: () => 'Trace Flags'
		},
		{
			accessorKey: 'severityText',
			header: () => 'Severity Text'
		},
		{
			accessorKey: 'severityNumber',
			header: () => 'Severity Number'
		},
		{
			accessorKey: 'serviceName',
			header: () => 'Service Name'
		},
		{
			accessorKey: 'body',
			header: () => 'Body'
		},
		{
			accessorKey: 'resourceSchemaUrl',
			header: () => 'Resource Schema URL'
		},
		{
			accessorKey: 'resourceAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Resource Attributes'
		},
		{
			accessorKey: 'scopeSchemaUrl',
			header: () => 'Scope Schema URL'
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
			accessorKey: 'scopeAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Scope Attributes'
		},
		{
			accessorKey: 'logAttributes',
			cell: (info) => JSON.stringify(info.getValue()),
			header: () => 'Log Attributes'
		}
	];

	// V8:
	const options = writable<TableOptions<LogRecord>>({
		data: data.logs,
		columns: defaultColumns,
		getCoreRowModel: getCoreRowModel()
	});

	// Create the table instance with required _features, columns, and data
	//
	// V9:
	// const table = createTable({
	// 	// 5a. New required option in V9. Tell the table which features you are
	// 	//     importing and using (better tree-shaking).
	// 	// _features,
	// 	// 5b. `Core` row model is now included by default, but you can still
	// 	//     override it here.
	// 	_rowModels: {},
	// 	defaultColumns,
	//     defaultData
	// 	// ...add additional table options here
	// });
	//
	// V8:
	const table = createSvelteTable(options);
</script>

<svelte:head>
	<title>Logs | Sweetcorn</title>
	<meta name="description" content="Sweetcorn app dashboard" />
</svelte:head>

<section>
	<h1 class="mb-4 font-semibold">Logs</h1>

	<div class="mb-4 text-sm">
		Showing results: {data.logs.length}
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
