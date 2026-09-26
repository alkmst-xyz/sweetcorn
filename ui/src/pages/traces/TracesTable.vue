<script setup lang="ts">
import type { ColumnDef } from '@tanstack/vue-table'
import { FlexRender, tableFeatures, useTable } from '@tanstack/vue-table'
import { useTanStackTableDevtools } from '@tanstack/vue-table-devtools'
import { h } from 'vue'
import { type TraceRecord } from '@/components/api'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/table'

const props = defineProps<{ data: TraceRecord[] }>()

const features = tableFeatures({})

const columns: Array<ColumnDef<typeof features, TraceRecord>> = [
  {
    accessorKey: 'timestamp',
    header: () => h('span', 'Timestamp'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'traceId',
    header: () => h('span', 'Trace ID'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'spanId',
    header: () => h('span', 'Span ID'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'parentSpanID',
    header: () => h('span', 'Parent Span ID'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'traceState',
    header: () => h('span', 'Trace State'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'spanName',
    header: () => h('span', 'Span Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'spanKind',
    header: () => h('span', 'Span Kind'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'serviceName',
    header: () => h('span', 'Service Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'resourceAttributes',
    header: () => h('span', 'Resource Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'scopeName',
    header: () => h('span', 'Scope Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'scopeVersion',
    header: () => h('span', 'Scope Version'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'scopeName',
    header: () => h('span', 'Scope Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'spanAttributes',
    header: () => h('span', 'Span Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'duration',
    header: () => h('span', 'Duration'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'statusCode',
    header: () => h('span', 'Status Code'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'statusMessage',
    header: () => h('span', 'Status Message'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'eventsTimestamps',
    header: () => h('span', 'Events Timestamps'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'eventsAttributes',
    header: () => h('span', 'Events Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'linksTraceIDs',
    header: () => h('span', 'Links TraceIDs'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'linksSpanIDs',
    header: () => h('span', 'Links SpanIDs'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'linksTraceStates',
    header: () => h('span', 'Links Trace States'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'linksAttributes',
    header: () => h('span', 'Links Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
]

const table = useTable({
  key: 'traces-table',
  features,
  columns,
  get data() {
    return props.data
  },
})

useTanStackTableDevtools(table)
</script>

<template>
  <Table>
    <TableHeader>
      <TableRow v-for="headerGroup in table.getHeaderGroups()" :key="headerGroup.id">
        <TableHead v-for="header in headerGroup.headers" :key="header.id">
          <FlexRender v-if="!header.isPlaceholder" :header="header" />
        </TableHead>
      </TableRow>
    </TableHeader>
    <TableBody>
      <TableRow v-for="row in table.getRowModel().rows" :key="row.id">
        <TableCell v-for="cell in row.getAllCells()" :key="cell.id">
          <FlexRender :cell="cell" />
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
</template>
