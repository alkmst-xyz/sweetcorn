<script setup lang="ts">
import type { ColumnDef } from '@tanstack/vue-table'
import { FlexRender, tableFeatures, useTable } from '@tanstack/vue-table'
import { useTanStackTableDevtools } from '@tanstack/vue-table-devtools'
import { h } from 'vue'
import { type LogRecord } from '@/components/api'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/table'

const props = defineProps<{ data: LogRecord[] }>()

const features = tableFeatures({})

const columns: Array<ColumnDef<typeof features, LogRecord>> = [
  {
    accessorKey: 'timestamp',
    header: () => h('span', 'Timestamp'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'timestampObserved',
    header: () => h('span', 'Timestamp Observed'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'severityNumber',
    header: () => h('span', 'Severity Number'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'severityText',
    header: () => h('span', 'Severity Text'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'body',
    header: () => h('span', 'Body'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'logAttributes',
    header: () => h('span', 'Log Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'logDroppedAttributesCount',
    header: () => h('span', 'Log Dropped Attributes Count'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'flags',
    header: () => h('span', 'Trace Flags'),
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
    accessorKey: 'eventName',
    header: () => h('span', 'Event Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'resourceAttributes',
    header: () => h('span', 'Resource Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'resourceDroppedAttributesCount',
    header: () => h('span', 'Resource Dropped Attributes Count'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'resourceServiceName',
    header: () => h('span', 'Service Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'resourceSchemaUrl',
    header: () => h('span', 'Resource Schema URL'),
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
    accessorKey: 'scopeAttributes',
    header: () => h('span', 'Scope Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'scopeDroppedAttributesCount',
    header: () => h('span', 'Scope Dropped Attributes Count'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'scopeSchemaUrl',
    header: () => h('span', 'Scope Schema URL'),
    cell: (info) => h('i', info.getValue<string>()),
  },
]

const table = useTable({
  key: 'logs-table',
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
