<script setup lang="ts">
import type { ColumnDef } from '@tanstack/vue-table'
import { FlexRender, tableFeatures, useTable } from '@tanstack/vue-table'
import { useTanStackTableDevtools } from '@tanstack/vue-table-devtools'
import { h } from 'vue'
import { type MetricsRecordGauge } from '@/components/api'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/table'

const props = defineProps<{ data: MetricsRecordGauge[] }>()

const features = tableFeatures({})

const columns: Array<ColumnDef<typeof features, MetricsRecordGauge>> = [
  {
    accessorKey: 'timestamp',
    header: () => h('span', 'Timestamp'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'serviceName',
    header: () => h('span', 'Service Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'metricName',
    header: () => h('span', 'Metric Name'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'metricDescription',
    header: () => h('span', 'Metric Description'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'metricUnit',
    header: () => h('span', 'Metric Unit'),
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
    accessorKey: 'attributes',
    header: () => h('span', 'Attributes'),
    cell: (info) => h('i', info.getValue<string>()),
  },
  {
    accessorKey: 'value',
    header: () => h('span', 'Value'),
    cell: (info) => h('i', info.getValue<string>()),
  },
]

const table = useTable({
  key: 'gauge-metrics-table',
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
