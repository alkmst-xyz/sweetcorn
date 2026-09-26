<script setup lang="ts">
import { type QuerySqlResponse } from '@/components/api'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/table'

defineProps<{ data: QuerySqlResponse }>()

/**
 * Formats values for display.
 * - Booleans: show 'true'/'false' instead of blank
 * - Nulls: show '-' or 'NULL'
 * - Objects/Arrays: stringify if necessary
 */
const formatValue = (val: string | boolean | number) => {
  if (val === null || val === undefined) return '-'
  if (typeof val === 'boolean') return val ? 'Yes' : 'No'
  if (typeof val === 'object') return JSON.stringify(val)
  return val
}
</script>

<template>
  <div v-if="data">
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead v-for="column in data.columns" :key="column">
            {{ column }}
          </TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <TableRow v-for="(row, rowIndex) in data.results" :key="rowIndex">
          <TableCell v-for="(cell, cellIndex) in row" :key="cellIndex">
            {{ formatValue(cell) }}
          </TableCell>
        </TableRow>
      </TableBody>
    </Table>
  </div>

  <div v-else>No results found</div>
</template>
