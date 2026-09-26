<script setup lang="ts">
import { getLogs } from '@/components/api'
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'
import LogsTable from './LogsTable.vue'
import { useQuery } from '@tanstack/vue-query'

useHead({
  title: 'Logs | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'Sweetcorn app dashboard',
    },
  ],
})

const { isPending, data, error } = useQuery({
  queryKey: ['logs'],
  queryFn: getLogs,
})
</script>

<template>
  <BaseLayout>
    <div>
      <h1 class="mb-4 font-semibold">Logs</h1>

      <div v-if="isPending">
        <span>Loading...</span>
      </div>

      <div v-if="error">
        <span>Error: {{ error.message }}</span>
      </div>

      <div v-if="data">
        <div>Number of logs: {{ data.length }}</div>
        <LogsTable :data="data" />
      </div>
    </div>
  </BaseLayout>
</template>
