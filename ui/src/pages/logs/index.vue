<script setup lang="ts">
import { getLogs, type LogRecord } from '@/components/api'
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'
import { onMounted, ref } from 'vue'
import LogsTable from './LogsTable.vue'

const logs = ref<LogRecord[]>([])
onMounted(async () => {
  logs.value = await getLogs()
})

useHead({
  title: 'Logs | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'Sweetcorn app dashboard',
    },
  ],
})
</script>

<template>
  <BaseLayout>
    <div>
      <h1 class="mb-4 font-semibold">Logs</h1>

      <div>Number of logs: {{ logs?.length }}</div>

      <LogsTable :data="logs" />
    </div>
  </BaseLayout>
</template>
