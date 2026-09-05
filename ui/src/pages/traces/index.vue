<script setup lang="ts">
import { getTraces, type TraceRecord } from '@/components/api'
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'
import { onMounted, ref } from 'vue'
import TracesTable from './TracesTable.vue'

const traces = ref<TraceRecord[]>([])
onMounted(async () => {
  traces.value = await getTraces()
})

useHead({
  title: 'Traces | Sweetcorn',
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
      <h1 class="mb-4 font-semibold">Traces</h1>
      <div>Number of traces: {{ traces?.length }}</div>

      <TracesTable :data="traces" />
    </div>
  </BaseLayout>
</template>
