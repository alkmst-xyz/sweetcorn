<script setup lang="ts">
import { postQuery, type QuerySqlResponse } from '@/components/api'
import { QueryInput } from '@/components/code'
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'
import { ref } from 'vue'
import QuerySqlTable from './QuerySqlTable.vue'

useHead({
  title: 'App | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'Sweetcorn app dashboard.',
    },
  ],
})

// TODO: Add error handling.
const queryResponse = ref<QuerySqlResponse | null>(null)
async function handleRun(query: string) {
  queryResponse.value = await postQuery(query)
}
</script>

<template>
  <BaseLayout>
    <section>
      <h1 class="mb-4 font-semibold">Overview</h1>

      <QueryInput @run="(val) => handleRun(val)" />
      <QuerySqlTable v-if="queryResponse" :data="queryResponse" />
    </section>
  </BaseLayout>
</template>
