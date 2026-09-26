<script setup lang="ts">
import QueryInput from '@/components/code/QueryInput.vue'
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'
import { ref } from 'vue'

useHead({
  title: 'App | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'Sweetcorn app dashboard.',
    },
  ],
})

const query = ref<string>('')
function handleRun(value: string) {
  query.value = value
}
</script>

<template>
  <BaseLayout>
    <section>
      <h1 class="mb-4 font-semibold">Overview</h1>

      <QueryInput @run="(v) => handleRun(v)" />

      <div>query:{{ query }}</div>
    </section>
  </BaseLayout>
</template>
