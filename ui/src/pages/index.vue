<script setup lang="ts">
import BaseLayout from '@/layouts/BaseLayout.vue'

import { useHead } from '@unhead/vue'

useHead({
  title: 'App | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'Sweetcorn app dashboard.',
    },
  ],
})
</script>

<template>
  <BaseLayout>
    <section>
      <h1 class="mb-4 font-semibold">Overview</h1>

      <p>WIP</p>
    </section>
  </BaseLayout>
</template>
