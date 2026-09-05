<script setup lang="ts">
import BaseLayout from '@/layouts/BaseLayout.vue'
import { useHead } from '@unhead/vue'

useHead({
  title: 'About | Sweetcorn',
  meta: [
    {
      name: 'description',
      content: 'About sweetcorn.',
    },
  ],
})
</script>

<template>
  <BaseLayout>
    <div>
      <h1 class="mb-4 font-semibold">About Sweetcorn</h1>

      <p>
        The Sweetcorn application acts a glue layer between OpenTelemetry and DuckDB. Clients can
        send data telemetry data via OTEL and sweetcorn with write it DuckDB with some
        transformations.
      </p>
    </div>
  </BaseLayout>
</template>
