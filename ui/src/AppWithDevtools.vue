<script setup lang="ts">
import { TanStackDevtools, type TanStackDevtoolsVuePlugin } from '@tanstack/vue-devtools'
import { VueQueryDevtoolsPanel } from '@tanstack/vue-query-devtools'
import { TableDevtoolsPanel } from '@tanstack/vue-table-devtools'

import App from './App.vue'

const plugins: TanStackDevtoolsVuePlugin[] = [
  {
    name: 'Vue Query',
    component: VueQueryDevtoolsPanel,
  },
  {
    name: 'Vue Table',
    component: TableDevtoolsPanel,
  },
]
</script>

<template>
  <App />
  <TanStackDevtools :plugins="plugins" />
</template>
