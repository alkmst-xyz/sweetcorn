<script setup lang="ts">
import type { TanStackDevtoolsVuePlugin } from '@tanstack/vue-devtools'
import { TanStackDevtools } from '@tanstack/vue-devtools'
import { VueQueryDevtoolsPanel } from '@tanstack/vue-query-devtools'
import { TableDevtoolsPanel } from '@tanstack/vue-table-devtools'
import { RouterView } from 'vue-router'

const plugins: TanStackDevtoolsVuePlugin[] = [
  { name: 'Vue Query', component: VueQueryDevtoolsPanel },
  { name: 'Vue Table', component: TableDevtoolsPanel },
]
</script>

<template>
  <RouterView />
  <TanStackDevtools :plugins="plugins" />
</template>
