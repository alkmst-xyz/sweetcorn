<script setup lang="ts">
import type { StatusResponse } from '../api'

defineProps<{ data: StatusResponse }>()
</script>

<template>
  <div class="flex w-fit items-center space-x-2 px-2 py-1 text-sm font-semibold">
    <template v-if="data.status === 'OK'">
      <div class="h-2 w-2 rounded-full bg-blue-500"></div>
      <p class="text-blue-500">Server connected</p>
    </template>
    <template v-else>
      <div class="h-2 w-2 rounded-full bg-red-500"></div>
      <p class="text-red-500">Connection Lost</p>
    </template>
  </div>
</template>
