<script setup lang="ts">
import github from '../../assets/github.svg'
import type { StatusResponse } from '../api'
import StatusIndicator from './StatusIndicator.vue'

defineProps<{ statusData: StatusResponse }>()
</script>

<template>
  <footer class="flex-col space-y-2 border-t px-4 pt-4 pb-6">
    <StatusIndicator :data="statusData" />

    <div class="flex w-full justify-between text-sm">
      <p>© 2025 Sweetcorn. Open source software.</p>

      <a href="https://github.com/alkmst-xyz/sweetcorn" class="flex items-center gap-1">
        <div class="h-4 w-4">
          <img :src="github" alt="GitHub" />
        </div>
        GitHub
      </a>
    </div>
  </footer>
</template>
