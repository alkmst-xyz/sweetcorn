<script setup lang="ts">
import { getHealthz, type StatusResponse } from '@/components/api'
import Footer from '@/components/ui/Footer.vue'
import Header from '@/components/ui/Header.vue'
import { onMounted, ref } from 'vue'

const healthStatus = ref<StatusResponse | null>(null)
onMounted(async () => {
  healthStatus.value = await getHealthz()
})
</script>

<template>
  <div class="mx-auto flex min-h-screen flex-col bg-violet-200">
    <Header />

    <main class="flex flex-1 flex-col px-4 py-1">
      <slot />
    </main>

    <Footer v-if="healthStatus" :status-data="healthStatus" />
  </div>
</template>
