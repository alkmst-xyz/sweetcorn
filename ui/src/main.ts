import './assets/main.css'

import { VueQueryPlugin } from '@tanstack/vue-query'
import { createHead } from '@unhead/vue/client'
import { createPinia } from 'pinia'
import { createApp } from 'vue'

import router from './router'
import App from './App.vue'
import AppWithDevtools from './AppWithDevtools.vue'

const head = createHead()
const app = getApp()

app.use(VueQueryPlugin)
app.use(createPinia())
app.use(router)
app.use(head)
app.mount('#app')

function getApp() {
  if (import.meta.env.DEV) {
    return createApp(AppWithDevtools)
  }
  return createApp(App)
}
