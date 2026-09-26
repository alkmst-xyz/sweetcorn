import './assets/main.css'

import { VueQueryPlugin } from '@tanstack/vue-query'
import { createHead } from '@unhead/vue/client'
import { createPinia } from 'pinia'
import { createApp } from 'vue'

import App from './App.vue'
import router from './router'

const app = createApp(App)
const head = createHead()

app.use(VueQueryPlugin)
app.use(createPinia())
app.use(router)
app.use(head)

app.mount('#app')
