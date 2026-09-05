import './assets/main.css'

import { TanStackDevtools } from '@tanstack/vue-devtools'
import { tableDevtoolsPlugin } from '@tanstack/vue-table-devtools'
import { createHead } from '@unhead/vue/client'
import { createPinia } from 'pinia'
import { createApp, defineComponent, h } from 'vue'

import App from './App.vue'
import router from './router'

const Root = defineComponent({
  setup() {
    return () => [
      h(App),
      h(TanStackDevtools, {
        plugins: [tableDevtoolsPlugin({})],
      }),
    ]
  },
})

const app = createApp(Root)
const head = createHead()

app.use(createPinia())
app.use(router)
app.use(head)

app.mount('#app')
