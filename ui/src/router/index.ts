import { createRouter, createWebHashHistory } from 'vue-router'
import HomePage from '../pages/index.vue'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    {
      path: '/',
      name: 'home',
      component: HomePage,
    },
    {
      path: '/about',
      name: 'about',
      component: () => import('../pages/about/index.vue'),
    },
    {
      path: '/logs',
      name: 'logs',
      component: () => import('../pages/logs/index.vue'),
    },
    {
      path: '/traces',
      name: 'traces',
      component: () => import('../pages/traces/index.vue'),
    },
    {
      path: '/metrics',
      name: 'metrics',
      component: () => import('../pages/metrics/index.vue'),
    },
  ],
})

export default router
