<script setup lang="ts">
import { useRoute } from 'vue-router'

/**
 * Navigation menu item.
 *
 * @property path - URL path.
 * @property title - Display name.
 * @property exact - Whether to match the `path` exactly. If `false`, the
 * item will match any URL that starts with `path`. Defaults to `true`.
 */
export type NavItemType = {
  path: string
  title: string
  exact?: boolean
}

defineProps<{ items: NavItemType[] }>()

const route = useRoute()

// TODO(joseph): Refactor
function getAriaCurrent(item: NavItemType): 'page' | undefined {
  if (item.exact === false) {
    if (route.fullPath.startsWith(item.path)) {
      return 'page'
    } else {
      return undefined
    }
  }

  if (route.fullPath == item.path) {
    return 'page'
  }

  return undefined
}
</script>

<template>
  <nav class="flex w-full">
    <ul class="flex space-x-2">
      <template v-for="(item, idx) in items" :key="idx">
        <li
          :aria-current="getAriaCurrent(item)"
          class="decoration-2 underline-offset-4 aria-[current=page]:font-semibold aria-[current=page]:underline"
        >
          <RouterLink :to="item.path">
            {{ item.title }}
          </RouterLink>
        </li>
      </template>
    </ul>
  </nav>
</template>
