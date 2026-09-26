<script setup lang="ts">
import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  completionKeymap,
} from '@codemirror/autocomplete'
import { defaultKeymap, history, historyKeymap, insertTab } from '@codemirror/commands'
import { sql } from '@codemirror/lang-sql'
import {
  bracketMatching,
  foldGutter,
  foldKeymap,
  indentOnInput,
  indentUnit,
} from '@codemirror/language'
import { EditorState } from '@codemirror/state'
import { oneDark } from '@codemirror/theme-one-dark'
import {
  drawSelection,
  dropCursor,
  EditorView,
  highlightActiveLine,
  highlightActiveLineGutter,
  highlightSpecialChars,
  keymap,
  lineNumbers,
  placeholder,
  ViewUpdate,
} from '@codemirror/view'
import { onBeforeUnmount, onMounted, useTemplateRef } from 'vue'
import { SWEETCORN_TABLE_CONFIG } from './constants'
import type { TableConfig } from './types'

const emit = defineEmits<{
  (e: 'run', query: string): void
}>()

const editorRef = useTemplateRef('editorRef')
let view: EditorView | null = null

const QUERY_PLACEHOLDER = 'select * from otel_logs;'
let queryLocal = ''

function updateQueryLocal(state: EditorState) {
  queryLocal = state.doc.toString()
}

function handleRun() {
  emit('run', queryLocal)
}

// Extension that plugs into the editor state and updates query locally.
const updateListenerExtension = EditorView.updateListener.of((update: ViewUpdate) => {
  if (update.docChanged) {
    updateQueryLocal(update.state)
  }
})

function getSqlSchema(config: TableConfig): Record<string, string[]> {
  const result: Record<string, string[]> = {}
  Object.entries(config).map(([, v]) => {
    result[v.name] = v.columns
  })
  return result
}

onMounted(() => {
  if (!editorRef.value) {
    return
  }

  const state = EditorState.create({
    doc: '',
    extensions: [
      // Based on 'basicSetup' (see https://codemirror.net/examples/basic/).
      lineNumbers(),
      foldGutter(),
      highlightSpecialChars(),
      history(),
      drawSelection(),
      dropCursor(),
      indentOnInput(),
      bracketMatching(),
      closeBrackets(),
      autocompletion(),
      highlightActiveLine(),
      highlightActiveLineGutter(),
      keymap.of([
        ...closeBracketsKeymap,
        ...defaultKeymap,
        ...historyKeymap,
        ...foldKeymap,
        ...completionKeymap,
        {
          key: 'Tab',
          preventDefault: true,
          run: insertTab,
        },
      ]),
      // Additional extensions.
      indentUnit.of('    '),
      placeholder(QUERY_PLACEHOLDER),
      sql({
        schema: getSqlSchema(SWEETCORN_TABLE_CONFIG),
      }),
      oneDark,
      updateListenerExtension,
    ],
  })

  updateQueryLocal(state)

  view = new EditorView({
    state,
    parent: editorRef.value,
  })
})

onBeforeUnmount(() => {
  if (!view) {
    return
  }
  view.destroy()
})
</script>

<template>
  <div class="bg-green-50 p-2">
    <div class="flex items-center justify-between pb-2">
      <span class="text-xs font-semibold uppercase">Query</span>

      <div class="flex gap-2">
        <button @click="handleRun">
          <span class="bg-blue-100 px-2 py-1 text-xs font-bold uppercase">Run</span>
        </button>
      </div>
    </div>

    <div class="border-2">
      <div ref="editorRef"></div>
    </div>
  </div>
</template>
