<script setup lang="ts">
export interface SheetAction {
  key: string;
  label: string;
  danger?: boolean;
}

defineProps<{ visible: boolean; actions: SheetAction[]; title?: string }>();
const emit = defineEmits<{ (e: 'select', key: string): void; (e: 'close'): void }>();
</script>

<template>
  <div v-if="visible" class="sheet-mask" @click.self="emit('close')">
    <div class="sheet">
      <div v-if="title" class="hint text-center" style="background: #fff; margin: 8px; border-radius: 12px; padding: 14px">
        {{ title }}
      </div>
      <div class="sheet-actions">
        <button
          v-for="action in actions"
          :key="action.key"
          class="sheet-action"
          :class="{ danger: action.danger }"
          @click="emit('select', action.key)"
        >
          {{ action.label }}
        </button>
      </div>
      <button class="sheet-cancel" @click="emit('close')">取消</button>
    </div>
  </div>
</template>
