<script setup lang="ts">
import { computed } from 'vue';
import { avatarColor, avatarText } from '@/core/format';

const props = withDefaults(
  defineProps<{
    userId: string;
    name?: string;
    src?: string;
    size?: number;
    /** undefined = 不显示在线点 */
    online?: boolean;
  }>(),
  { name: '', src: '', size: 44, online: undefined }
);

const label = computed(() => avatarText(props.name || props.userId));
const bg = computed(() => avatarColor(props.userId || props.name || '?'));
const radius = computed(() => (props.size >= 40 ? '8px' : '6px'));
</script>

<template>
  <span class="avatar-wrap" :style="{ width: `${size}px`, height: `${size}px` }">
    <span
      class="avatar"
      :style="{
        width: `${size}px`,
        height: `${size}px`,
        background: src ? '#f2f2f2' : bg,
        borderRadius: radius,
        fontSize: `${Math.round(size * 0.38)}px`
      }"
    >
      <img v-if="src" :src="src" :alt="name || userId" />
      <template v-else>{{ label }}</template>
    </span>
    <span v-if="online !== undefined" class="presence-dot" :class="{ on: online }" />
  </span>
</template>
