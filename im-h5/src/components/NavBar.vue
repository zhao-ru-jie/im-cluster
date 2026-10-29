<script setup lang="ts">
import { useRouter } from 'vue-router';

withDefaults(
  defineProps<{
    title: string;
    subtitle?: string;
    back?: boolean;
    solid?: boolean;
  }>(),
  { subtitle: '', back: true, solid: false }
);

const emit = defineEmits<{ (e: 'back'): void }>();
const router = useRouter();

function onBack(): void {
  emit('back');
  if (window.history.length > 1) router.back();
  else void router.replace('/conversations');
}
</script>

<template>
  <header class="nav" :class="{ solid }">
    <div class="nav-side">
      <button v-if="back" class="nav-back" @click="onBack">返回</button>
      <slot name="left" />
    </div>
    <div class="nav-title">
      {{ title }}
      <span v-if="subtitle" class="nav-subtitle">{{ subtitle }}</span>
    </div>
    <div class="nav-side right">
      <slot name="right" />
    </div>
  </header>
</template>
