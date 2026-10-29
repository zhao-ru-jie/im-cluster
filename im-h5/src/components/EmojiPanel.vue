<script setup lang="ts">
import { EMOJI_LIST } from '@/core/emoji';

const emit = defineEmits<{ (e: 'pick', emoji: string): void }>();
</script>

<template>
  <div class="emoji-panel">
    <button v-for="(emoji, index) in EMOJI_LIST" :key="`${emoji}-${index}`" class="emoji-item" @click="emit('pick', emoji)">
      {{ emoji }}
    </button>
  </div>
</template>
