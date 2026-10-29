<script setup lang="ts">
import { computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useImStore } from '@/stores/im';

const route = useRoute();
const router = useRouter();
const im = useImStore();

const tabs = [
  { path: '/conversations', icon: '💬', label: '消息' },
  { path: '/contacts', icon: '👥', label: '通讯录' },
  { path: '/me', icon: '👤', label: '我的' }
];

const active = computed(() => tabs.find((tab) => route.path.startsWith(tab.path))?.path ?? '');

function go(path: string): void {
  if (route.path !== path) void router.replace(path);
}
</script>

<template>
  <nav class="tabbar">
    <button
      v-for="tab in tabs"
      :key="tab.path"
      class="tabbar-item"
      :class="{ active: active === tab.path }"
      @click="go(tab.path)"
    >
      <span class="tabbar-icon">{{ tab.icon }}</span>
      <span>{{ tab.label }}</span>
      <span v-if="tab.path === '/conversations' && im.unreadTotal > 0" class="badge tabbar-badge">
        {{ im.unreadTotal > 99 ? '99+' : im.unreadTotal }}
      </span>
    </button>
  </nav>
</template>
