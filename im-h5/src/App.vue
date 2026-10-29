<script setup lang="ts">
import { onBeforeUnmount, onMounted, watch } from 'vue';
import { useRouter } from 'vue-router';
import { unlockAudio } from '@/core/notify';
import { useAuthStore } from '@/stores/auth';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import { useUiStore } from '@/stores/ui';

const router = useRouter();
const auth = useAuthStore();
const im = useImStore();
const contact = useContactStore();
const ui = useUiStore();

/** 长连接只挂一次：bootstrap 里重复 onFrame 会让一条消息被处理多遍 */
let booted = false;

function boot(): void {
  if (booted || !auth.loggedIn) return;
  booted = true;
  contact.bindFrames();
  void contact.loadProfiles();
  void im.bootstrap();
}

function teardown(): void {
  booted = false;
  im.disconnect();
}

watch(
  () => auth.loggedIn,
  (loggedIn) => {
    if (loggedIn) boot();
    else teardown();
  },
  { immediate: true }
);

// 被踢 / 401：清干净本地登录态再回登录页，否则重连会一直被拒
watch(
  () => im.kickedReason,
  (reason) => {
    if (!reason) return;
    ui.error(`已离线：${reason}`);
    im.kickedReason = '';
    auth.logout();
    void router.replace('/login');
  }
);

/** iOS 的音频上下文必须先有用户手势解锁，否则第一条提示音是哑的 */
function onFirstGesture(): void {
  unlockAudio();
  window.removeEventListener('touchstart', onFirstGesture);
  window.removeEventListener('click', onFirstGesture);
}

onMounted(() => {
  window.addEventListener('touchstart', onFirstGesture, { passive: true });
  window.addEventListener('click', onFirstGesture);
});

onBeforeUnmount(() => {
  window.removeEventListener('touchstart', onFirstGesture);
  window.removeEventListener('click', onFirstGesture);
  teardown();
});
</script>

<template>
  <div class="app-shell">
    <div v-if="ui.banner" class="banner" :class="{ error: ui.bannerKind === 'error' }">{{ ui.banner }}</div>
    <router-view />
    <div class="toast-wrap">
      <div v-for="item in ui.toasts" :key="item.id" class="toast" :class="item.kind">{{ item.text }}</div>
    </div>
  </div>
</template>
