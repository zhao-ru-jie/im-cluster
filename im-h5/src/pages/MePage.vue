<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { useRouter } from 'vue-router';
import Avatar from '@/components/Avatar.vue';
import NavBar from '@/components/NavBar.vue';
import TabBar from '@/components/TabBar.vue';
import { clearAll } from '@/core/db';
import { useAuthStore } from '@/stores/auth';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import { useUiStore } from '@/stores/ui';

const router = useRouter();
const auth = useAuthStore();
const im = useImStore();
const contact = useContactStore();
const ui = useUiStore();

const nickname = ref(auth.profile?.nickname ?? auth.userId);
const saving = ref(false);

const devices = computed(() => contact.devices);
const socketText = computed(() => {
  switch (im.socketState) {
    case 'open':
      return '已连接';
    case 'connecting':
      return '连接中…';
    case 'closed':
      return im.retryInMs ? `已断开，${Math.max(1, Math.round(im.retryInMs / 1000))} 秒后重试` : '已断开';
    case 'kicked':
      return '已被下线';
    default:
      return '未连接';
  }
});

async function saveProfile(): Promise<void> {
  const name = nickname.value.trim();
  if (!name) {
    ui.error('昵称不能为空');
    return;
  }
  saving.value = true;
  try {
    await auth.saveProfile({ nickname: name });
    await contact.saveLocalProfile({
      userId: auth.userId,
      nickname: name,
      avatar: auth.profile?.avatar,
      updatedAt: Date.now()
    });
    ui.success('已保存');
  } catch (e) {
    ui.error(e instanceof Error ? e.message : '保存失败');
  } finally {
    saving.value = false;
  }
}

async function clearCache(): Promise<void> {
  if (!window.confirm('清空本地缓存的会话和消息？登录状态会保留。')) return;
  await clearAll();
  ui.success('缓存已清空');
}

function logout(): void {
  if (!window.confirm('退出登录？')) return;
  im.disconnect();
  auth.logout();
  void router.replace('/login');
}

onMounted(() => {
  void contact.loadDevices();
});
</script>

<template>
  <div class="page">
    <NavBar title="我的" :back="false" />

    <div class="page-body page-with-tab">
      <div class="profile">
        <Avatar :user-id="auth.userId" :name="auth.profile?.nickname || auth.userId" :src="auth.profile?.avatar" :size="60" />
        <div class="profile-main">
          <div class="profile-name">{{ auth.profile?.nickname || auth.userId }}</div>
          <div class="small muted">{{ auth.userId }} · {{ auth.deviceId }} · {{ socketText }}</div>
        </div>
      </div>

      <div class="field section">
        <div class="row">
          <label>昵称</label>
          <input v-model="nickname" placeholder="昵称" autocomplete="off" @keyup.enter="saveProfile" />
          <button class="btn small" :disabled="saving" @click="saveProfile">保存</button>
        </div>
        <div class="row">
          <label>接口</label>
          <span class="mono small muted cell-title-text">{{ auth.endpoints.restBase || '同源' }}</span>
        </div>
        <div class="row">
          <label>WebSocket</label>
          <span class="mono small muted cell-title-text">{{ auth.endpoints.wsUrl }}</span>
        </div>
      </div>

      <div class="list-group-title">在线设备 {{ devices.length }}</div>
      <div v-if="!devices.length" class="empty">没有在线设备</div>
      <div v-else class="list">
        <div v-for="device in devices" :key="device.deviceId" class="cell">
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ device.deviceId }}</span>
              <span v-if="device.deviceId === auth.deviceId" class="tag green">本机</span>
            </div>
            <div class="cell-sub">{{ device.transport }} · {{ device.gateway || '未知网关' }}</div>
          </div>
          <button class="btn small ghost" @click="contact.kickDevice(device.deviceId)">
            {{ device.online ? '踢下线' : '移除' }}
          </button>
        </div>
      </div>

      <div class="section">
        <div class="list">
          <button class="cell" @click="contact.kickOthers()">
            <div class="cell-main">
              <div class="cell-title"><span class="cell-title-text">退出其它设备</span></div>
              <div class="cell-sub">保留本机 {{ auth.deviceId }}</div>
            </div>
            <span class="cell-arrow" />
          </button>
          <button class="cell" @click="clearCache">
            <div class="cell-main">
              <div class="cell-title"><span class="cell-title-text">清空本地缓存</span></div>
              <div class="cell-sub">会话与消息的 IndexedDB 副本</div>
            </div>
            <span class="cell-arrow" />
          </button>
        </div>
      </div>

      <div class="logout">
        <button class="btn danger block" @click="logout">退出登录</button>
      </div>
    </div>

    <TabBar />
  </div>
</template>

<style scoped>
.profile {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 20px 16px;
  background: var(--c-card);
}

.profile-main {
  min-width: 0;
}

.profile-name {
  font-size: 18px;
  font-weight: 600;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.logout {
  padding: 24px 16px calc(24px + var(--safe-bottom));
}
</style>
