<script setup lang="ts">
import { reactive, ref } from 'vue';
import { useRouter } from 'vue-router';
import { defaultEndpoints, useAuthStore } from '@/stores/auth';
import { useUiStore } from '@/stores/ui';

const router = useRouter();
const auth = useAuthStore();
const ui = useUiStore();

const form = reactive({
  userId: auth.userId,
  deviceId: auth.deviceId || 'web',
  restBase: auth.endpoints.restBase,
  wsUrl: auth.endpoints.wsUrl
});
const submitting = ref(false);
const advanced = ref(false);

async function submit(): Promise<void> {
  const userId = form.userId.trim();
  if (!userId) {
    ui.error('请输入 userId');
    return;
  }
  if (!form.wsUrl.trim()) {
    ui.error('WebSocket 地址不能为空');
    return;
  }
  submitting.value = true;
  try {
    await auth.login({
      userId,
      deviceId: form.deviceId.trim() || 'web',
      endpoints: { restBase: form.restBase.trim(), wsUrl: form.wsUrl.trim() }
    });
    await router.replace('/conversations');
  } catch (e) {
    ui.error(e instanceof Error ? e.message : '登录失败');
  } finally {
    submitting.value = false;
  }
}

function resetEndpoints(): void {
  const preset = defaultEndpoints();
  form.restBase = preset.restBase;
  form.wsUrl = preset.wsUrl;
  ui.toast('已恢复默认地址');
}
</script>

<template>
  <div class="page">
    <div class="login-hero">
      <div class="login-logo">IM</div>
      <div class="login-title">im-cluster</div>
      <div class="login-sub">输入任意 userId 即可进入（服务端无鉴权）</div>
    </div>

    <div class="field">
      <div class="row">
        <label>userId</label>
        <input v-model="form.userId" placeholder="例如 alice" autocomplete="off" @keyup.enter="submit" />
      </div>
      <div class="row">
        <label>设备 ID</label>
        <input v-model="form.deviceId" placeholder="web" autocomplete="off" />
      </div>
    </div>

    <div class="hint">
      多开两个浏览器、用不同 userId + 不同设备 ID 登录，就能互相发消息；同一设备 ID 登录会把上一个顶下线。
    </div>

    <div class="field section">
      <button class="cell" @click="advanced = !advanced">
        <div class="cell-main">
          <div class="cell-title">接口地址</div>
          <div class="cell-sub">{{ form.restBase || '同源' }} · {{ form.wsUrl }}</div>
        </div>
        <span class="cell-arrow" />
      </button>
    </div>

    <div v-if="advanced" class="field">
      <div class="row">
        <label>REST</label>
        <input v-model="form.restBase" placeholder="留空 = 与页面同源" autocomplete="off" />
      </div>
      <div class="row">
        <label>WebSocket</label>
        <input v-model="form.wsUrl" placeholder="ws://host:8081/ws" autocomplete="off" />
      </div>
    </div>

    <div class="login-actions">
      <button class="btn block" :disabled="submitting" @click="submit">
        {{ submitting ? '进入中…' : '进入' }}
      </button>
      <button v-if="advanced" class="btn ghost block section" @click="resetEndpoints">恢复默认地址</button>
    </div>
  </div>
</template>

<style scoped>
.login-hero {
  padding: 56px 24px 28px;
  text-align: center;
}

.login-logo {
  width: 64px;
  height: 64px;
  margin: 0 auto 14px;
  border-radius: 16px;
  background: var(--c-primary);
  color: #fff;
  font-size: 26px;
  font-weight: 700;
  line-height: 64px;
}

.login-title {
  font-size: 20px;
  font-weight: 600;
}

.login-sub {
  margin-top: 8px;
  font-size: 13px;
  color: var(--c-text-3);
}

.login-actions {
  padding: 20px 16px calc(20px + var(--safe-bottom));
}
</style>
