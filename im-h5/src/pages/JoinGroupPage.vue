<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import NavBar from '@/components/NavBar.vue';
import { groupConversationId } from '@/core/format';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import { useUiStore } from '@/stores/ui';

/**
 * 用邀请码 / 邀请链接加入群聊。
 *
 * <p>服务端只为「码」建模（二维码就是这条链接的图形化），所以这里也只解析出 code：
 * 用户把整条 `im://group/join?code=xxx` 或 `https://.../join?code=xxx` 粘进来都能用。
 *
 * <p>三种结果分开处理：直接进群（跳群详情）、还需要审批（留在本页并说明）、失败（展示服务端文案）。
 */
const route = useRoute();
const router = useRouter();
const contact = useContactStore();
const im = useImStore();
const ui = useUiStore();

const input = ref('');
const busy = ref(false);
/** 提交过申请时把结果留在页面上，避免用户以为「点了没反应」 */
const pendingNotice = ref('');

const code = computed(() => extractCode(input.value));
const canSubmit = computed(() => !!code.value && !busy.value);

/** 从「纯码 / im:// 链接 / https 链接」里取出 code */
function extractCode(raw: string): string {
  const text = raw.trim();
  if (!text) return '';
  const match = /[?&]code=([^&\s]+)/i.exec(text);
  if (match) return decodeURIComponent(match[1]);
  // 没有 query 形态时按纯码处理（码本身是 [A-Za-z0-9]，不含 : / ? 等字符）
  return /^[A-Za-z0-9_-]+$/.test(text) ? text : '';
}

async function submit(): Promise<void> {
  if (!canSubmit.value) {
    ui.error('请填写有效的邀请码');
    return;
  }
  busy.value = true;
  pendingNotice.value = '';
  try {
    const result = await contact.joinByInvite(code.value);
    if (!result.ok) return;
    if (result.approvalPending) {
      pendingNotice.value = '已提交入群申请，管理员通过后就能进群。';
      return;
    }
    const conversationId = groupConversationId(result.groupId);
    im.ensureConversation(conversationId, 'GROUP');
    await router.replace({ name: 'chat', params: { conversationId }, query: { type: 'GROUP' } });
  } finally {
    busy.value = false;
  }
}

onMounted(() => {
  // 落地页/深链接会带 ?code=，直接填好省一次粘贴
  const fromQuery = route.query.code;
  if (typeof fromQuery === 'string' && fromQuery) input.value = fromQuery;
});
</script>

<template>
  <div class="page">
    <NavBar title="加入群聊" />

    <div class="page-body">
      <div class="field section">
        <div class="row">
          <input
            v-model="input"
            placeholder="邀请码或邀请链接"
            autocomplete="off"
            @keyup.enter="submit"
          />
          <button class="btn small" :disabled="!canSubmit" @click="submit">
            {{ busy ? '处理中…' : '加入' }}
          </button>
        </div>
        <div class="hint">
          支持粘贴完整链接（{im://group/join?code=xxxx} 或 https 落地页），会自动取出邀请码。
        </div>
        <div v-if="code" class="hint">识别到邀请码：<span class="mono">{{ code }}</span></div>
      </div>

      <div v-if="pendingNotice" class="field section">
        <div class="row">
          <span class="hint">{{ pendingNotice }}</span>
        </div>
        <div class="row">
          <button class="btn small ghost" @click="router.push('/conversations')">返回会话</button>
        </div>
      </div>

      <div class="hint section">
        邀请码可能限制了使用次数与有效期；需要审批的群在管理员通过前不会出现在会话列表里。
        二维码与邀请链接等价，扫出来拿到的同样是这里的 code。
      </div>
    </div>
  </div>
</template>

<style scoped>
.mono {
  font-family: var(--font-mono, monospace);
}
</style>
