<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import ActionSheet from '@/components/ActionSheet.vue';
import ChatComposer from '@/components/ChatComposer.vue';
import MessageBubble from '@/components/MessageBubble.vue';
import NavBar from '@/components/NavBar.vue';
import { formatChatTime, groupIdOfConversation, peerOfConversation, stringifyAttrs } from '@/core/format';
import type { MediaPayload } from '@/core/media';
import { useAuthStore } from '@/stores/auth';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import { useUiStore } from '@/stores/ui';
import type { ChatMessage } from '@/types/local';
import type { ConvType } from '@/types/protocol';

const route = useRoute();
const router = useRouter();
const auth = useAuthStore();
const im = useImStore();
const contact = useContactStore();
const ui = useUiStore();

const bodyRef = ref<HTMLElement | null>(null);
const preview = ref('');
const sheetOpen = ref(false);
const groupOpen = ref(false);
const selected = ref<ChatMessage | null>(null);

const conversationId = computed(() => String(route.params.conversationId ?? ''));
const convType = computed<ConvType>(() => {
  const type = route.query.type;
  if (type === 'GROUP' || type === 'SINGLE') return type;
  return conversationId.value.startsWith('g:') ? 'GROUP' : 'SINGLE';
});
/** 发消息用的「对方」：单聊是对方 userId，群聊是 groupId */
const peer = computed(() =>
  convType.value === 'GROUP'
    ? groupIdOfConversation(conversationId.value)
    : peerOfConversation(conversationId.value, auth.userId)
);
const messages = computed(() => im.messagesOf(conversationId.value));
const group = computed(() => contact.groupOf(peer.value));

const title = computed(() => {
  if (convType.value === 'GROUP') return contact.groupNameOf(conversationId.value);
  return contact.displayName(peer.value) || peer.value || '会话';
});

const subtitle = computed(() => {
  if (convType.value === 'GROUP') {
    const members = contact.membersOf(peer.value);
    const size = members?.size || members?.members.length || 0;
    return size ? `${size} 人` : '';
  }
  return contact.isOnline(peer.value) ? '在线' : '离线';
});

const typingText = computed(() => {
  const entry = im.typing[conversationId.value];
  if (!entry) return '';
  return convType.value === 'GROUP' ? `${contact.displayName(entry.userId)} 正在输入…` : '对方正在输入…';
});

/**
 * 导航栏标题：有人正在输入时整条替换成人名/状态文本，位置就是顶部正中（微信的做法）。
 *
 * <p>不用「在消息列表末尾加一行」：那行会在自己不滚动时看不见，而滚动后又会把最后一条
 * 消息顶上去，等于用一条布局位移换一个状态提示。放标题位置既不占用消息区，也不打断阅读。
 */
const titleText = computed(() => typingText.value || title.value);

const sheetActions = computed(() => {
  const message = selected.value;
  if (!message) return [];
  const list: Array<{ key: string; label: string; danger?: boolean }> = [];
  if (message.contentType === 'TEXT' && message.body) list.push({ key: 'copy', label: '复制' });
  if (message.from === auth.userId && message.seq > 0 && !message.recalled) {
    list.push({ key: 'recall', label: '撤回' });
  }
  list.push({ key: 'delete', label: '删除', danger: true });
  return list;
});

/* ------------------------------- 滚动 ------------------------------- */

function scrollToBottom(): void {
  void nextTick(() => {
    const el = bodyRef.value;
    if (el) el.scrollTop = el.scrollHeight;
  });
}

/** 只在原本就在底部时跟随新消息，否则用户翻历史会被拽回去 */
function nearBottom(): boolean {
  const el = bodyRef.value;
  if (!el) return true;
  return el.scrollHeight - el.scrollTop - el.clientHeight < 120;
}

async function onScroll(): Promise<void> {
  const el = bodyRef.value;
  if (!el || el.scrollTop > 40 || im.loadingHistory) return;
  const previousHeight = el.scrollHeight;
  const previousTop = el.scrollTop;
  await im.loadMore(conversationId.value);
  await nextTick();
  // 往前插了一页：把视口钉在原来的那条消息上，不然会「跳一下」
  el.scrollTop = el.scrollHeight - previousHeight + previousTop;
}

watch(
  () => messages.value.length,
  () => {
    if (nearBottom()) scrollToBottom();
  }
);

/* ------------------------------- 消息操作 ------------------------------- */

function openSheet(message: ChatMessage): void {
  selected.value = message;
  sheetOpen.value = true;
}

async function onRetry(message: ChatMessage): Promise<void> {
  await im.resend(message);
  scrollToBottom();
}

async function onSheetSelect(key: string): Promise<void> {
  const message = selected.value;
  sheetOpen.value = false;
  if (!message) return;
  if (key === 'copy') {
    if (!navigator.clipboard) {
      ui.error('当前浏览器不支持复制');
      return;
    }
    try {
      await navigator.clipboard.writeText(message.body);
      ui.success('已复制');
    } catch {
      ui.error('复制失败');
    }
    return;
  }
  if (key === 'recall') {
    await im.recall(message);
    return;
  }
  if (key === 'delete') await im.removeMessage(message);
}

function showTime(index: number): boolean {
  const list = messages.value;
  if (index === 0) return true;
  return list[index].ts - list[index - 1].ts > 5 * 60 * 1000;
}

/* ------------------------------- 发送 ------------------------------- */

function onSendText(text: string): void {
  void im.sendText(peer.value, convType.value, text);
  scrollToBottom();
}

function onSendMedia(payload: MediaPayload): void {
  void im.sendMedia(peer.value, convType.value, {
    contentType: payload.contentType,
    body: payload.body,
    attrs: stringifyAttrs(payload.attrs)
  });
  scrollToBottom();
}

function onTyping(typing: boolean): void {
  im.setTyping(conversationId.value, convType.value, typing);
}

function onComposerError(message: string): void {
  ui.error(message);
}

/* ------------------------------- 生命周期 ------------------------------- */

function init(): void {
  if (!conversationId.value) return;
  im.activeConversation = conversationId.value;
  im.ensureConversation(conversationId.value, convType.value);
  void im.openConversation(conversationId.value, convType.value).then(scrollToBottom);
  if (convType.value === 'GROUP') void contact.loadGroup(peer.value);
  else void contact.loadPresence([peer.value]);
}

function onVisibility(): void {
  if (document.visibilityState === 'visible') im.markRead(conversationId.value);
}

async function leaveGroup(): Promise<void> {
  groupOpen.value = false;
  if (!window.confirm('退出后不再接收该群消息，确定退出？')) return;
  await contact.leaveGroup(peer.value);
  void router.replace('/conversations');
}

onMounted(() => {
  init();
  document.addEventListener('visibilitychange', onVisibility);
});

onBeforeUnmount(() => {
  document.removeEventListener('visibilitychange', onVisibility);
  im.setTyping(conversationId.value, convType.value, false);
  im.activeConversation = '';
});

watch(conversationId, () => {
  preview.value = '';
  selected.value = null;
  init();
});
</script>

<template>
  <div class="page">
    <NavBar :title="titleText" :subtitle="subtitle" :title-muted="!!typingText" solid>
      <template #right>
        <button v-if="convType === 'GROUP'" class="nav-side-btn" @click="groupOpen = true">详情</button>
      </template>
    </NavBar>

    <div ref="bodyRef" class="chat-body" @scroll.passive="onScroll">
      <div v-if="im.hasMore[conversationId] || im.loadingHistory" class="chat-tip">
        {{ im.loadingHistory ? '加载中…' : '上滑加载更早的消息' }}
      </div>

      <div v-if="!messages.length" class="empty">
        {{ im.socketState === 'open' ? '还没有消息，打个招呼吧' : '连接中，稍后可以发送消息' }}
      </div>

      <template v-for="(message, index) in messages" :key="message.key">
        <div v-if="showTime(index)" class="chat-tip">{{ formatChatTime(message.ts) }}</div>
        <MessageBubble
          :message="message"
          :sender-name="contact.displayName(message.from) || message.from"
          :self="message.from === auth.userId"
          :is-group="convType === 'GROUP'"
          :avatar="contact.avatarOf(message.from)"
          @longpress="openSheet"
          @preview-image="preview = $event"
          @retry="onRetry"
        />
      </template>
    </div>

    <ChatComposer
      :disabled="im.socketState === 'kicked'"
      @send-text="onSendText"
      @send-media="onSendMedia"
      @typing="onTyping"
      @error="onComposerError"
    />

    <ActionSheet :visible="sheetOpen" :actions="sheetActions" @select="onSheetSelect" @close="sheetOpen = false" />

    <div v-if="groupOpen" class="sheet-mask" @click.self="groupOpen = false">
      <div class="sheet">
        <div class="group-card">
          <div class="cell-title">{{ title }}</div>
          <div class="small muted group-line">{{ group?.announcement || '暂无群公告' }}</div>
          <div class="small muted group-line">{{ subtitle }}</div>
        </div>
        <div class="sheet-actions">
          <button class="sheet-action danger" @click="leaveGroup">退出群聊</button>
        </div>
        <button class="sheet-cancel" @click="groupOpen = false">取消</button>
      </div>
    </div>

    <div v-if="preview" class="mask" @click="preview = ''">
      <img :src="preview" alt="图片预览" />
      <button class="mask-close" @click.stop="preview = ''">×</button>
    </div>
  </div>
</template>

<style scoped>
.nav-side-btn {
  font-size: 14px;
  color: var(--c-text);
}

.group-card {
  margin: 8px;
  padding: 14px;
  border-radius: 12px;
  background: #fff;
  color: var(--c-text);
}

.group-line {
  margin-top: 6px;
  line-height: 1.5;
}
</style>
