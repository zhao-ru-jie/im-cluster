<script setup lang="ts">
import { computed, onMounted, onBeforeUnmount, ref } from 'vue';
import { useRouter } from 'vue-router';
import ActionSheet from '@/components/ActionSheet.vue';
import Avatar from '@/components/Avatar.vue';
import NavBar from '@/components/NavBar.vue';
import TabBar from '@/components/TabBar.vue';
import { formatListTime, groupIdOfConversation, peerOfConversation } from '@/core/format';
import { useAuthStore } from '@/stores/auth';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import type { ConversationItem } from '@/types/protocol';

const router = useRouter();
const auth = useAuthStore();
const im = useImStore();
const contact = useContactStore();

const sheetOpen = ref(false);
const current = ref<ConversationItem | null>(null);
/** 长按弹层后手指抬起仍会触发 click，用它挡掉一次误进会话 */
let suppressClick = false;

const actions = computed(() => {
  const item = current.value;
  if (!item) return [];
  return [
    { key: 'pin', label: item.pinned ? '取消置顶' : '置顶聊天' },
    { key: 'mute', label: item.muted ? '开启消息提醒' : '消息免打扰' }
  ];
});

function peerIdOf(item: ConversationItem): string {
  return item.type === 'GROUP'
    ? groupIdOfConversation(item.conversationId)
    : peerOfConversation(item.conversationId, auth.userId);
}

function nameOf(item: ConversationItem): string {
  const id = peerIdOf(item);
  return item.type === 'GROUP' ? contact.groupNameOf(item.conversationId) : contact.displayName(id) || id;
}

function srcOf(item: ConversationItem): string | undefined {
  return item.type === 'GROUP' ? undefined : contact.avatarOf(peerIdOf(item));
}

function openChat(item: ConversationItem): void {
  if (suppressClick) {
    suppressClick = false;
    return;
  }
  void router.push({
    name: 'chat',
    params: { conversationId: item.conversationId },
    query: { type: item.type }
  });
}

let pressTimer = 0;

function startPress(item: ConversationItem): void {
  cancelPress();
  pressTimer = window.setTimeout(() => {
    pressTimer = 0;
    suppressClick = true;
    current.value = item;
    sheetOpen.value = true;
  }, 480);
}

function cancelPress(): void {
  if (pressTimer) {
    clearTimeout(pressTimer);
    pressTimer = 0;
  }
}

async function onSelect(key: string): Promise<void> {
  const item = current.value;
  sheetOpen.value = false;
  if (!item) return;
  if (key === 'pin') await im.setFlag(item.conversationId, { pinned: !item.pinned });
  else if (key === 'mute') await im.setFlag(item.conversationId, { muted: !item.muted });
}

onMounted(() => {
  void im.refreshConversations(true);
  void contact.loadProfiles();
  void contact.refreshAllGroups();
});

onBeforeUnmount(cancelPress);
</script>

<template>
  <div class="page">
    <NavBar title="消息" :back="false">
      <template #right>
        <button class="nav-side-btn" @click="router.push('/contacts')">通讯录</button>
      </template>
    </NavBar>

    <div class="page-body page-with-tab">
      <div v-if="!im.sortedConversations.length" class="empty">
        还没有会话<br />
        去「通讯录」加个好友开始聊天
      </div>

      <div v-else class="list">
        <div
          v-for="item in im.sortedConversations"
          :key="item.conversationId"
          class="cell conv-cell"
          @click="openChat(item)"
          @touchstart.passive="startPress(item)"
          @touchend="cancelPress"
          @touchmove="cancelPress"
          @touchcancel="cancelPress"
          @contextmenu.prevent="current = item; sheetOpen = true"
        >
          <Avatar :user-id="peerIdOf(item)" :name="nameOf(item)" :src="srcOf(item)" />
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ nameOf(item) }}</span>
              <span v-if="item.pinned" class="tag">置顶</span>
            </div>
            <div class="cell-sub">{{ item.lastDigest || '暂无消息' }}</div>
          </div>
          <div class="cell-extra">
            <span>{{ item.lastTime ? formatListTime(item.lastTime) : '' }}</span>
            <span v-if="item.unread > 0" class="badge" :class="{ dot: item.muted }">
              {{ item.muted ? '' : item.unread > 99 ? '99+' : item.unread }}
            </span>
            <span v-else-if="item.muted" class="tag">免打扰</span>
          </div>
        </div>
      </div>
    </div>

    <TabBar />

    <ActionSheet :visible="sheetOpen" :actions="actions" @select="onSelect" @close="sheetOpen = false" />
  </div>
</template>

<style scoped>
.conv-cell {
  min-height: 64px;
}

.nav-side-btn {
  font-size: 14px;
  color: var(--c-text);
}
</style>
