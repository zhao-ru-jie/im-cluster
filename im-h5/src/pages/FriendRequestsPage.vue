<script setup lang="ts">
import { computed, onMounted } from 'vue';
import Avatar from '@/components/Avatar.vue';
import NavBar from '@/components/NavBar.vue';
import { formatListTime } from '@/core/format';
import { useContactStore } from '@/stores/contact';
import { useUiStore } from '@/stores/ui';
import type { FriendRequestItem } from '@/types/protocol';

const contact = useContactStore();
const ui = useUiStore();

/** 待处理的排前面：这是唯一需要用户动手的一组 */
const incoming = computed(() =>
  [...contact.incoming].sort((a, b) => {
    if (a.status !== b.status) return a.status === 'PENDING' ? -1 : 1;
    return b.updatedAt - a.updatedAt;
  })
);
const outgoing = computed(() => contact.outgoing);

function statusText(item: FriendRequestItem, incomingSide: boolean): string {
  if (item.status === 'PENDING') return incomingSide ? '请求添加你为好友' : '等待对方验证';
  if (item.status === 'ACCEPTED') return '已添加';
  return incomingSide ? '已拒绝' : '对方已拒绝';
}

async function accept(fromId: string): Promise<void> {
  await contact.acceptRequest(fromId);
}

async function reject(fromId: string): Promise<void> {
  if (!window.confirm('拒绝后对方会收到提示，确定拒绝？')) return;
  await contact.rejectRequest(fromId);
}

async function cancel(toId: string): Promise<void> {
  await contact.cancelRequest(toId);
}

function messageOf(item: FriendRequestItem): string {
  return item.message ? `附言：${item.message}` : '';
}

onMounted(() => {
  void contact.loadRequests();
  // 名字/头像来自本地资料缓存，申请列表里的 userId 得靠它渲染
  void contact.loadProfiles();
  if (!contact.incoming.length && !contact.outgoing.length) {
    ui.toast('暂无好友申请');
  }
});
</script>

<template>
  <div class="page">
    <NavBar title="新的朋友" />

    <div class="page-body">
      <div class="list-group-title">收到的申请 {{ contact.pendingIncomingCount }}</div>

      <div v-if="!incoming.length" class="empty">还没有人申请加你为好友</div>
      <div v-else class="list">
        <div v-for="item in incoming" :key="`in-${item.fromId}`" class="cell">
          <Avatar
            :user-id="item.fromId"
            :name="contact.displayName(item.fromId)"
            :src="contact.avatarOf(item.fromId)"
          />
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ contact.displayName(item.fromId) }}</span>
              <span v-if="item.status === 'ACCEPTED'" class="tag green">已添加</span>
            </div>
            <div class="cell-sub">{{ statusText(item, true) }}</div>
            <div v-if="messageOf(item)" class="request-message">{{ messageOf(item) }}</div>
            <div class="request-time">{{ formatListTime(item.updatedAt) }}</div>
          </div>
          <div v-if="item.status === 'PENDING'" class="request-actions">
            <button class="btn small" @click="accept(item.fromId)">同意</button>
            <button class="btn small ghost" @click="reject(item.fromId)">拒绝</button>
          </div>
        </div>
      </div>

      <div class="list-group-title">我发出的申请 {{ outgoing.length }}</div>

      <div v-if="!outgoing.length" class="empty">还没有发出过申请</div>
      <div v-else class="list">
        <div v-for="item in outgoing" :key="`out-${item.toId}`" class="cell">
          <Avatar
            :user-id="item.toId"
            :name="contact.displayName(item.toId)"
            :src="contact.avatarOf(item.toId)"
          />
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ contact.displayName(item.toId) }}</span>
              <span v-if="item.status === 'ACCEPTED'" class="tag green">已通过</span>
              <span v-else-if="item.status === 'REJECTED'" class="tag orange">被拒绝</span>
            </div>
            <div class="cell-sub">{{ statusText(item, false) }}</div>
            <div v-if="messageOf(item)" class="request-message">{{ messageOf(item) }}</div>
          </div>
          <button
            v-if="item.status === 'PENDING'"
            class="btn small ghost"
            @click="cancel(item.toId)"
          >
            撤回
          </button>
        </div>
      </div>

      <div class="hint">
        只有对方同意之后，双方才会出现在彼此的好友列表里；对方同意/拒绝时你会立刻收到通知。
      </div>
    </div>
  </div>
</template>

<style scoped>
.request-actions {
  display: flex;
  gap: 8px;
  flex: none;
}

.request-message {
  margin-top: 4px;
  font-size: 13px;
  color: var(--c-text);
  word-break: break-word;
}

.request-time {
  margin-top: 2px;
  font-size: 11px;
  color: var(--c-text-3);
}
</style>
