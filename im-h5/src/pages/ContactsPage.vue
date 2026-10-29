<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { useRouter } from 'vue-router';
import Avatar from '@/components/Avatar.vue';
import NavBar from '@/components/NavBar.vue';
import TabBar from '@/components/TabBar.vue';
import { singleConversationId } from '@/core/format';
import { useAuthStore } from '@/stores/auth';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import { useUiStore } from '@/stores/ui';

const router = useRouter();
const auth = useAuthStore();
const im = useImStore();
const contact = useContactStore();
const ui = useUiStore();

const friendInput = ref('');
const requestMessage = ref('');
const groupInput = ref('');
const showAdd = ref(false);
const showCreate = ref(false);

const friends = computed(() => contact.friends);
const groups = computed(() => Object.values(contact.groups));

function openSingle(userId: string): void {
  const id = singleConversationId(auth.userId, userId);
  im.ensureConversation(id, 'SINGLE');
  void router.push({ name: 'chat', params: { conversationId: id }, query: { type: 'SINGLE' } });
}

function openGroup(groupId: string): void {
  void contact.loadGroup(groupId, false);
  void router.push({ name: 'chat', params: { conversationId: `g:${groupId}` }, query: { type: 'GROUP' } });
}

/** 发送好友申请（不是直接加好友：对方同意之后才会进好友列表） */
async function sendRequest(): Promise<void> {
  if (await contact.sendFriendRequest(friendInput.value, requestMessage.value)) {
    friendInput.value = '';
    requestMessage.value = '';
    showAdd.value = false;
  }
}

async function createGroup(): Promise<void> {
  const name = groupInput.value.trim();
  if (!name) {
    ui.error('请输入群名');
    return;
  }
  const group = await contact.createGroup(name, contact.friends);
  if (group) {
    groupInput.value = '';
    showCreate.value = false;
    openGroup(group.groupId);
  }
}

onMounted(async () => {
  void contact.loadProfiles();
  // 待处理申请数决定「新的朋友」入口上的红点，冷启动时不能等长连接推送
  void contact.loadRequests();
  await contact.loadFriends();
  void contact.loadBlocks();
  // 好友列表回来了才知道要查谁的在线状态
  contact.requestPresence(contact.friends);
});
</script>

<template>
  <div class="page">
    <NavBar title="通讯录" :back="false">
      <template #right>
        <button class="nav-side-btn" @click="showAdd = !showAdd">加好友</button>
      </template>
    </NavBar>

    <div class="page-body page-with-tab">
      <div class="list">
        <div class="cell" @click="router.push('/requests')">
          <div class="new-friend-icon">👋</div>
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">新的朋友</span>
            </div>
            <div class="cell-sub">好友申请 / 等待验证</div>
          </div>
          <div class="cell-extra">
            <span v-if="contact.pendingIncomingCount > 0" class="badge">
              {{ contact.pendingIncomingCount > 99 ? '99+' : contact.pendingIncomingCount }}
            </span>
            <span class="cell-arrow" />
          </div>
        </div>
      </div>

      <div v-if="showAdd" class="field section">
        <div class="row">
          <input
            v-model="friendInput"
            placeholder="对方 userId"
            autocomplete="off"
            @keyup.enter="sendRequest"
          />
        </div>
        <div class="row">
          <input
            v-model="requestMessage"
            placeholder="验证消息（选填，例如：我是产品的小李）"
            autocomplete="off"
            @keyup.enter="sendRequest"
          />
          <button class="btn small" @click="sendRequest">发送申请</button>
        </div>
        <div class="hint">对方同意后，双方才会出现在彼此的好友列表里。</div>
      </div>

      <div class="list-group-title">好友 {{ friends.length }}</div>
      <div v-if="!friends.length" class="empty">还没有好友</div>
      <div v-else class="list">
        <div v-for="userId in friends" :key="userId" class="cell" @click="openSingle(userId)">
          <Avatar
            :user-id="userId"
            :name="contact.displayName(userId)"
            :src="contact.avatarOf(userId)"
            :online="contact.isOnline(userId)"
          />
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ contact.displayName(userId) }}</span>
              <span v-if="contact.isOnline(userId)" class="tag green">在线</span>
            </div>
            <div class="cell-sub">{{ userId }}</div>
          </div>
        </div>
      </div>

      <div class="list-group-title">
        群聊 {{ groups.length }}
        <button class="link-btn" @click="showCreate = !showCreate">新建</button>
      </div>

      <div v-if="showCreate" class="field">
        <div class="row">
          <input v-model="groupInput" placeholder="群名称" autocomplete="off" @keyup.enter="createGroup" />
          <button class="btn small" @click="createGroup">创建</button>
        </div>
      </div>

      <div v-if="!groups.length" class="empty">还没有群聊</div>
      <div v-else class="list">
        <div v-for="group in groups" :key="group.groupId" class="cell" @click="openGroup(group.groupId)">
          <Avatar :user-id="group.groupId" :name="group.name" />
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ group.name }}</span>
              <span v-if="group.mutedAll" class="tag orange">全员禁言</span>
            </div>
            <div class="cell-sub">{{ group.announcement || group.groupId }}</div>
          </div>
        </div>
      </div>

      <div v-if="contact.blocks.length" class="list-group-title">黑名单 {{ contact.blocks.length }}</div>
      <div v-if="contact.blocks.length" class="list">
        <div v-for="userId in contact.blocks" :key="userId" class="cell">
          <Avatar :user-id="userId" :name="contact.displayName(userId)" />
          <div class="cell-main">
            <div class="cell-title">
              <span class="cell-title-text">{{ contact.displayName(userId) }}</span>
            </div>
          </div>
          <button class="btn small ghost" @click="contact.unblock(userId)">移出</button>
        </div>
      </div>
    </div>

    <TabBar />
  </div>
</template>

<style scoped>
.nav-side-btn,
.link-btn {
  font-size: 14px;
  color: var(--c-text);
}

.link-btn {
  margin-left: 8px;
  color: var(--c-text-2);
}

.new-friend-icon {
  width: 44px;
  height: 44px;
  flex: none;
  border-radius: 8px;
  background: #fff3e0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 22px;
}
</style>
