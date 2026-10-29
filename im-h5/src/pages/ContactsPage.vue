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

async function addFriend(): Promise<void> {
  if (await contact.addFriend(friendInput.value)) {
    friendInput.value = '';
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
      <div v-if="showAdd" class="field">
        <div class="row">
          <input v-model="friendInput" placeholder="对方 userId" autocomplete="off" @keyup.enter="addFriend" />
          <button class="btn small" @click="addFriend">添加</button>
        </div>
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
</style>
