<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import Avatar from '@/components/Avatar.vue';
import NavBar from '@/components/NavBar.vue';
import { formatListTime, groupConversationId } from '@/core/format';
import { useAuthStore } from '@/stores/auth';
import { useContactStore } from '@/stores/contact';
import { useImStore } from '@/stores/im';
import { useUiStore } from '@/stores/ui';
import type { GroupInviteItem, GroupJoinRequestItem } from '@/types/protocol';

/**
 * 群详情 / 群管理。
 *
 * <p>把「资料、成员、邀请码、入群申请」放在一页，是因为它们共用同一份权限判定
 * （{@code role.canManageGroup}）：拆成多个页面会出现「在 A 页能看到入口、点了才发现没权限」。
 *
 * <p>普通成员看到的只是资料 + 成员 + 退出；管理区整块不渲染，不是渲染后置灰——
 * 待审列表本身就是隐私（谁想进这个群）。
 */
const route = useRoute();
const router = useRouter();
const auth = useAuthStore();
const contact = useContactStore();
const im = useImStore();
const ui = useUiStore();

const groupId = computed(() => String(route.params.groupId ?? ''));
const group = computed(() => contact.groupOf(groupId.value));
const members = computed(() => contact.membersOf(groupId.value));
const myRole = computed(() => contact.myRoleIn(groupId.value));
const canManage = computed(() => contact.canManageGroup(groupId.value));
const isOwner = computed(() => myRole.value === 'OWNER');

const memberIds = computed(() => members.value?.members ?? []);
const pendingJoins = computed(() => contact.pendingJoinsOf(groupId.value));
/** 我自己在这群里的申请（没申请过 / 不是成员时才有意义） */
const myJoin = computed(() => contact.myJoinRequestOf(groupId.value) ?? null);
const pendingTotal = computed(() => contact.joinRequestTotals[groupId.value] ?? pendingJoins.value.length);
const invites = computed(() => contact.invitesOf(groupId.value));

const editingName = ref('');
const editingAnnouncement = ref('');
const memberInput = ref('');
const maxMembersInput = ref(0);

/** 生成邀请码的选项：0 表示服务端默认（不显式过期 / 不限次数） */
const inviteTtlHours = ref(24);
const inviteMaxUses = ref(0);
const inviteApproval = ref(false);
const latestInvite = ref<GroupInviteItem | null>(null);

function roleText(targetId: string): string {
  const role = members.value?.roles?.[targetId] ?? 'MEMBER';
  if (role === 'OWNER') return '群主';
  if (role === 'ADMIN') return '管理员';
  return '成员';
}

function muteLeft(targetId: string): number {
  const until = members.value?.mutes?.[targetId] ?? 0;
  return until > Date.now() ? Math.ceil((until - Date.now()) / 1000) : 0;
}

function inviteUseText(invite: GroupInviteItem): string {
  const used = invite.maxUses > 0 ? `${invite.uses}/${invite.maxUses}` : `${invite.uses}/不限`;
  if (invite.expiresAt <= 0) return `${used} 次 · 长期`;
  return `${used} 次 · ${formatListTime(invite.expiresAt)} 到期`;
}

function joinStatusText(item: GroupJoinRequestItem): string {
  if (item.status === 'APPROVED') return '已通过';
  if (item.status === 'REJECTED') return '已拒绝';
  return '待审批';
}

async function copy(text: string): Promise<void> {
  if (!navigator.clipboard) {
    ui.error('当前浏览器不支持复制');
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
    ui.success('已复制');
  } catch {
    ui.error('复制失败');
  }
}

function linkOf(invite: GroupInviteItem): string {
  // 服务端下发的 link 优先；linkBase 的约定是「前缀 + code」，
  // 默认值 `im://group/join?code=` 已经自带 `?code=`，所以这里只做拼接、不要再补一次。
  if (invite.link) return invite.link;
  return `${contact.inviteLinkBase || 'im://group/join?code='}${invite.code}`;
}

/** 刚生成的那条（单独置顶展示，省得用户在列表里找） */
const latestLink = computed(() => (latestInvite.value ? linkOf(latestInvite.value) : ''));

async function generateInvite(): Promise<void> {
  const invite = await contact.createInvite(groupId.value, {
    ttlHours: inviteTtlHours.value,
    maxUses: inviteMaxUses.value,
    approvalRequired: inviteApproval.value
  });
  if (invite) latestInvite.value = invite;
}

async function revoke(invite: GroupInviteItem): Promise<void> {
  if (!window.confirm('吊销后这条链接立即失效，确定？')) return;
  await contact.revokeInvite(groupId.value, invite.code);
  if (latestInvite.value?.code === invite.code) latestInvite.value = null;
}

async function saveName(): Promise<void> {
  const name = editingName.value.trim();
  if (!name) {
    ui.error('群名不能为空');
    return;
  }
  await contact.renameGroup(groupId.value, name);
  editingName.value = '';
}

async function saveAnnouncement(): Promise<void> {
  await contact.setAnnouncement(groupId.value, editingAnnouncement.value.trim());
  editingAnnouncement.value = '';
}

async function saveMaxMembers(): Promise<void> {
  if (maxMembersInput.value <= 0) {
    ui.error('人数上限必须大于 0');
    return;
  }
  await contact.setMaxMembers(groupId.value, maxMembersInput.value);
}

async function addMembers(): Promise<void> {
  const ids = memberInput.value
    .split(/[\s,，]+/)
    .map((id) => id.trim())
    .filter(Boolean);
  if (!ids.length) {
    ui.error('请输入至少一个 userId');
    return;
  }
  await contact.addMembers(groupId.value, ids);
  memberInput.value = '';
}

async function toggleAdmin(targetId: string): Promise<void> {
  await contact.setRole(groupId.value, targetId, roleText(targetId) === '管理员' ? 'MEMBER' : 'ADMIN');
}

async function kick(targetId: string): Promise<void> {
  if (!window.confirm(`把 ${contact.displayName(targetId)} 移出群聊？`)) return;
  await contact.removeMember(groupId.value, targetId);
}

async function mute(targetId: string): Promise<void> {
  const muted = muteLeft(targetId) > 0;
  await contact.muteMember(groupId.value, targetId, muted ? 0 : 600);
}

async function approve(item: GroupJoinRequestItem): Promise<void> {
  await contact.approveJoin(groupId.value, item.applicantId);
}

async function reject(item: GroupJoinRequestItem): Promise<void> {
  if (!window.confirm('拒绝后对方会收到提示，确定拒绝？')) return;
  await contact.rejectJoin(groupId.value, item.applicantId);
}

async function leave(): Promise<void> {
  if (!window.confirm('退出后不再接收该群消息，确定退出？')) return;
  await contact.leaveGroup(groupId.value);
  void router.replace('/conversations');
}

async function disband(): Promise<void> {
  if (!window.confirm('解散后所有成员都会被移除，且不可恢复，确定？')) return;
  await contact.disband(groupId.value);
  void router.replace('/conversations');
}

function openChat(): void {
  const conversationId = groupConversationId(groupId.value);
  im.ensureConversation(conversationId, 'GROUP');
  void router.push({ name: 'chat', params: { conversationId }, query: { type: 'GROUP' } });
}

/** 群管理页是「深链接」入口：直接打开时本地没有成员表，得先拉一次 */
async function load(): Promise<void> {
  if (!groupId.value) return;
  await contact.loadGroup(groupId.value);
  void contact.loadProfiles();
  await contact.loadMyJoinRequest(groupId.value);
  if (computedCanManage()) {
    await Promise.all([contact.loadPendingJoins(groupId.value), contact.loadInvites(groupId.value)]);
  }
  maxMembersInput.value = group.value?.maxMembers ?? 0;
}

/** 权限依赖成员表，必须等 loadGroup 之后再判断 */
function computedCanManage(): boolean {
  return contact.canManageGroup(groupId.value);
}

onMounted(load);
watch(groupId, load);
</script>

<template>
  <div class="page">
    <NavBar :title="group?.name || '群聊'" :subtitle="memberIds.length ? `${memberIds.length} 人` : ''">
      <template #right>
        <button class="nav-side-btn" @click="openChat">发消息</button>
      </template>
    </NavBar>

    <div class="page-body">
      <div v-if="!group" class="empty">群信息加载中…</div>

      <template v-else>
        <div class="list">
          <div class="cell">
            <Avatar :user-id="groupId" :name="group.name" :size="44" />
            <div class="cell-main">
              <div class="cell-title">
                <span class="cell-title-text">{{ group.name }}</span>
                <span v-if="group.mutedAll" class="tag orange">全员禁言</span>
                <span v-if="group.dismissed" class="tag orange">已解散</span>
              </div>
              <div class="cell-sub">{{ groupId }}</div>
            </div>
            <span class="tag">{{ roleText(auth.userId) }}</span>
          </div>
          <div class="cell">
            <div class="cell-main">
              <div class="cell-title"><span class="cell-title-text">群公告</span></div>
              <div class="cell-sub announcement">{{ group.announcement || '暂无群公告' }}</div>
            </div>
          </div>
          <div class="cell">
            <div class="cell-main">
              <div class="cell-title"><span class="cell-title-text">人数上限</span></div>
              <div class="cell-sub">{{ group.maxMembers > 0 ? `${group.maxMembers} 人` : '不限' }}</div>
            </div>
          </div>
        </div>

        <div v-if="myJoin" class="list">
          <div class="cell">
            <div class="cell-main">
              <div class="cell-title">
                <span class="cell-title-text">我的入群申请</span>
                <span class="tag" :class="{ green: myJoin.status === 'APPROVED' }">{{ joinStatusText(myJoin) }}</span>
              </div>
              <div v-if="myJoin.message" class="cell-sub">附言：{{ myJoin.message }}</div>
              <div class="cell-sub">{{ formatListTime(myJoin.updatedAt) }}</div>
            </div>
          </div>
        </div>

        <!-- 管理区：整块不给普通成员渲染 -->
        <template v-if="canManage">
          <div class="list-group-title">群资料</div>
          <div class="field section">
            <div class="row">
              <input v-model="editingName" placeholder="新的群名称" autocomplete="off" @keyup.enter="saveName" />
              <button class="btn small" @click="saveName">改名</button>
            </div>
            <div class="row">
              <input
                v-model="editingAnnouncement"
                placeholder="新的群公告"
                autocomplete="off"
                @keyup.enter="saveAnnouncement"
              />
              <button class="btn small" @click="saveAnnouncement">发布</button>
            </div>
            <div class="row">
              <input v-model.number="maxMembersInput" type="number" min="1" placeholder="人数上限" />
              <button class="btn small" @click="saveMaxMembers">保存</button>
            </div>
            <div class="row">
              <label>全员禁言</label>
              <button class="btn small ghost" @click="contact.setMuteAll(groupId, !group.mutedAll)">
                {{ group.mutedAll ? '解除禁言' : '开启禁言' }}
              </button>
            </div>
          </div>

          <div class="list-group-title">
            入群申请 {{ pendingTotal }}
            <button class="link-btn" @click="contact.loadPendingJoins(groupId)">刷新</button>
          </div>
          <div v-if="!pendingJoins.length" class="empty">没有待审批的申请</div>
          <div v-else class="list">
            <div v-for="item in pendingJoins" :key="item.applicantId" class="cell">
              <Avatar
                :user-id="item.applicantId"
                :name="contact.displayName(item.applicantId)"
                :src="contact.avatarOf(item.applicantId)"
              />
              <div class="cell-main">
                <div class="cell-title">
                  <span class="cell-title-text">{{ contact.displayName(item.applicantId) }}</span>
                  <span class="tag">{{ joinStatusText(item) }}</span>
                </div>
                <div v-if="item.message" class="cell-sub">附言：{{ item.message }}</div>
                <div class="cell-sub">{{ formatListTime(item.createdAt) }} 申请</div>
              </div>
              <div class="row-actions">
                <button class="btn small" @click="approve(item)">通过</button>
                <button class="btn small ghost" @click="reject(item)">拒绝</button>
              </div>
            </div>
          </div>

          <div class="list-group-title">
            邀请码 {{ invites.length }}
            <button class="link-btn" @click="contact.loadInvites(groupId)">刷新</button>
          </div>
          <div class="field section">
            <div class="row">
              <label>有效期(小时)</label>
              <input v-model.number="inviteTtlHours" type="number" min="0" placeholder="0 = 默认" />
            </div>
            <div class="row">
              <label>可用次数</label>
              <input v-model.number="inviteMaxUses" type="number" min="0" placeholder="0 = 不限" />
            </div>
            <div class="row">
              <label>需要审批</label>
              <input v-model="inviteApproval" type="checkbox" />
            </div>
            <div class="row">
              <button class="btn small" @click="generateInvite">生成邀请链接</button>
            </div>
            <div class="hint">二维码就是这条链接的图形化：{im://group/join?code=xxxx}，客户端只为「码」建模。</div>
          </div>

          <div v-if="latestInvite" class="list">
            <div class="cell">
              <div class="cell-main">
                <div class="cell-title">
                  <span class="cell-title-text mono">{{ latestInvite.code }}</span>
                  <span v-if="latestInvite.approvalRequired" class="tag orange">需审批</span>
                </div>
                <div class="cell-sub break-all">{{ latestLink }}</div>
                <div class="cell-sub">{{ inviteUseText(latestInvite) }}</div>
              </div>
              <div class="row-actions">
                <button class="btn small" @click="copy(latestLink)">复制</button>
              </div>
            </div>
          </div>

          <div v-if="!invites.length" class="empty">还没有放出邀请码</div>
          <div v-else class="list">
            <div v-for="invite in invites" :key="invite.code" class="cell">
              <div class="cell-main">
                <div class="cell-title">
                  <span class="cell-title-text mono">{{ invite.code }}</span>
                  <span v-if="invite.approvalRequired" class="tag orange">需审批</span>
                </div>
                <div class="cell-sub">{{ inviteUseText(invite) }} · {{ invite.inviterId }} 生成</div>
              </div>
              <div class="row-actions">
                <button class="btn small ghost" @click="copy(linkOf(invite))">复制</button>
                <button class="btn small danger ghost" @click="revoke(invite)">吊销</button>
              </div>
            </div>
          </div>

          <div class="list-group-title">添加成员</div>
          <div class="field section">
            <div class="row">
              <input
                v-model="memberInput"
                placeholder="userId，多个用空格分隔"
                autocomplete="off"
                @keyup.enter="addMembers"
              />
              <button class="btn small" @click="addMembers">添加</button>
            </div>
            <div class="hint">直接拉人只在服务端允许时生效；需要审批的场景请用邀请码。</div>
          </div>
        </template>

        <div class="list-group-title">成员 {{ memberIds.length }}</div>
        <div class="list">
          <div v-for="targetId in memberIds" :key="targetId" class="cell">
            <Avatar
              :user-id="targetId"
              :name="contact.displayName(targetId)"
              :src="contact.avatarOf(targetId)"
            />
            <div class="cell-main">
              <div class="cell-title">
                <span class="cell-title-text">{{ contact.displayName(targetId) }}</span>
                <span class="tag" :class="{ green: roleText(targetId) !== '成员' }">{{ roleText(targetId) }}</span>
                <span v-if="muteLeft(targetId) > 0" class="tag orange">禁言 {{ muteLeft(targetId) }}s</span>
              </div>
              <div class="cell-sub">{{ targetId }}</div>
            </div>
            <div v-if="canManage && targetId !== auth.userId" class="row-actions">
              <button v-if="isOwner" class="btn small ghost" @click="toggleAdmin(targetId)">
                {{ roleText(targetId) === '管理员' ? '撤管' : '设管' }}
              </button>
              <button class="btn small ghost" @click="mute(targetId)">
                {{ muteLeft(targetId) > 0 ? '解除' : '禁言' }}
              </button>
              <button class="btn small danger ghost" @click="kick(targetId)">移出</button>
            </div>
          </div>
        </div>

        <div class="section">
          <button class="btn danger block" @click="leave">退出群聊</button>
          <button v-if="isOwner" class="btn danger ghost block disband" @click="disband">解散群聊</button>
        </div>
      </template>
    </div>
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

.announcement {
  white-space: pre-wrap;
  word-break: break-word;
}

.break-all {
  word-break: break-all;
}

.row-actions {
  display: flex;
  gap: 6px;
  flex: none;
}

.disband {
  margin-top: 12px;
}
</style>
