<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from 'vue';
import Avatar from './Avatar.vue';
import { isEmojiOnly } from '@/core/emoji';
import { formatDuration, formatSize, parseAttrs } from '@/core/format';
import type { ChatMessage } from '@/types/local';

const props = withDefaults(
  defineProps<{
    message: ChatMessage;
    senderName: string;
    self: boolean;
    isGroup?: boolean;
    avatar?: string;
  }>(),
  { isGroup: false, avatar: '' }
);

const emit = defineEmits<{
  (e: 'longpress', message: ChatMessage): void;
  (e: 'preview-image', src: string): void;
  (e: 'retry', message: ChatMessage): void;
}>();

const attrs = computed(() => parseAttrs(props.message.attrs));
const emojiOnly = computed(() => props.message.contentType === 'TEXT' && isEmojiOnly(props.message.body));
const playing = ref(false);
let audio: HTMLAudioElement | null = null;

const statusText = computed(() => {
  switch (props.message.status) {
    case 'sending':
      return '发送中';
    case 'failed':
      return '发送失败，点击重发';
    case 'sent':
      return '已发送';
    case 'delivered':
      return '已送达';
    case 'read':
      return '已读';
    case 'recalled':
      return '';
    default:
      return '';
  }
});

let pressTimer = 0;

function startPress(): void {
  cancelPress();
  pressTimer = window.setTimeout(() => {
    pressTimer = 0;
    emit('longpress', props.message);
  }, 480);
}

function cancelPress(): void {
  if (pressTimer) {
    clearTimeout(pressTimer);
    pressTimer = 0;
  }
}

function onStatusTap(): void {
  if (props.message.status === 'failed') emit('retry', props.message);
}

function toggleVoice(): void {
  const src = attrs.value.url;
  if (!src) return;
  if (audio && playing.value) {
    audio.pause();
    playing.value = false;
    return;
  }
  audio = audio ?? new Audio(src);
  audio.onended = () => {
    playing.value = false;
  };
  playing.value = true;
  void audio.play().catch(() => {
    playing.value = false;
  });
}

onBeforeUnmount(() => {
  cancelPress();
  audio?.pause();
  audio = null;
});
</script>

<template>
  <div
    class="msg-row"
    :class="{ self }"
    @touchstart.passive="startPress"
    @touchend="cancelPress"
    @touchmove="cancelPress"
    @touchcancel="cancelPress"
    @contextmenu.prevent="emit('longpress', message)"
  >
    <Avatar v-if="!self" :user-id="message.from" :name="senderName" :src="avatar" :size="38" />

    <div class="msg-content">
      <div v-if="isGroup && !self && !message.recalled" class="msg-sender">{{ senderName }}</div>

      <!-- 已撤回 -->
      <template v-if="message.recalled">
        <div class="bubble plain">{{ self ? '你撤回了一条消息' : '对方撤回了一条消息' }}</div>
      </template>

      <!-- 图片 -->
      <template v-else-if="message.contentType === 'IMAGE'">
        <img
          v-if="attrs.url"
          class="media"
          :src="attrs.url"
          :alt="message.body || '图片'"
          @click="emit('preview-image', attrs.url!)"
        />
        <div v-else class="bubble">[图片]</div>
      </template>

      <!-- 语音 -->
      <template v-else-if="message.contentType === 'VOICE'">
        <div class="bubble" @click="toggleVoice">
          <div class="voice">
            <span>{{ playing ? '⏸' : '▶' }}</span>
            <span>{{ attrs.duration ? formatDuration(attrs.duration) : '语音' }}</span>
            <span class="muted small">{{ playing ? '播放中' : '点击播放' }}</span>
          </div>
        </div>
      </template>

      <!-- 视频 -->
      <template v-else-if="message.contentType === 'VIDEO'">
        <video
          v-if="attrs.url"
          class="media"
          :src="attrs.url"
          controls
          playsinline
          webkit-playsinline
          preload="metadata"
        />
        <div v-else class="bubble">[视频]</div>
      </template>

      <!-- 文件 -->
      <template v-else-if="message.contentType === 'FILE'">
        <a class="bubble" :href="attrs.url" :download="attrs.name || 'file'" target="_blank" rel="noopener">
          <div class="file">
            <span class="file-icon">FILE</span>
            <span>
              <div style="font-size: 14px">{{ attrs.name || message.body || '文件' }}</div>
              <div class="muted small">{{ formatSize(attrs.size) }}</div>
            </span>
          </div>
        </a>
      </template>

      <!-- 文本（纯表情放大显示） -->
      <template v-else>
        <div class="bubble" :class="{ 'emoji-only': emojiOnly }">{{ message.body }}</div>
      </template>

      <div v-if="self" class="msg-meta" @click="onStatusTap">
        <span
          class="msg-status"
          :class="{ sending: message.status === 'sending', failed: message.status === 'failed' }"
        >
          {{ statusText }}
        </span>
      </div>
    </div>
  </div>
</template>
