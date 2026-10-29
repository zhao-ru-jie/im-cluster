<script setup lang="ts">
import { nextTick, ref } from 'vue';
import EmojiPanel from './EmojiPanel.vue';
import {
  MediaTooLargeError,
  VoiceRecorder,
  fileToPayload,
  imageToPayload,
  isVoiceSupported,
  videoToPayload,
  type MediaPayload
} from '@/core/media';

const props = withDefaults(defineProps<{ disabled?: boolean; placeholder?: string }>(), {
  disabled: false,
  placeholder: '发消息…'
});

const emit = defineEmits<{
  (e: 'send-text', text: string): void;
  (e: 'send-media', payload: MediaPayload): void;
  (e: 'typing', typing: boolean): void;
  (e: 'error', message: string): void;
}>();

const text = ref('');
const mode = ref<'text' | 'emoji' | 'plus'>('text');
const textarea = ref<HTMLTextAreaElement | null>(null);
const imageInput = ref<HTMLInputElement | null>(null);
const videoInput = ref<HTMLInputElement | null>(null);
const fileInput = ref<HTMLInputElement | null>(null);

const voiceMode = ref(false);
const recording = ref(false);
const busy = ref(false);
const recorder = new VoiceRecorder();

/** 「正在输入」的重发间隔。服务端对 true 的去抖是 3s、对端 6s 自动过期，2s 重发既不丢状态也不至于每键一帧 */
const TYPING_THROTTLE = 2000;
/** 停手多久补一条 typing=false */
const TYPING_IDLE = 2000;

let typingTimer = 0;
let typingSentAt = 0;
let typingActive = false;

function notifyTyping(): void {
  const now = Date.now();
  // 首帧立刻发；之后按 TYPING_THROTTLE 节流，避免每输入一个字就上行一帧
  if (!typingActive || now - typingSentAt >= TYPING_THROTTLE) {
    emit('typing', true);
    typingActive = true;
    typingSentAt = now;
  }
  if (typingTimer) clearTimeout(typingTimer);
  typingTimer = window.setTimeout(stopTyping, TYPING_IDLE);
}

function stopTyping(): void {
  if (typingTimer) {
    clearTimeout(typingTimer);
    typingTimer = 0;
  }
  if (!typingActive) return;
  typingActive = false;
  typingSentAt = 0;
  emit('typing', false);
}

function autoGrow(): void {
  const el = textarea.value;
  if (!el) return;
  el.style.height = 'auto';
  el.style.height = `${Math.min(el.scrollHeight, 110)}px`;
}

function onInput(): void {
  autoGrow();
  notifyTyping();
}

function send(): void {
  const value = text.value.trim();
  if (!value || props.disabled) return;
  emit('send-text', value);
  text.value = '';
  stopTyping();
  void nextTick(autoGrow);
}

function onKeydown(event: KeyboardEvent): void {
  // 中文输入法组合期间的回车不能当发送
  if (event.key !== 'Enter' || event.shiftKey || event.isComposing) return;
  event.preventDefault();
  send();
}

function pickEmoji(emoji: string): void {
  text.value += emoji;
  notifyTyping();
  void nextTick(() => {
    autoGrow();
    textarea.value?.focus();
  });
}

function toggleMode(next: 'emoji' | 'plus'): void {
  mode.value = mode.value === next ? 'text' : next;
  voiceMode.value = false;
}

async function handleFile(event: Event, kind: 'image' | 'video' | 'file'): Promise<void> {
  const input = event.target as HTMLInputElement;
  const selected = input.files?.[0];
  input.value = '';
  if (!selected) return;
  busy.value = true;
  try {
    const payload =
      kind === 'image' ? await imageToPayload(selected)
        : kind === 'video' ? await videoToPayload(selected)
          : await fileToPayload(selected);
    emit('send-media', payload);
    mode.value = 'text';
  } catch (e) {
    emit('error', e instanceof MediaTooLargeError || e instanceof Error ? e.message : '处理失败');
  } finally {
    busy.value = false;
  }
}

async function startRecord(event: Event): Promise<void> {
  event.preventDefault();
  if (!isVoiceSupported()) {
    emit('error', '当前浏览器不支持录音（微信 iOS 需要 14.3+）');
    return;
  }
  try {
    await recorder.start();
    recording.value = true;
  } catch {
    recording.value = false;
    emit('error', '无法使用麦克风，请检查权限');
  }
}

async function stopRecord(event: Event): Promise<void> {
  event.preventDefault();
  if (!recording.value) return;
  recording.value = false;
  try {
    const payload = await recorder.stop();
    emit('send-media', payload);
  } catch (e) {
    const message = e instanceof Error ? e.message : '录音失败';
    if (!message.includes('太短')) emit('error', message);
  }
}
</script>

<template>
  <div class="composer">
    <div class="composer-row">
      <button class="composer-btn" :class="{ active: voiceMode }" @click="voiceMode = !voiceMode; mode = 'text'">
        {{ voiceMode ? '⌨️' : '🎤' }}
      </button>

      <template v-if="voiceMode">
        <button
          class="recording"
          :class="{ live: recording }"
          @touchstart="startRecord"
          @touchend="stopRecord"
          @touchcancel="stopRecord"
          @mousedown.prevent="startRecord"
          @mouseup.prevent="stopRecord"
          @mouseleave="recording && stopRecord($event)"
        >
          {{ recording ? '松手发送…' : '按住说话' }}
        </button>
      </template>

      <template v-else>
        <textarea
          ref="textarea"
          v-model="text"
          class="composer-input"
          rows="1"
          :placeholder="busy ? '正在处理媒体…' : placeholder"
          :disabled="disabled || busy"
          @input="onInput"
          @keydown="onKeydown"
        />
        <button class="composer-btn" :class="{ active: mode === 'emoji' }" @click="toggleMode('emoji')">😊</button>
        <button v-if="!text.trim()" class="composer-btn" :class="{ active: mode === 'plus' }" @click="toggleMode('plus')">
          ＋
        </button>
        <button v-else class="composer-send" :disabled="disabled || busy" @click="send">发送</button>
      </template>
    </div>

    <EmojiPanel v-if="mode === 'emoji'" @pick="pickEmoji" />

    <div v-if="mode === 'plus'" class="plus-panel">
      <button class="plus-item" @click="imageInput?.click()">
        <span class="box">🖼️</span>
        图片
      </button>
      <button class="plus-item" @click="videoInput?.click()">
        <span class="box">🎬</span>
        视频
      </button>
      <button class="plus-item" @click="fileInput?.click()">
        <span class="box">📎</span>
        文件
      </button>
      <button class="plus-item" @click="voiceMode = true; mode = 'text'">
        <span class="box">🎤</span>
        语音
      </button>
    </div>

    <input ref="imageInput" type="file" accept="image/*" hidden @change="handleFile($event, 'image')" />
    <input ref="videoInput" type="file" accept="video/*" hidden @change="handleFile($event, 'video')" />
    <input ref="fileInput" type="file" hidden @change="handleFile($event, 'file')" />
  </div>
</template>
