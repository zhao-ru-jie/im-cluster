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

/**
 * 「正在输入」的重发间隔：4s。
 *
 * <p>这个值的**下限**由对端的过期时间决定（{@code stores/im.ts} 的 {@code TYPING_TTL}=6s）：
 * 重发间隔必须小于它，否则用户一直在打字、对面三个点却自己灭了。上限则由「越少越好」决定——
 * 它不代表用户打字的速度，只代表「我还在这儿」的心跳。
 *
 * <p>取 4s 而不是贴着 6s：留 2s 给网络抖动与两端时钟差；同时 4s &gt; 服务端
 * {@code TypingNotifier} 的 3s 去抖窗口，每次上行都会被真正扇出，没有白发的帧。
 */
const TYPING_THROTTLE = 4000;
/**
 * 停手多久补一条 typing=false：3s。
 *
 * <p>比「重发间隔」短一点（否则停手后对方还要多等一个周期才看到消失），
 * 又不能太短：一句完整的话中间常有停顿（想下一句、切输入法），1~2s 就发 false
 * 会让对面的「正在输入」反复闪。
 */
const TYPING_IDLE = 3000;

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
  // 内容被删空就不再是「正在输入」：微信在输入框为空时同样不显示，
  // 也让「全选删除后停在那儿」不会占着对方的状态栏
  if (!text.value.trim()) {
    stopTyping();
    return;
  }
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
  // 录音不是打字：不掐掉的话对方会看到「正在输入」一直挂着
  stopTyping();
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
