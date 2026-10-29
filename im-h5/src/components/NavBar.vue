<script setup lang="ts">
import { useRouter } from 'vue-router';

withDefaults(
  defineProps<{
    title: string;
    subtitle?: string;
    back?: boolean;
    solid?: boolean;
    /**
     * 标题此刻是「状态文本」而不是实体名（例如「对方正在输入…」）。
     *
     * <p>微信的做法是把它放在标题位置，但视觉上要弱于人名：人名是加粗的实体标识，
     * 状态是一句话。用这个开关让调用方表达「我传的是状态」，样式由 `.nav-title.muted` 负责。
     */
    titleMuted?: boolean;
  }>(),
  { subtitle: '', back: true, solid: false, titleMuted: false }
);

const emit = defineEmits<{ (e: 'back'): void }>();
const router = useRouter();

function onBack(): void {
  emit('back');
  if (window.history.length > 1) router.back();
  else void router.replace('/conversations');
}
</script>

<template>
  <header class="nav" :class="{ solid }">
    <div class="nav-side">
      <button v-if="back" class="nav-back" @click="onBack">返回</button>
      <slot name="left" />
    </div>
    <div class="nav-title" :class="{ muted: titleMuted }">
      {{ title }}
      <span v-if="subtitle" class="nav-subtitle">{{ subtitle }}</span>
    </div>
    <div class="nav-side right">
      <slot name="right" />
    </div>
  </header>
</template>
