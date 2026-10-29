import { createRouter, createWebHashHistory } from 'vue-router';
import { useAuthStore } from '@/stores/auth';

/**
 * 路由说明：
 *
 * - 用 hash 模式：产物要能丢到任意静态目录/OSS/微信里直接打开，不需要服务端 rewrite；
 * - 聊天页把 conversationId 放在路径里（里面含 `:`），靠 `params` 自动编码，别手拼字符串；
 * - 守卫里先 `await restore()`，否则刷新页面时 userId 还没从 localStorage 回来，
 *   会被误判成未登录踢到登录页。
 */
const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/conversations' },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/pages/LoginPage.vue'),
      meta: { guest: true }
    },
    {
      path: '/conversations',
      name: 'conversations',
      component: () => import('@/pages/ConversationsPage.vue')
    },
    {
      path: '/chat/:conversationId',
      name: 'chat',
      component: () => import('@/pages/ChatPage.vue')
    },
    {
      path: '/contacts',
      name: 'contacts',
      component: () => import('@/pages/ContactsPage.vue')
    },
    {
      path: '/me',
      name: 'me',
      component: () => import('@/pages/MePage.vue')
    },
    { path: '/:pathMatch(.*)*', redirect: '/conversations' }
  ]
});

router.beforeEach(async (to) => {
  const auth = useAuthStore();
  if (!auth.restored) await auth.restore();

  if (to.meta.guest) {
    return auth.loggedIn ? { path: '/conversations' } : true;
  }
  if (!auth.loggedIn) {
    return { path: '/login', query: to.fullPath === '/' ? {} : { redirect: to.fullPath } };
  }
  return true;
});

export default router;
