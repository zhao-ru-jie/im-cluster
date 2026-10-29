import { createApp } from 'vue';
import { createPinia } from 'pinia';
import App from './App.vue';
import router from './router';
import './styles/base.css';

const app = createApp(App);

// pinia 必须先装：路由守卫里会 useAuthStore()
app.use(createPinia());
app.use(router);
app.mount('#app');
