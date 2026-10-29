import { fileURLToPath, URL } from 'node:url';
import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

/**
 * 构建目标说明：
 * - `base: './'`：产物可以放在任意子目录（nginx / OSS / 后端 static）下直接打开；
 * - `target: 'es2018'`：兼容 iOS 12+/Android 5+ 的老 WebView，以及微信内置浏览器；
 * - dev 期不配 proxy：服务端 CORS 默认放开（`im.http.cors.allowed-origins=*`），
 *   前端可以直接跨端口打 8080/8081；要改成同源部署时用 `VITE_REST_BASE` / `VITE_WS_BASE` 覆盖。
 */
export default defineConfig({
  base: './',
  plugins: [vue()],
  resolve: {
    // 与 tsconfig 的 paths 保持一致：'@/x' → 'src/x'
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  build: {
    target: 'es2018',
    cssTarget: 'chrome70',
    sourcemap: false,
    chunkSizeWarningLimit: 800
  },
  server: {
    host: true,
    port: 5173
  }
});
