import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import {defineConfig} from 'vite';

export default defineConfig(() => {
  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    server: {
      // HMR is disabled in AI Studio via DISABLE_HMR env var.
      // Do not modify—file watching is disabled to prevent flickering during agent edits.
      hmr: process.env.DISABLE_HMR !== 'true',
      // Disable file watching when DISABLE_HMR is true to save CPU during agent edits.
      watch: process.env.DISABLE_HMR === 'true' ? null : {},
      // LIVE mode calls same-origin /api/* and /ws/*; forward them to FastAPI
      // (streamforge/api/main.py, default :8000) so `npm run dev` works with
      // the backend running. Override with FASTAPI_URL if needed.
      proxy: {
        '/api': {
          target: process.env.FASTAPI_URL || 'http://localhost:8000',
          changeOrigin: true,
        },
        '/ws': {
          target: (process.env.FASTAPI_URL || 'http://localhost:8000').replace(/^http/, 'ws'),
          ws: true,
          changeOrigin: true,
        },
      },
    },
  };
});
