import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// En développement local, les appels /api sont proxifiés vers l'API FastAPI.
// En production (Docker), nginx gère lui-même ce proxy.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
});
