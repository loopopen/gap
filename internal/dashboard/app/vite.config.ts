import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  base: './',
  plugins: [react()],
  build: {
    chunkSizeWarningLimit: 1000,
    rollupOptions: {
      output: {
        manualChunks: {
          vendor: ['axios'],
          ui: ['@traefik-labs/faency'],
        }
      }
    }
  },
  server: {
    proxy: {
      '^/dashboard/api/': {
        target: 'http://localhost:8080',
        changeOrigin: true
      }
    }
  }
})

