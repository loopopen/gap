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
          react: ['react', 'react-dom'],
          vendor: ['axios'],
          ui: ['antd'],
        }
      }
    }
  },
  server: {
    proxy: {
      '^/api/gap.*/': {
        target: 'http://localhost:8080',
        changeOrigin: true
      }
    }
  }
})

