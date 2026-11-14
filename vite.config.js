import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    allowedHosts: ['kb-mybastracker-simulatedlive.onrender.com']
  },
  // `npm test` (vitest). Only *.test.jsx under src/; not part of the build.
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{js,jsx}'],
  }
})
