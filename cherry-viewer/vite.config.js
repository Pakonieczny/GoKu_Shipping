import { defineConfig } from 'vite';

export default defineConfig({
  root: 'cherry-viewer',
  base: '/',
  build: {
    outDir: '../cherry-viewer-dist',
    emptyOutDir: true,
  },
});
