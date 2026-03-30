import { defineConfig } from 'vite';

export default defineConfig({
  root: 'cherry-viewer',
  base: '/cherry-viewer/',
  build: {
    outDir: '../cherry-viewer-dist/cherry-viewer',
    emptyOutDir: true,
  },
});
