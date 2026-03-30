import { defineConfig } from 'vite';

export default defineConfig({
  root: 'cherry-viewer',
  build: {
    outDir: '../dist/cherry-viewer',
    emptyOutDir: true,
  },
});
