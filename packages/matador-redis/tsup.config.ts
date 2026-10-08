import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['cjs', 'esm'],
  dts: false,
  splitting: false,
  sourcemap: true,
  clean: false,
  outDir: 'dist',
  target: 'node18',
  treeshake: true,
  external: ['@zdavison/matador'],
});
