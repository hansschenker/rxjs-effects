import { defineConfig } from "vitest/config";

export default defineConfig({
  build: {
    lib: { entry: "src/index.ts", formats: ["es"], fileName: () => "index.js" },
    rollupOptions: { external: ["rxjs"] }
  },
  test: { include: ["test/**/*.test.ts"] }
});
