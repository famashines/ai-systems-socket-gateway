import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.ts",
  outputDir: "../../test-results",
  workers: 1,
  retries: 0,
  timeout: 120_000,
  reporter: [["list"], ["html", { outputFolder: "../../playwright-report", open: "never" }]],
  use: { ...devices["Desktop Chrome"], trace: "on", screenshot: "on" },
});
