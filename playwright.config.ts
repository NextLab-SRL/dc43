import { defineConfig } from '@playwright/test';

const baseURL = process.env.SETUP_WIZARD_BASE_URL ?? 'http://localhost:8002';

export default defineConfig({
  testDir: 'tests/playwright',
  timeout: 2 * 60 * 1000,
  expect: {
    timeout: 5000,
  },
  reporter: [
    ['list'],
    ['html', { outputFolder: 'playwright-report', open: 'never' }],
  ],
  use: {
    baseURL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    actionTimeout: 15000,
    navigationTimeout: 30000,
  },
});
