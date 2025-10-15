import { defineConfig } from '@playwright/test';

const baseURL = process.env.SETUP_WIZARD_BASE_URL ?? 'http://localhost:8002';
const webServerCommand =
  process.env.SETUP_WIZARD_SERVER_COMMAND ?? 'npm run start:contracts-app';
const shouldStartServer = !process.env.PLAYWRIGHT_SKIP_CONTRACTS_SERVER;

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
  webServer: shouldStartServer
    ? [
        {
          command: webServerCommand,
          url: baseURL,
          reuseExistingServer: !process.env.CI,
          timeout: 120 * 1000,
        },
      ]
    : undefined,
});
