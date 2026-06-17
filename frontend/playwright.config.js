import { defineConfig, devices } from '@playwright/test';

/**
 * Configuration des tests end-to-end Playwright.
 *
 * Les tests s'exécutent contre la stack Docker Compose complète
 * (frontend nginx + API + PostgreSQL seedé). L'URL cible est surchargeable
 * via la variable d'environnement BASE_URL (défaut : http://localhost:5173,
 * port exposé par le service `frontend`).
 *
 *   # 1. démarrer la stack
 *   docker compose up -d
 *   # 2. lancer les tests
 *   cd frontend && npm run test:e2e
 */
export default defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: process.env.BASE_URL ?? 'http://localhost:5173',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
});
