import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

/**
 * Tests d'accessibilité automatisés (axe-core) — vérification objective de la
 * conformité RGAA / WCAG revendiquée par le frontend. On échoue sur toute
 * violation de gravité « serious » ou « critical ».
 */
test.describe('Accessibilité (axe-core / RGAA)', () => {
  test('la page d\'accueil ne présente aucune violation serious/critical', async ({ page }) => {
    await page.goto('/');

    // Attendre le rendu principal avant l'audit.
    await expect(page.getByRole('main')).toBeVisible();
    // Auditer l'état CHARGÉ (et non les placeholders transitoires) : on attend
    // que plus aucun panneau ne soit en cours de chargement (aria-busy).
    await page.waitForFunction(
      () => !document.querySelector('[aria-busy="true"]'),
      null,
      { timeout: 20_000 },
    );

    const results = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa'])
      .analyze();

    const blocking = results.violations.filter(
      (v) => v.impact === 'serious' || v.impact === 'critical',
    );

    // Message lisible en cas d'échec (liste des règles enfreintes).
    expect(
      blocking,
      blocking.map((v) => `${v.id} (${v.impact}): ${v.help}`).join('\n'),
    ).toEqual([]);
  });
});
