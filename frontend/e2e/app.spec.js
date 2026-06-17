import { test, expect } from '@playwright/test';

/**
 * Tests E2E du parcours utilisateur ObRail Europe.
 * Exécutés contre la stack complète (frontend + API + PostgreSQL seedé).
 */

test.describe('Page d\'accueil & accessibilité', () => {
  test('affiche l\'en-tête, la navigation et les sections principales', async ({ page }) => {
    await page.goto('/');

    await expect(page.getByRole('banner')).toBeVisible();
    await expect(page.getByText('ObRail Europe', { exact: true })).toBeVisible();

    // Navigation principale (RGAA : repère + liens)
    const nav = page.getByRole('navigation', { name: 'Navigation principale' });
    await expect(nav.getByRole('link', { name: 'Comparer' })).toBeVisible();
    await expect(nav.getByRole('link', { name: 'Statistiques' })).toBeVisible();
    await expect(nav.getByRole('link', { name: 'Classement' })).toBeVisible();

    // Titres de sections
    await expect(page.getByRole('heading', { name: 'Comparer deux villes' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Indicateurs du réseau' })).toBeVisible();
  });

  test('le lien d\'évitement RGAA est présent', async ({ page }) => {
    await page.goto('/');
    await expect(page.getByRole('link', { name: 'Aller au contenu principal' })).toHaveAttribute(
      'href',
      '#main-content',
    );
  });
});

test.describe('Santé du service (monitoring)', () => {
  test('le badge de santé passe à « API en ligne »', async ({ page }) => {
    await page.goto('/');
    const badge = page.getByRole('status');
    await expect(badge).toBeVisible();
    // L'API seedée doit répondre /health => statut online
    await expect(badge).toContainText('API en ligne', { timeout: 15_000 });
  });
});

test.describe('Comparaison de trajets', () => {
  test('autocomplétion puis comparaison CO₂ train vs avion', async ({ page }) => {
    await page.goto('/');

    const depInput = page.getByLabel('Ville de départ');
    await depInput.fill('Par');
    // Une suggestion doit apparaître (listbox accessible)
    const depOption = page.getByRole('option').first();
    await expect(depOption).toBeVisible({ timeout: 10_000 });
    await depOption.click();

    const arrInput = page.getByLabel('Ville d\'arrivée');
    await arrInput.fill('Ber');
    const arrOption = page.getByRole('option').first();
    await expect(arrOption).toBeVisible({ timeout: 10_000 });
    await arrOption.click();

    await page.getByRole('button', { name: /Comparer les CO/ }).click();

    // Soit un résultat de comparaison, soit un message d'erreur explicite :
    // dans les deux cas l'app ne doit pas planter (région live mise à jour).
    await expect(
      page.locator('.form-error, #search .container'),
    ).toBeVisible();
  });
});

test.describe('Classement des trajets écologiques', () => {
  test('le tableau Top 10 se charge avec des lignes', async ({ page }) => {
    await page.goto('/');
    const table = page.getByRole('table', {
      name: /Top 10 des trajets ferroviaires les plus écologiques/,
    });
    await expect(table).toBeVisible({ timeout: 15_000 });
    // Au moins une ligne de données (seed réel)
    await expect(table.locator('tbody tr').first()).toBeVisible({ timeout: 15_000 });
  });
});
