import { expect, Locator, Page, test } from '@playwright/test';
import {
  setupWizardScenarioEntries,
  SetupWizardScenario,
} from './scenarios';

test.describe('Contracts setup wizard', () => {
  for (const [scenarioName, scenario] of setupWizardScenarioEntries) {
    test(`@${scenarioName} completes the setup wizard`, async ({ page }, testInfo) => {
      testInfo.annotations.push({ type: 'scenario', description: scenarioName });
      testInfo.annotations.push({ type: 'description', description: scenario.description });
      testInfo.attachments.push({
        name: 'scenario.json',
        contentType: 'application/json',
        body: Buffer.from(JSON.stringify(scenario, null, 2), 'utf-8'),
      });

      await test.step('Open the wizard', async () => {
        await page.goto('/setup?restart=1');
        await expect(page.locator('#setup-root')).toBeVisible();
        await expect(page.locator('[data-step1-wizard]')).toBeVisible();
      });

      await completeModuleSelection(page, scenario);
      await fillConfiguration(page, scenario);
      const projectUse = (testInfo.project.use as { baseURL?: string } | undefined) ?? {};
      const expectedBaseURL =
        projectUse.baseURL ?? process.env.SETUP_WIZARD_BASE_URL ?? 'http://localhost:8002';
      await finishWizard(page, expectedBaseURL);
    });
  }
});

async function completeModuleSelection(page: Page, scenario: SetupWizardScenario) {
  await test.step('Select required modules', async () => {
    for (const [moduleKey, optionKey] of Object.entries(scenario.moduleSelections)) {
      await test.step(`Select ${moduleKey} → ${optionKey}`, async () => {
        const result = await ensureModuleVisible(page, moduleKey);
        const option = result.card.locator(`input[type="radio"][value="${optionKey}"]`).first();
        await expect(option, `Module '${moduleKey}' option '${optionKey}' is unavailable`).toHaveCount(1);

        const optionDisabled = await option.isDisabled();
        const optionChecked = await option.isChecked();

        if (result.permanentlyHidden || result.hidden) {
          if (!optionChecked) {
            const available = await result.card
              .locator('input[type="radio"]').evaluateAll((elements) =>
                elements.map((element) => ({
                  value: (element as HTMLInputElement).value,
                  checked: (element as HTMLInputElement).checked,
                })),
              );
            throw new Error(
              `Module '${moduleKey}' is hidden but option '${optionKey}' is not preselected. Current selections: ${JSON.stringify(
                available,
              )}`,
            );
          }
          return;
        }

        if (optionDisabled) {
          if (!optionChecked) {
            throw new Error(`Option '${optionKey}' for module '${moduleKey}' is disabled.`);
          }
          return;
        }

        const inputId = await option.getAttribute('id');
        if (await option.isVisible()) {
          await option.scrollIntoViewIfNeeded();
          await option.check();
        } else if (inputId) {
          const label = result.card.locator(`label[for="${inputId}"]`).first();
          await expect(label, `Hidden option '${optionKey}' for module '${moduleKey}' is missing a visible label.`).toBeVisible();
          await label.scrollIntoViewIfNeeded();
          await label.click();
        } else {
          await option.scrollIntoViewIfNeeded();
          await option.click({ force: true });
        }

        await expect(option, `Failed to select option '${optionKey}' for module '${moduleKey}'.`).toBeChecked();
      });
    }

    await page.getByRole('button', { name: 'Continue' }).click();
    await expect(page).toHaveURL('**/setup?step=2');
    await expect(page.locator('[data-step2-wizard]')).toBeVisible();
  });
}

async function fillConfiguration(page: Page, scenario: SetupWizardScenario) {
  await test.step('Fill configuration overrides', async () => {
    await expect(page.locator('form').filter({ has: page.locator('button', { name: 'Review summary' }) })).toBeVisible();

    for (const [fieldName, value] of Object.entries(scenario.configurationOverrides)) {
      await test.step(`Fill ${fieldName}`, async () => {
        const field = page.locator(`[name="${fieldName}"]`).first();
        await expect(field, `Configuration field '${fieldName}' is not present.`).toBeVisible();

        const tagName = await field.evaluate((element) => element.tagName.toLowerCase());
        if (tagName === 'select') {
          await field.selectOption(value);
        } else {
          await field.fill(value);
        }
      });
    }

    await page.getByRole('button', { name: 'Review summary' }).click();
    await expect(page).toHaveURL('**/setup?step=3');
    await expect(page.locator('[data-step3-wizard]')).toBeVisible();
  });
}

async function finishWizard(page: Page, baseURL: string) {
  await test.step('Review summary and finish', async () => {
    await expect(page.locator('h2', { hasText: 'Summary' })).toBeVisible();
    await page.getByRole('button', { name: 'Mark setup as complete' }).click();

    await expect(page).toHaveURL(new RegExp(`^${escapeForRegex(trimTrailingSlash(baseURL))}/?$`));
  });
}

async function ensureModuleVisible(page: Page, moduleKey: string) {
  const card = page.locator(`[data-module-card][data-module-key="${moduleKey}"]`).first();
  await card.waitFor({ state: 'attached' });

  let visibility = await moduleVisibility(card);

  if (visibility.permanentlyHidden) {
    return { card, hidden: true, permanentlyHidden: true };
  }

  if (visibility.group) {
    await ensureGroupActive(page, visibility.group);
    visibility = await moduleVisibility(card);
  }

  if (visibility.hidden) {
    await expect(
      card,
      `Module '${moduleKey}' did not become visible after activating group '${visibility.group ?? 'default'}'.`,
    ).toBeVisible();
    return { card, hidden: false, permanentlyHidden: false };
  }

  await expect(card, `Module '${moduleKey}' should be visible but is not.`).toBeVisible();
  return { card, hidden: false, permanentlyHidden: false };
}

async function moduleVisibility(card: Locator) {
  return card.evaluate((element) => {
    const computeHidden = (node: Element | null): boolean => {
      if (!node) {
        return false;
      }

      const htmlElement = node as HTMLElement;
      if (
        htmlElement.hidden ||
        htmlElement.hasAttribute('hidden') ||
        htmlElement.classList.contains('d-none')
      ) {
        return true;
      }

      const style = getComputedStyle(htmlElement);
      if (style.display === 'none' || style.visibility === 'hidden') {
        return true;
      }

      return computeHidden(htmlElement.parentElement);
    };

    const rect = element.getBoundingClientRect();
    const hidden = computeHidden(element) || rect.width === 0 || rect.height === 0;
    const group = element.closest('[data-step1-section]')?.getAttribute('data-step1-section') ?? null;
    const permanentlyHidden = element.getAttribute('data-module-hidden') === 'true';

    return { hidden, group, permanentlyHidden };
  });
}

async function ensureGroupActive(page: Page, group: string) {
  const toggle = page.locator(`[data-step1-nav="${group}"]`).first();
  await expect(toggle, `Missing navigation button for module group '${group}'.`).toBeVisible();
  await toggle.scrollIntoViewIfNeeded();

  const isActive = async () =>
    toggle.evaluate((element) => {
      const htmlElement = element as HTMLElement;
      const ariaPressed = htmlElement.getAttribute('aria-pressed');
      const ariaExpanded = htmlElement.getAttribute('aria-expanded');
      const ariaCurrent = htmlElement.getAttribute('aria-current');
      const datasetActive = (htmlElement.dataset && htmlElement.dataset.active) ?? null;

      if (
        ariaPressed === 'true' ||
        ariaExpanded === 'true' ||
        ariaCurrent === 'true' ||
        datasetActive === 'true' ||
        htmlElement.classList.contains('active')
      ) {
        return true;
      }

      return false;
    });

  if (!(await isActive())) {
    await toggle.click();
    await expect
      .poll(async () => await isActive(), {
        message: `Navigation button for group '${group}' did not activate after clicking.`,
      })
      .toBeTruthy();
  }
}

function trimTrailingSlash(value: string) {
  return value.endsWith('/') ? value.slice(0, -1) : value;
}

function escapeForRegex(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
