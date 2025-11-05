import { expect, test, type APIRequestContext } from '@playwright/test';

interface SeededContract {
  readonly id: string;
  readonly version: string;
}

async function ensureContract(request: APIRequestContext): Promise<SeededContract> {
  const now = Date.now();
  const id = `playwright.contract.${now}`;
  const version = '0.1.0';
  const payload = {
    id,
    version,
    name: 'Playwright seeded contract',
    kind: 'DataContract',
    apiVersion: '3.0.2',
    status: 'draft',
    description: 'Contract created automatically for data product editor tests.',
    schemaObjects: [
      {
        name: 'orders',
        properties: [
          {
            name: 'order_id',
            physicalType: 'string',
            required: true,
          },
        ],
      },
    ],
    servers: [
      {
        server: 'contracts-app-tests',
        type: 'filesystem',
        path: '/tmp/orders',
      },
    ],
  };

  const response = await request.post('/contracts/new', {
    form: {
      payload: JSON.stringify(payload),
    },
    timeout: 30_000,
  });

  if (!response.ok()) {
    throw new Error(`Failed to seed contract: ${response.status()} ${await response.text()}`);
  }

  return { id, version };
}

test.describe('Data product editor', () => {
  test('creates a data product linked to an existing contract', async ({ page, request }) => {
    const contract = await ensureContract(request);
    const productId = `playwright.product.${Date.now()}`;

    await page.goto('/data-products/new');
    await expect(page.locator('#data-product-form')).toBeVisible();

    await expect(page.locator(`#contract-id-options option[value="${contract.id}"]`)).toHaveCount(1);

    await page.getByLabel('Data product ID').fill(productId);
    await page.getByLabel('Version').fill('0.1.0');
    await page.getByLabel('Status').selectOption('draft');
    await page.getByLabel('Name').fill('Playwright data product');
    await page.getByLabel('Description').fill('Created from the automated UI test.');
    await page.getByLabel('Tags').fill('playwright, test');

    const inputPort = page.locator('.input-port').first();
    await inputPort.getByLabel('Port name').fill('source_orders');
    await inputPort.getByLabel('Contract ID').fill(contract.id);
    await inputPort.getByLabel('Contract version').fill(contract.version);
    await expect(
      inputPort.locator(`.contract-version-options option[value="${contract.version}"]`),
    ).toHaveCount(1);
    await inputPort.getByLabel('Source data product').fill('upstream.contracts');
    await inputPort.getByLabel('Source output port').fill('orders_raw');

    const outputPort = page.locator('.output-port').first();
    await outputPort.getByLabel('Port name').fill('analytics');
    await outputPort.getByLabel('Contract ID').fill(contract.id);
    await outputPort.getByLabel('Contract version').fill(contract.version);
    await expect(
      outputPort.locator(`.contract-version-options option[value="${contract.version}"]`),
    ).toHaveCount(1);
    await outputPort.getByLabel('Dataset ID').fill('analytics.orders');
    await outputPort.getByLabel('Stage contract').fill(`${contract.id}.staging`);

    await outputPort.getByRole('button', { name: 'Add property' }).click();
    const outputCustomRow = outputPort.locator('.custom-property').last();
    await outputCustomRow.getByLabel('Property').fill('dc43.owner');
    await outputCustomRow.getByLabel('Value').fill('data-platform');

    await inputPort.getByRole('button', { name: 'Add property' }).click();
    const inputCustomRow = inputPort.locator('.custom-property').last();
    await inputCustomRow.getByLabel('Property').fill('dc43.lineage.note');
    await inputCustomRow.getByLabel('Value').fill('seeded for e2e coverage');

    await Promise.all([
      page.waitForURL('**/data-products'),
      page.getByRole('button', { name: 'Save data product' }).click(),
    ]);

    const row = page.getByRole('row', { name: /Playwright data product/i });
    await expect(row).toBeVisible();
    await expect(row).toContainText(productId);

    await row.getByRole('link', { name: 'Playwright data product' }).click();
    await page.waitForURL(`**/data-products/${productId}`);
    await expect(page.getByRole('heading', { level: 1, name: 'Playwright data product' })).toBeVisible();
    await expect(page.locator('body')).toContainText(contract.id);
    await expect(page.locator('body')).toContainText('analytics.orders');
  });
});
