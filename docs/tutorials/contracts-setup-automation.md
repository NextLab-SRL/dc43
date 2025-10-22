# Automate the contracts app setup wizard

The contracts application exposes a three-step **Environment setup** wizard under `/setup` that guides platform teams through module selection, connection details, and a final completion badge. Each step uses stable `data-*` attributes that power the bundled JavaScript navigation helpers, so UI automation tools can hook into the same selectors without relying on brittle DOM positions.

This guide shows how to:

- run the pre-baked Playwright scenarios that ship alongside the repository,
- capture new flows and extend the scenario catalogue, and
- verify the wizard manually from the UI when you want to explore changes without automation.

## 1. Run the packaged Playwright scenarios

1. Install the Python dependencies that power the contracts application and make the packages importable:

   ```bash
   pip install -q pyspark==3.5.1 fastapi httpx uvicorn jinja2 python-multipart
   pip install -e .
   ```

   The FastAPI server exposes `/setup` and the supporting templates when these packages are available on the Python path.
2. Install the Node dependencies that drive the TypeScript-based Playwright suite:

   ```bash
   npm install
   ```

   The `postinstall` script downloads the necessary Playwright browsers (including system dependencies) so the UI tests can run without extra setup.
3. Run the tests. The Playwright configuration automatically starts a contracts app instance on `http://localhost:8002` before exercising any scenarios and shuts it down when the suite finishes. List the available scenarios with:

   ```bash
   npm run test:ui -- --list
   ```

   The list output mirrors the scenario catalogue defined in `packages/dc43-contracts-app/tests/playwright/scenarios.ts`. The bundled scenarios include:

   - `@local_only` (tagged with `@happy_path`) for self-contained workstation demos that stick to embedded services and filesystem stores.
   - `@databricks_jobs` (tagged with `@governance_focus` and `@databricks`) for Unity Catalog-heavy pipelines where Databricks configuration is the primary concern.
   - `@enterprise_oidc` for Collibra, Databricks, Terraform, and OIDC integrations exercised together.

4. Execute a scenario. The example below drives the `local_only` configuration in headless mode. Add `--headed` when you want to watch the browser as it progresses through each wizard stage:

   ```bash
   npm run test:ui -- --grep @local_only
   ```

   The runner targets `http://localhost:8002` by default, loads `/setup?restart=1`, selects the configured module options, fills any overridden form fields, and verifies the wizard's `data-current-step` attribute advances to Step 2 and Step 3 before marking the setup complete. Because the UI keeps the Step 1 panel mounted for reference, the tests watch the attribute instead of requiring Step 1 to disappear. The summary step asserts that the **Download configuration bundle** link streams a non-empty ZIP before clicking **Mark setup as complete**. Pass `--headed` or run `npm run test:ui:headed -- --grep @local_only` for interactive sessions.

5. Need to observe each stage or capture an audit trail? Enable Playwright's inspector or capture a trace:

   ```bash
   npm run test:ui:debug -- --grep @enterprise_oidc
   # or
   npx playwright test --grep @enterprise_oidc --trace on
   ```

   The tests attach the exercised scenario as JSON to each run. Open the HTML report (`npx playwright show-report`) to review the recorded steps, download the scenario payload, or open the generated traces.

Add the same command to your CI pipeline to reuse the bundled selectors without copying code. Because the helper always resets the wizard, multiple jobs can run the flow sequentially against the same server without leaking state.

## 2. Extend or override scenarios

Scenarios are stored in `packages/dc43-contracts-app/tests/playwright/scenarios.ts` as TypeScript objects that map module selections and configuration overrides. Each key corresponds to the wizard's data attributes so the automation stays aligned with server-side validation. Add a new entry that mirrors the structure below when you want the suite to drive a different configuration:

```ts
export const setupWizardScenarios = {
  ...
  my_custom_flow: {
    description: 'Short explanation shown in test annotations.',
    moduleSelections: {
      contracts_backend: 'filesystem',
      user_interface: 'local_web',
    },
    configurationOverrides: {
      config__contracts_backend__work_dir: '/srv/contracts',
    },
  },
};
```

Playwright automatically discovers the new scenario on the next run, making it available through `--grep @my_custom_flow`. If you prefer to keep local experiments out of version control, duplicate `packages/dc43-contracts-app/tests/playwright/setup-wizard.spec.ts`, import your own scenario catalogue, and execute the copy from your workstation.

When authoring a brand-new path, use Playwright's recorder to bootstrap selectors before copying them into the scenario definition:

```bash
npm run codegen:setup
```

Walk through the wizard manually, then translate the generated actions into `moduleSelections` and `configurationOverrides` entries. The recorder is especially useful for discovering new field names whenever server-side modules introduce additional configuration.

Need to take over before the suite finishes? Set `KEEP_WIZARD_OPEN=1` (or run `npm run test:ui:handoff -- --grep @databricks_jobs`) to let the automation prepare the summary screen, download the bundle, and then pause so you can continue manually. When you're ready for Playwright to clean up, press **Enter** in the terminal running the test. If you're launching the handoff from a non-interactive terminal, close the Playwright browser window (or press <kbd>Ctrl</kbd>+<kbd>C</kbd>/<kbd>Ctrl</kbd>+<kbd>Break</kbd>) to resume cleanup. The browser stays open the entire time, letting you experiment with additional actions or re-run the generated scripts from the downloaded archive.

## 3. Explore the wizard from the UI

Automation is helpful for regression coverage, but the contracts app UI still exposes everything you need for manual validation:

1. Launch the app and open `http://localhost:8002/setup?restart=1`. Step 1 displays grouped module cards keyed by `data-module-key` attributes so you can confirm the selectors your scripts expect.
2. Continue to Step 2 to fill required configuration fields. Inputs are named `config__<module_key>__<field>`; the same names appear in the JSON state embedded in the page so you can inspect available overrides while iterating on automation. When you want a quick baseline, click **Generate sample configuration** to populate the form with realistic placeholders sourced from `dc43_contracts_app/static/setup-wizard-template.json`.
3. Finish the wizard on Step 3. The server persists selections, redirects back to `/`, and renders the completion badge that both humans and automation rely on to verify success. The automated suite now waits for the configuration bundle download to succeed before finishing, so you can spot regressions where the export breaks even when the UI appears healthy.

Refer back to the scenario definitions whenever you need a reminder of the combinations exercised by CI, then branch out manually to test edge cases such as missing configuration or invalid credentials.

## 4. Reuse the selectors in other frameworks

Prefer Cypress (or another browser runner) instead of Playwright? Target the same `data-module-key` hooks and form field names so both suites stay aligned:

```js
cy.visit('http://localhost:8002/setup?restart=1');
cy.get('[data-module-key="contracts_backend"] input[value="filesystem"]').check();
cy.contains('button', 'Continue').click();
```

The wizard embeds machine-readable JSON under `#setup-state` that mirrors the server's understanding of required fields, dependencies, and automatic selections. Parse that payload from your preferred framework to keep selector drift under control as new modules ship.
