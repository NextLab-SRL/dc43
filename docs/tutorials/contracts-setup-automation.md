# Automate the contracts app setup wizard

The contracts application exposes a three-step **Environment setup** wizard under `/setup` that guides platform teams through module selection, connection details, and a final completion badge.【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L18-L339】 Each step uses stable `data-*` attributes that power the bundled JavaScript navigation helpers, so UI automation tools can hook into the same selectors without relying on brittle DOM positions.【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L70-L205】【F:packages/dc43-contracts-app/src/dc43_contracts_app/static/setup.js†L72-L404】

This guide shows how to:

- run the pre-baked Playwright scenarios that ship alongside the repository,
- capture new flows and extend the scenario catalogue, and
- verify the wizard manually from the UI when you want to explore changes without automation.

## 1. Run the packaged Playwright scenarios

1. Prepare the contracts app just like you would for manual testing by installing dependencies and starting the FastAPI server (`uvicorn dc43_contracts_app.server:app --host 0.0.0.0 --port 8000`).【F:docs/getting-started/spark-contract-app-helper.md†L6-L49】 The `/setup?restart=1` query parameter resets `setup_state.json` before the wizard renders so every run starts from a clean slate.【F:packages/dc43-contracts-app/src/dc43_contracts_app/server.py†L6149-L6285】
2. Install Playwright's Python bindings alongside the repo and pull the Chromium runtime the script expects:

   ```bash
   pip install playwright
   playwright install chromium
   ```

3. Inspect the available scenarios:

   ```bash
   python scripts/ui/setup_wizard.py --list
   ```

   The script reads `scripts/ui/setup_wizard_scenarios.json` and prints the short descriptions you can target from CI or local runs.【F:scripts/ui/setup_wizard.py†L98-L119】【F:scripts/ui/setup_wizard_scenarios.json†L1-L89】 The bundled `enterprise_oidc` scenario shows how to pre-populate Collibra credentials, Databricks tokens, OAuth settings, and Terraform deployment values when you need a fully managed environment baseline.【F:scripts/ui/setup_wizard_scenarios.json†L45-L111】

4. Execute a scenario. The example below drives the `happy_path` configuration in headless mode and captures a screenshot once the wizard lands on the home page:

   ```bash
   python scripts/ui/setup_wizard.py --scenario happy_path --headless --screenshot artifacts/setup.png
   ```

   The runner loads `/setup?restart=1`, selects the configured module options, fills any overridden form fields, asserts that the summary renders, and checks that the browser is redirected back to `/` after marking the setup complete.【F:scripts/ui/setup_wizard.py†L122-L203】 Pass `--keep-open` when you want to watch the flow interactively instead of running headless.

5. Need to observe each stage or capture an audit trail? Combine `--step-through` with the scenario you want to validate. The runner pauses after every wizard step so you can inspect the UI before continuing, automatically keeping the browser open at the end. Pair it with `--log-actions` to persist the executed steps as JSON:

   ```bash
   python scripts/ui/setup_wizard.py \
     --scenario enterprise_oidc \
     --step-through \
     --log-actions artifacts/enterprise-oidc.json
   ```

   The JSON log lists every selector interaction and button press in order, which makes it easy to replay or audit the scenario without enabling verbose Playwright tracing.【F:scripts/ui/setup_wizard.py†L50-L303】

Add the same command to your CI pipeline to reuse the bundled selectors without copying code. Because the helper always resets the wizard, multiple jobs can run the flow sequentially against the same server without leaking state.【F:scripts/ui/setup_wizard.py†L111-L127】【F:packages/dc43-contracts-app/src/dc43_contracts_app/server.py†L6149-L6285】

## 2. Extend or override scenarios

Scenarios are stored as JSON mappings of module selections and configuration overrides. Each key corresponds to the wizard's data attributes so the automation stays aligned with server-side validation.【F:scripts/ui/setup_wizard_scenarios.json†L1-L117】【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L98-L304】 Add a new entry that mirrors the structure below when you want the script to drive a different configuration:

```json
{
  "my_custom_flow": {
    "description": "Short explanation shown by --list.",
    "moduleSelections": {
      "contracts_backend": "filesystem",
      "user_interface": "local_web"
    },
    "configurationOverrides": {
      "config__contracts_backend__work_dir": "/srv/contracts"
    }
  }
}
```

Store custom definitions in a separate file and point the runner at it with `--scenario-file` to keep local experiments out of version control:

```bash
python scripts/ui/setup_wizard.py --scenario my_custom_flow --scenario-file /path/to/scenarios.json
```

When authoring a brand-new path, use Playwright's recorder to bootstrap selectors before copying them into the JSON structure:

```bash
playwright codegen http://localhost:8000/setup?restart=1
```

Walk through the wizard manually, then translate the generated actions into `moduleSelections` and `configurationOverrides` entries. The recorder is especially useful for discovering new field names whenever server-side modules introduce additional configuration.【F:packages/dc43-contracts-app/src/dc43_contracts_app/server.py†L6168-L6285】

## 3. Explore the wizard from the UI

Automation is helpful for regression coverage, but the contracts app UI still exposes everything you need for manual validation:

1. Launch the app and open `http://localhost:8000/setup?restart=1`. Step 1 displays grouped module cards keyed by `data-module-key` attributes so you can confirm the selectors your scripts expect.【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L98-L200】
2. Continue to Step 2 to fill required configuration fields. Inputs are named `config__<module_key>__<field>`; the same names appear in the JSON state embedded in the page so you can inspect available overrides while iterating on automation.【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L214-L304】
3. Finish the wizard on Step 3. The server persists selections, redirects back to `/`, and renders the completion badge that both humans and automation rely on to verify success.【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L308-L339】【F:packages/dc43-contracts-app/src/dc43_contracts_app/server.py†L6231-L6285】

Refer back to the scenario JSON whenever you need a reminder of the combinations exercised by CI, then branch out manually to test edge cases such as missing configuration or invalid credentials.

## 4. Reuse the selectors in other frameworks

Prefer Cypress (or another browser runner) instead of Playwright? Target the same `data-module-key` hooks and form field names so both suites stay aligned:

```js
cy.visit('http://localhost:8000/setup?restart=1');
cy.get('[data-module-key="contracts_backend"] input[value="filesystem"]').check();
cy.contains('button', 'Continue').click();
```

The wizard embeds machine-readable JSON under `#setup-state` that mirrors the server's understanding of required fields, dependencies, and automatic selections.【F:packages/dc43-contracts-app/src/dc43_contracts_app/templates/setup.html†L340-L341】【F:packages/dc43-contracts-app/src/dc43_contracts_app/server.py†L4799-L4837】 Parse that payload from your preferred framework to keep selector drift under control as new modules ship.
