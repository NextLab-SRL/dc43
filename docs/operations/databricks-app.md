# Deploying the dc43 Databricks App

The `dc43-databricks-app` package ships a Streamlit application designed to run
as a [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html).
It gives platform and governance teams one console over:

- **Contracts & drift** — ODCS contracts resolved through the dc43 service
  clients, with live schema comparison against Unity Catalog
  (`system.information_schema.columns`). Breaking findings signal that a new
  contract version should be drafted, mirroring the dc43 drafting loop.
- **Criticality** — a 0–100 score per table blending *observed* importance
  (downstream blast radius and read activity from `system.access.table_lineage`)
  with *declared* importance (the `criticality` Unity Catalog tag). Divergence
  between observed and declared is raised as a governance gap (`under_declared`).
- **Cost & pipelines** — spend attribution from `system.billing.usage ×
  system.billing.list_prices` and job reliability from
  `system.lakeflow.job_run_timeline`. Every panel exposes the SQL it runs.

## Contract sources

The app talks to contracts only through `dc43_service_clients`, so any backend
works without UI changes. Resolution order:

| Environment variable | Client | Use case |
| --- | --- | --- |
| `DC43_CONTRACTS_URL` (+ optional `DC43_CONTRACTS_TOKEN`) | `RemoteContractServiceClient` | A deployed dc43 service backend (`dc43_service_backends.webapp`) |
| `DC43_CONTRACT_PATH` | `LocalContractServiceClient` + `FSContractStore` | Contracts on a UC volume / DBFS: `<path>/<contract_id>/<version>.json` |
| *(neither)* | bundled demo contracts | Local evaluation, demos |

## Deployment

1. Create the app (Workspace → Compute → Apps) from
   `packages/dc43-databricks-app`, or sync the folder with the CLI. The
   provided `app.yaml` starts Streamlit and `requirements.txt` installs the
   package with the `databricks` and `http` extras.
2. Bind a SQL warehouse as an app resource named `sql-warehouse`; `app.yaml`
   maps it to `DATABRICKS_WAREHOUSE_ID`.
3. Grant the app's service principal access to system tables:

   ```sql
   GRANT USE CATALOG ON CATALOG system TO `<app-service-principal>`;
   GRANT SELECT ON SCHEMA system.billing  TO `<app-service-principal>`;
   GRANT SELECT ON SCHEMA system.access   TO `<app-service-principal>`;
   GRANT SELECT ON SCHEMA system.lakeflow TO `<app-service-principal>`;
   ```

   `system.access` and `system.lakeflow` must be enabled once per metastore via
   the system schema enablement API.
4. Point `DC43_CONTRACT_PATH` (or `DC43_CONTRACTS_URL`) at your contract
   source, and optionally tag tables:

   ```sql
   ALTER TABLE main.sales.orders SET TAGS ('criticality' = 'high');
   ```

Without a warehouse or contract source, the app runs entirely on bundled
deterministic demo data — `streamlit run src/dc43_databricks_app/app.py`
works on a laptop with no Databricks access. Live queries are fail-soft:
if a system-table query errors (missing grants, warehouse down), the affected
panel falls back to demo data with a visible warning.

## Criticality score

```
score = 0.40 × blast_radius  (distinct downstream tables, 90d lineage, log-scaled)
      + 0.30 × usage         (read events, 30d, log-scaled)
      + 0.30 × declared      (criticality tag: critical=100 … low=25)
```

`under_declared` is raised when observed importance (blast + usage, normalised
0–100) exceeds the declared tag by more than 30 points — for example an
untagged table silently feeding seventeen downstream tables.

## Tests

```bash
pip install -e "packages/dc43-databricks-app[test]"
pytest -q packages/dc43-databricks-app/tests
```
