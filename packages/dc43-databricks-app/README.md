# dc43-databricks-app

A Databricks App (Streamlit) that joins dc43 contract services with Databricks
system tables: contract listing and schema drift vs Unity Catalog,
criticality-aware scoring (lineage blast radius × observed usage × declared
`criticality` tag), cost attribution from `system.billing`, and pipeline
reliability from `system.lakeflow`.

Contracts resolve exclusively through `dc43_service_clients` — a remote dc43
service backend (`DC43_CONTRACTS_URL`) or a filesystem store on a UC volume
(`DC43_CONTRACT_PATH`) — with bundled demo contracts and deterministic demo
data so the app runs without any workspace:

```bash
pip install -e "packages/dc43-databricks-app[test]"
streamlit run src/dc43_databricks_app/app.py
```

Full deployment guidance (warehouse resource binding, system-table grants,
contract sources) lives in
[`docs/operations/databricks-app.md`](../../docs/operations/databricks-app.md).
