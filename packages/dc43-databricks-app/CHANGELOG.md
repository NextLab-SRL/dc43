# dc43-databricks-app changelog

## [Unreleased]

### Added
- Initial release: a Databricks App (Streamlit) that joins dc43 contract
  services with Databricks system tables — contract listing and schema drift
  vs Unity Catalog, criticality-aware scoring (lineage blast radius ×
  observed usage × declared `criticality` tag), cost attribution from
  `system.billing`, and pipeline reliability from `system.lakeflow`.
- Contract source resolution via `dc43_service_clients`: remote HTTP backend
  (`DC43_CONTRACTS_URL`), local `FSContractStore` on a UC volume
  (`DC43_CONTRACT_PATH`), or bundled demo contracts.
- Demo mode with deterministic datasets so the app runs without a workspace.
