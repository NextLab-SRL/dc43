# dc43-core changelog

## [Unreleased]

## [0.43.0.0] - 2026-08-31

### Added
- Added `resolve_table_name`, `resolve_storage_path`, `list_schema_objects`, and `find_schema_object` to facilitate standard ODCS table and path resolution across Server types (Databricks, Snowflake, BigQuery, SQL, and storage paths) and SchemaObjects (`physicalName`).
- Enhanced `build_odcs()` with `physical_name` and `physical_type` parameters.

## [0.42.0.0] - 2026-05-21

### Fixed
- Coerce version and apiVersion fields to strings in to_model() to handle unquoted numeric versions when parsing YAML (e.g. version: 5).
- Support 'v' prefixed apiVersion values (e.g. 'v3.0.2', 'v3.1.0') in ensure_version() to comply with the official Open Data Contract Standard (Bitol) specification and support Collibra exports.

## [0.41.0.0] - 2026-03-26

## [0.40.0.0] - 2026-03-19

### Added
- Bumped Open Data Contract Standard (ODCS) support to `>=3.0.2,<4.0.0` permitting `3.1.0` documents, and exposed `ODCS_SUPPORTED_VERSIONS` for abstraction.

## [0.39.0.0] - 2026-03-18

### Changed
- Version aligned to 0.39.0.0

## [0.35.0.0] - 2026-03-09

### Added
- Initial extraction of ODCS/ODPS helpers and SemVer utilities from the
  service backends into a standalone shared package used by all dc43
  components.
