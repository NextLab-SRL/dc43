# dc43 changelog

## [Unreleased]
### Added
- Added a generated Mermaid dependency graph and supporting script so internal package
  relationships are easier to audit during releases.

### Changed
- Documented the Gitflow-based branching expectations and clarified how merges from `dev` to `main`
  trigger automated releases in the release guide.
- Relaxed internal package pins in `setup.py` to resolve pip conflicts when installing extras
  that pull from local editable copies.
- Migrated the ODCS/ODPS helpers into the backend package and kept the meta
  distribution as a thin compatibility layer to eliminate dependency cycles
  when installing integration extras.
