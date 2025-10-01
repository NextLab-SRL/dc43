# AGENTS Instructions

## Development
- Use `rg` for searching instead of `grep -R`.
- Install dependencies for tests and demo with:
  ```bash
  pip install -q pyspark==3.5.1 fastapi httpx uvicorn jinja2 python-multipart
  pip install -e . -q
  ```
- Run tests with `pytest -q` after making changes.
- Keep code small, readable, and documented.
- The demo application now lives under `packages/dc43-demo-app/src/dc43_demo_app`.
  When updating documentation, integration tests, or packaging metadata, ensure
  references use the `dc43_demo_app` import path and include the dedicated
  package in release metadata where relevant.

## Release process
- Package release automation reads package definitions from `scripts/_packages.py`.
  When adding, moving, or renaming source packages (including subpackages within
  the main `dc43` distribution), update the relevant `paths` list so the release
  tooling detects changes. The `dc43` entry must include `src/dc43` along with
  `pyproject.toml`, and the demo package is tracked separately under
  `packages/dc43-demo-app`.
- Use `python scripts/release.py` to preview releases and `python
  scripts/release.py --apply --push` to publish tags once validated.
- Internal packages live under `packages/` and rely on their own `pyproject.toml`
  files. Update their metadata and changelogs alongside code changes.

## Testing & quality
- `pytest -q` runs the test suite. Some tests write temporary artifacts under
  `test_temp_files/`; the directory is cleaned automatically by pytest.
- Linting is handled via reviewers; no automated linter is configured in this
  repository, so keep style consistent with existing code (PEP 8-ish).
- For local manual validation of the demo application, start the FastAPI server
  with `uvicorn dc43_demo_app.server:app --reload` and open the reported URL.
