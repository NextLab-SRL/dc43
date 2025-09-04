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

