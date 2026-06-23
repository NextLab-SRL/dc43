---
description: How to publish a Release Candidate to Test PyPI via PR
---

# Publishing Release Candidates to Test PyPI

When working on a feature branch (Pull Request) and you want to publish the package to Test PyPI for validation, **you must not manually bump the version numbers in `VERSION`, `pyproject.toml`, or `CHANGELOG.md` to a new `rc`**. 

## Why?
The CI workflow defined in `.github/workflows/ci.yml` leverages `scripts/test_pypi_versions.py`. This script automatically overrides any existing `rc` suffix on the *base version* and injects the current GitHub Actions run number (e.g. `rc1234`). 
Bumping the version locally to `rc2` simply pollutes the Git history, as the CI will ignore the manual step and rewrite the package metadata anyway to `0.41.0.0rc<RUN_NUMBER>`.

## The correct workflow:
1. Make your normal code changes.
2. Add your feature or fix entries to the `## [Unreleased]` section of the relevant `CHANGELOG.md` files. **Do not create a dated `rc` header**.
3. Push your commits to your Pull Request.
4. Add the GitHub label `publish-test-pypi` to the Pull Request.
5. The CI will trigger, automatically append the correct `rc` suffix using the run number, and publish the packages to Test PyPI.
