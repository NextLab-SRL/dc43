"""Shared pytest configuration for the contracts app package tests."""

# The suite relies on the package's declared runtime dependencies (including
# ``tomlkit``) so we intentionally avoid conditional skips here. Install the
# project in editable mode with the test extras before running the tests to
# ensure all imports succeed.
