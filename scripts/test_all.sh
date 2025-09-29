#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

install_deps=true
pytest_args=()

for arg in "$@"; do
  if [[ "$arg" == "--skip-install" ]]; then
    install_deps=false
  else
    pytest_args+=("$arg")
  fi
done

if [[ "$install_deps" == true ]]; then
    python -m pip install -e ".[test]"
    python -m pip install -e "packages/dc43-service-backends[http]"
    python -m pip install -e "packages/dc43-service-clients[http]"
fi

pytest_targets=(
  tests
  packages/dc43-service-clients/tests
  packages/dc43-service-backends/tests
  packages/dc43-integrations/tests
)

pytest_cmd=(pytest -q)
pytest_cmd+=("${pytest_targets[@]}")
if (( ${#pytest_args[@]} )); then
  pytest_cmd+=("${pytest_args[@]}")
fi

"${pytest_cmd[@]}"
