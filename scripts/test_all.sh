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
fi

pytest -q tests packages/dc43-service-clients/tests \
  packages/dc43-service-backends/tests packages/dc43-integrations/tests \
  "${pytest_args[@]}"
