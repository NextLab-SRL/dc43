# Contracts app container

The contracts helper lives under `packages/dc43-contracts-app`. Ship it as a container so
Spark developers can generate integration stubs without cloning the repository. The image
runs the FastAPI UI and talks to the shared governance backend in **remote** mode by
default.

## Build and publish

The packaging helper accepts `contracts-app` as a target. Build an image locally or push
directly to your registry:

```bash
# Build only
python scripts/package_http_backend.py --target contracts-app --image dc43-contracts-app:local

# Build and push
python scripts/package_http_backend.py \
  --target contracts-app \
  --image myregistry.azurecr.io/dc43/contracts-app:latest \
  --push
```

To drive Docker manually run:

```bash
docker build -t dc43-contracts-app -f deploy/contracts-app/Dockerfile .
```

## Runtime configuration

The container exposes the UI on port `8000` and expects a reachable governance backend.
Set the following environment variables when starting the container:

- `DC43_CONTRACTS_APP_BACKEND_URL` – URL for the HTTP backend published by your
  operations team (for example `https://governance.example.com`).
- `DC43_BACKEND_TOKEN` – shared secret required by the backend service.
- `DC43_CONTRACTS_APP_BACKEND_MODE` – override to `embedded` if you want the container to
  spawn the in-process backend instead of dialing a remote service.

Example:

```bash
docker run --rm \
  -p 8000:8000 \
  -e DC43_CONTRACTS_APP_BACKEND_URL="https://governance.example.com" \
  -e DC43_BACKEND_TOKEN="super-secret" \
  myregistry.azurecr.io/dc43/contracts-app:latest
```

Mount a contracts directory under `/contracts` and set `DC43_CONTRACT_STORE` if you run in
embedded mode and need persistent drafts.
