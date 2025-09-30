# dc43 service backend HTTP container

This directory contains ready-to-use assets for packaging the dc43 service
backends behind the HTTP interface. The image installs the local packages from
this repository, so you can iterate on backend implementations and rebuild the
container without publishing wheels.

## Building the image

From the repository root, build the Docker image with the provided Dockerfile:

```bash
docker build -t dc43-service-backends-http -f deploy/http-backend/Dockerfile .
```

The build stage copies the service client and backend packages and installs the
HTTP extra so FastAPI and its dependencies are available in the runtime image.

## Runtime configuration

The container honours the following environment variables:

- `DC43_CONTRACT_STORE` (default: `/contracts`) – filesystem path where
  contracts and local governance drafts are read from.
- `DC43_BACKEND_TOKEN` (optional) – when set, the server requires every request
  to include `Authorization: Bearer <token>`.

Mount the contract directory and publish the HTTP port when running the
container:

```bash
docker run \
  --rm \
  -p 8001:8001 \
  -e DC43_BACKEND_TOKEN="super-secret" \
  -v /local/contracts:/contracts:ro \
  dc43-service-backends-http
```

You can omit the token for local experimentation. Use a read/write mount if you
expect the local governance backend to persist draft proposals in the contract
store.

## Health checks and probes

The FastAPI application exposes an unauthenticated readiness endpoint at
`/health`. Combine it with your orchestrator's liveness/readiness probes. When a
token is configured, supply the same header to the probe command:

```bash
curl -fsSL -H 'Authorization: Bearer super-secret' http://localhost:8001/health
```

## Customising dependencies

If you maintain custom backends inside the repository, adjust the Dockerfile to
copy their package directories before installing. For fully external
implementations published to PyPI, replace the `pip install` commands with the
corresponding package names.
