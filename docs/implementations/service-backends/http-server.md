# Service backend HTTP server

The `dc43-service-backends` package now ships a FastAPI application that exposes
the contract, data-quality, and governance interfaces over HTTP. Pipelines can
communicate with the service through the remote clients in
`dc43_service_clients.*`, keeping their dependency footprint minimal while the
backend layer evolves independently.

## Application entrypoint

The module [`dc43_service_backends.webapp`](../../../packages/dc43-service-backends/src/dc43_service_backends/webapp.py)
builds a ready-to-serve FastAPI instance. Configure it with environment
variables and run it with `uvicorn`:

```bash
pip install "dc43-service-backends[http]"

export DC43_CONTRACT_STORE=/opt/dc43/contracts
export DC43_BACKEND_TOKEN="super-secret"  # optional bearer token

uvicorn dc43_service_backends.webapp:app --host 0.0.0.0 --port 8001
```

- **`DC43_CONTRACT_STORE`** — directory that stores contract JSON documents. The
  local governance backend also persists draft proposals in the same path.
- **`DC43_BACKEND_TOKEN`** — optional shared secret. When set, every request must
  include an `Authorization: Bearer …` header. Omit the variable to run the
  service without authentication (useful for local experiments).

You can exercise the API with `curl`:

```bash
curl -H 'Authorization: Bearer super-secret' \
  http://localhost:8001/contracts/orders/latest
```

## Docker deployment assets

Ready-to-build container assets live under
[`deploy/http-backend/`](../../../deploy/http-backend/README.md). The bundled
Dockerfile installs the repository's client and backend packages with the HTTP
extra, exposing the FastAPI application on port `8001`.

Build the image and run it while mounting your contract directory and supplying
the optional token:

```bash
docker build -t dc43-service-backends-http -f deploy/http-backend/Dockerfile .

docker run \
  -p 8001:8001 \
  -e DC43_BACKEND_TOKEN="super-secret" \
  -v /local/contracts:/contracts:ro \
  dc43-service-backends-http
```

- Mount the contract directory (`/local/contracts`) read-only when contracts are
  managed externally. Use a read/write mount if you rely on the draft-store
  behaviour of the local governance backend.
- Drop the `DC43_BACKEND_TOKEN` variable to disable authentication for internal
  networks.

The container exposes a `/health` endpoint suitable for readiness probes. When a
token is configured, include the same bearer header in your probe command.
Combine the image with your preferred process manager or orchestrator; remote
clients only require the base URL and optional token to interact with the
service, making it straightforward to scale the backend separately from the
pipelines.
