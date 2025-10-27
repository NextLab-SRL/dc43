# Spark developers: consume shared dc43 services

Follow this guide once your platform team exposes the dc43 governance services over HTTP. You will re-use the Spark helpers but
point them at the shared backend instead of embedding the store locally.

## 1. Gather connection details

Ask your operations team for:

- The base URL of the governance service (for example `https://governance.example.com`).
- The shared `DC43_BACKEND_TOKEN` secret, if authentication is enabled.
- Context about the backing contract store (filesystem, SQL, Collibra, Delta) so you can mirror behaviour locally when needed.

## 2. Install the runtime helpers

You only need the service clients (with the HTTP extra) and Spark integrations when the backend runs remotely:

```bash
python -m venv .venv
source .venv/bin/activate
pip install "dc43-service-clients[http]"
pip install "dc43-integrations[spark]"
```

If you already installed the full stack using `pip install -e .` you can reuse that environment.

## 3. Configure the remote backend

Choose one of the following methods:

### Environment variables

```bash
export DC43_CONTRACTS_APP_BACKEND_MODE="remote"
export DC43_CONTRACTS_APP_BACKEND_URL="https://governance.example.com"
export DC43_BACKEND_TOKEN="super-secret"
```

### Explicit configuration file

Update your TOML configuration:

```toml
# dc43-service-backends.toml
[contract_store]
type = "remote"
base_url = "https://governance.example.com"
token = "super-secret"
```

Load it at runtime:

```python
from dc43_service_clients import load_governance_client

governance_service = load_governance_client("/path/to/dc43-service-backends.toml")
```

## 4. Enforce contracts from Spark

Once the governance client is initialised, the IO helpers behave the same way as in the local guide:

```python
from dc43_integrations.spark.io import (
    ContractVersionLocator,
    GovernanceSparkWriteRequest,
    write_with_governance,
)

write_with_governance(
    df=orders_df,
    request=GovernanceSparkWriteRequest(
        context={
            "contract": {
                "contract_id": "sales.orders",
                "version_selector": ">=0.1.0",
            }
        },
        dataset_locator=ContractVersionLocator(dataset_version="latest"),
        mode="append",
    ),
    governance_service=governance_service,
    enforce=True,
    auto_cast=True,
)
```

Remote calls automatically forward the token you configured. You can continue using the Collibra stub for integration tests while
pointing staging and production pipelines at the shared backend.

## 5. Next steps

- Ask your ops partners for the health endpoint to integrate with monitoring (`/health` on the FastAPI service).
- Use the [contracts app helper](spark-contract-app-helper.md) to generate stubs for new pipelines.
