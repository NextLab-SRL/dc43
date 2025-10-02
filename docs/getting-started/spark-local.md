# Spark developers: run dc43 fully locally

Use this flow when you want to experiment with dc43 on a laptop, Databricks Repo, or CI runner without calling remote services.
Everything runs in-process so you can validate contracts before your platform team provisions shared governance backends.

## 1. Install the local extras

Create (or reuse) a Python environment for your Spark notebooks and install the local extras:

```bash
python -m venv .venv
source .venv/bin/activate
pip install "dc43-service-backends[local]"
pip install "dc43-integrations[spark]"
```

Working from a source checkout? Run `pip install -e .` from the repository root so notebooks pick up changes immediately.

## 2. Configure the embedded backends

The dc43 backends read their configuration from TOML. Copy the templates under [`docs/templates/`](../templates/) and adapt them
to your environment. A minimal filesystem-backed configuration looks like this:

```toml
# dc43-service-backends.toml
[contract_store]
type = "filesystem"
root = "~/contracts"

[data_quality]
type = "stub"

[auth]
token = ""  # leave empty when you embed everything in the same process
```

Point the loader at this file before you start orchestrations:

```python
from pathlib import Path
from dc43_service_backends.config import load_config
from dc43_service_backends.contracts import load_contract_store
from dc43_integrations.spark.contracts import SparkContractLoader

config = load_config(Path.home() / ".config/dc43/dc43-service-backends.toml")
contract_store = load_contract_store(config.contract_store)
loader = SparkContractLoader(spark, contract_store)
contract = loader.load_latest_version("sales.orders")
```

## 3. Run pipelines with the local store

You can now use helpers such as `write_with_contract` to validate schema alignment without an HTTP hop:

```python
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_integrations.spark.io import write_with_contract, ContractVersionLocator

service_client = LocalContractServiceClient(contract_store)

write_with_contract(
    df=orders_df,
    contract_id="sales.orders",
    contract_service=service_client,
    expected_contract_version=">=0.1.0",
    dataset_locator=ContractVersionLocator(dataset_version="latest"),
    mode="append",
    enforce=True,
    auto_cast=True,
)
```

Switch the `[contract_store]` section to `type = "collibra_stub"` when you want to emulate Collibra responses during unit tests.
The stub ships with the same contract resolution flow as the HTTP service, making it easy to swap the real backend later.

## 4. Next steps

- Move to the [remote service guide](spark-remote.md) when your platform team provides a shared backend.
- Explore the [component documentation](../component-contract-store.md) for deeper implementation details.
