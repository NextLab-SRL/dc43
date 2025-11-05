# Spark developers: generate stubs with the contracts app helper

The contracts app ships a visual "integration helper" that turns approved contracts into starter code for your pipelines. Follow
these steps to run the helper locally or against the shared governance service.

## 1. Install the contracts app

The helper lives in the `dc43-contracts-app` package. Install it alongside the Spark extras so generated snippets match the APIs
you use in notebooks:

```bash
python -m venv .venv
source .venv/bin/activate
pip install "dc43-contracts-app[spark]"
```

If you work from this repository run `pip install -e .[demo]` to install the demo dependencies and reuse editable sources.

## 2. Point the app at a backend

Choose one of the following configurations:

- **Use the shared service** – set the backend URL and token you received from the operations team:

  ```bash
  export DC43_CONTRACTS_APP_BACKEND_MODE="remote"
  export DC43_CONTRACTS_APP_BACKEND_URL="https://governance.example.com"
  export DC43_BACKEND_TOKEN="super-secret"
  ```

- **Run everything locally** – leave the defaults in place and the helper will spawn the embedded backend with an in-memory
  contract store. Populate it with sample contracts under `~/.config/dc43/contracts_app/contracts/` or point
  `DC43_CONTRACT_STORE` to an existing repository of ODCS documents.

## 3. Start the UI

You can run the helper directly from your virtual environment or rely on a
container published by the operations team.

### Option A – run from source

Launch the FastAPI app with Uvicorn:

```bash
uvicorn dc43_contracts_app.server:app --host 0.0.0.0 --port 8000
```

Visit `http://localhost:8000/integration-helper` in your browser. The landing
page shows your available datasets on the left and the integration helper on the
right. Contracts and governed data products now share the sidebar so you can
bind existing ports into the same lineage you use for raw contract feeds.

### Option B – run from a container

If your platform team published the contracts app image, pull it and point the
container at the governance backend:

```bash
docker run --rm \
  -p 8000:8000 \
  -e DC43_CONTRACTS_APP_BACKEND_URL="https://governance.example.com" \
  -e DC43_BACKEND_TOKEN="super-secret" \
  myregistry.azurecr.io/dc43/contracts-app:latest
```

Override `DC43_CONTRACTS_APP_BACKEND_MODE=embedded` and mount `/contracts` if
you need the container to spawn the backend locally.

### Inspect governance metrics

Once the UI is running, open any dataset or contract entry from the catalog to
review the metrics recorded by the governance service. The dataset view groups
observations by the status timestamp so you can see the latest snapshot alongside
earlier runs, while the contract overview highlights the same metrics filtered to
the active version. Numeric metrics now render as interactive charts, letting you
hover across the timeline to reveal dataset versions, contract revisions, and the
recorded values. This makes it easy to validate row counts, violation totals, and
other KPIs without querying the backend directly.

### Create or edit data products visually

The data products tab now includes **Add** and **Edit** buttons when a data
product backend is configured. Clicking either option launches an editor that
lets you manage the full definition—ID, status, descriptive metadata, input and
output ports, and custom properties—without hand-writing JSON. Contract and
dataset selectors pull from the live catalog so you can search for the correct
IDs instead of memorising version numbers, and the form automatically suggests
the next semantic version when you publish a draft. Save the form to persist the
definition through the same backend service used by your governance pipelines.
Draft suffixes such as ``-draft`` are handled automatically, so promoting a
provisional definition immediately bumps the base version without manual edits.

### Incorporate data products

The helper lists governed data products beneath the contract catalog. Choose a
product version to drop a node onto the canvas, then inspect its inputs and
outputs from the selection sidebar. Drag from a product’s output port into a
transformation input to read from the curated dataset, or route transformation
outputs into a product input port to model writes back into the governed layer.
Port metadata travels with the node so generated stubs include the correct
contract IDs, product identifiers, and dataset hints.

## 4. Generate a Spark stub

1. Select contracts and/or data products from the catalog tree.
2. Connect contract or product outputs into transformation inputs, then link transformation outputs back to contracts or product input ports.
3. For each transformation, choose the integration strategy (Spark batch, Delta Live Tables, streaming, ...).
4. Click **Generate stub**. The helper calls `/api/integration-helper/stub` to assemble a tailored Spark snippet.
5. Copy the highlighted code block and paste it into your notebook or repo. The snippet already imports
   the governance-first helpers (``read_with_governance``/``write_with_governance``), sets up the expected contract
   version, and includes TODO markers for business-specific logic.

You can switch the target language from the dropdown above the stub (for example to Python or SQL) and regenerate as often as
needed.

### Contract status guardrails

The generated snippet defaults to the production-safe policy of rejecting
non-active contracts (for example, drafts or deprecated versions). This aligns
with the integration tests shipped in `dc43_integrations` and matches the demo
pipeline behaviour. If you need to run development jobs against a draft
contract, configure the provided strategies directly in the stub:

```python
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
)
from dc43_integrations.spark.violation_strategy import NoOpWriteViolationStrategy
from dc43_service_clients.governance import GovernanceReadContext

read_status = DefaultReadStatusStrategy(allowed_contract_statuses=("active", "draft"))
write_strategy = NoOpWriteViolationStrategy(allowed_contract_statuses=("active", "draft"))

df, status = read_with_governance(
    spark,
    GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            contract={
                "contract_id": "orders_enriched",
                "version_selector": "==3.0.0",
            }
        )
    ),
    governance_service=governance_client,
    status_strategy=read_status,
    enforce=True,
)

write_with_governance(
    df=df,
    request=GovernanceSparkWriteRequest(
        context={
            "contract": {
                "contract_id": "orders_enriched",
                "contract_version": "3.0.0",
            }
        }
    ),
    governance_service=governance_client,
    violation_strategy=write_strategy,
    return_status=True,
)
```

Only relax the guardrails in non-production environments and capture the
override in your run metadata so downstream consumers know a draft contract was
used. The integration helper annotates the UI with the same defaults and points
to the relevant strategy options.

## 5. Keep everything in sync

- Refresh the page after the governance team publishes a new contract version so the helper pulls the latest schema.
- Re-run the helper whenever you change integration strategies—the generator adapts to streaming vs. batch choices.
- Pair this walkthrough with the [remote Spark guide](spark-remote.md) so the generated stub points at the correct backend.
