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
bind existing ports into the same lineage you use for raw contract feeds. The
sidebar scrolls independently, keeping long contract or product lists visible,
and you can drag either card straight into the canvas to position new nodes.

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
review the metrics recorded by the governance service. The dataset overview page
now renders the metric trend chart at the dataset level, plotting every recorded
version while keeping the history table sorted (and re-sortable) by dataset
version, contract, and contract version. Numeric metrics render as interactive
charts, letting you hover across the timeline to reveal dataset versions,
contract revisions, and the recorded values without querying the backend
directly. Each dataset record lists an observation scope (for example, “Pre-write
dataframe snapshot” or “Governed read snapshot”) so you can tell whether the
metrics reflect the dataframe evaluated before a write, a streaming micro-batch,
or a governed read. Use the scope badge to separate slice-level validations from
full dataset verdicts when investigating unexpected counts. Older runs that
predate the metadata emit a neutral “Snapshot” badge so the catalog still renders
even when governance stores have not populated the scope fields yet.

Individual dataset version pages now focus on the selected run only, leaning on
the governance service’s batched status lookups instead of rebuilding the full
compatibility matrix. Opening a single dataset version therefore issues one
targeted activity request plus a compact metrics query, keeping the UI snappy
even when governance stores track hundreds of historical versions.

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
product version and either click **Add to pipeline** or drag the card directly
onto the canvas using the handle at the top of each result to place a node, then
grab the header handle on any contract or product card to reposition it as the
flow evolves. When a product already exposes governed ports the helper now drops
the referenced contracts alongside the node and inserts dedicated transformations
that link each input contract through its product port to the published outputs.
You can delete or rewire those auto-generated connections as needed, but they
give you a ready-made context for the product’s lineage. Inspect its inputs and
outputs from the selection sidebar. Drag from a product’s output port into a
transformation input to read from the curated dataset, or route transformation
outputs into a product input port to model writes back into the governed layer.
Each data product node now exposes **Add output** and **Add input** controls for
proposing new ports, and the selection panel mirrors those buttons so you can add
bindings without leaving the sidebar. When you confirm a new port name the
helper shows a reminder that a new product version must be published, then holds
code generation until that release is available. Port metadata travels with the
node so generated stubs include the correct contract IDs, product identifiers,
and dataset hints once the bindings are published.

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
