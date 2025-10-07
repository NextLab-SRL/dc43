from __future__ import annotations

from textwrap import dedent
from typing import Any, Dict
import html


def _section(title: str, body: str) -> dict[str, str]:
    """Create a guide section with normalised HTML content."""

    return {"title": title, "content": dedent(body).strip()}


def _code_section(title: str, code: str, lead: str | None = None) -> dict[str, str]:
    """Create a section that embeds a syntax-highlighted Python snippet."""

    snippet = html.escape(dedent(code).strip())
    parts: list[str] = []
    if lead:
        parts.append(dedent(lead).strip())
    parts.append(f"<pre><code class=\"language-python\">{snippet}</code></pre>")
    return {"title": title, "content": "\n\n".join(parts).strip()}

_DEFAULT_SLICE = {
    "orders": "2024-01-01",
    "customers": "2024-01-01",
}

_INVALID_SLICE = {
    "orders": "2025-09-28",
    "orders__valid": "2025-09-28",
    "orders__reject": "2025-09-28",
    "customers": "2024-01-01",
}

# Predefined pipeline scenarios exposed in the UI. Each scenario describes the
# parameters passed to the example pipeline along with a human readable
# description shown to the user.
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "no-contract": {
        "label": "No contract provided",
        "description": (
            "<p>Run the pipeline without supplying an output contract.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and "
            "<code>customers:1.0.0</code> with schema validation.</li>"
            "<li><strong>Contract:</strong> None provided, so no draft can be"
            " created.</li>"
            "<li><strong>Writes:</strong> Planned dataset <code>result-no-existing-contract</code>"
            " is blocked before any files are materialised, so no version is"
            " assigned.</li>"
            "<li><strong>Status:</strong> The run exits with an error because the contract is"
            " missing.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Write["Plan result-no-existing-contract\nno output contract"]
                    Write -->|no contract| Block[Run blocked, nothing written]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE, orders="2025-10-05"),
        "params": {
            "contract_id": None,
            "contract_version": None,
            "dataset_name": "result-no-existing-contract",
            "run_type": "enforce",
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  The pipeline is triggered without an output contract, so the
                  orchestration logic refuses to materialise a dataset. The
                  UI still walks the user through the planned inputs and
                  explains why no new slice is created. Use it to demonstrate
                  that contract registration is a first-class requirement, not
                  an optional check bolted on at the end.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Contract-aware pipelines must prevent accidental writes when
                  governance artefacts are missing. This scenario underlines:
                </p>
                <ul>
                  <li>The guard-rail that blocks the run before any files are
                      written.</li>
                  <li>How the status message communicates the failure back to
                      operators.</li>
                  <li>The relationship between planned datasets and contract
                      catalogues in DC43.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <p>
                  The example focuses on the enforcement workflow:
                </p>
                <ul>
                  <li><strong>Contract validation</strong> – enforcement mode
                      requires an active contract. Leaving the field empty is
                      treated as a hard error.</li>
                  <li><strong>Planning vs. materialisation</strong> – the
                      engine plans the write but cancels it before data is
                      committed, showcasing reversible planning.</li>
                  <li><strong>User feedback</strong> – the final status message
                      in the run log tells operators exactly what is missing.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>The job resolves the latest <code>orders</code> and
                      <code>customers</code> slices and prepares them for the
                      join.</li>
                  <li>When it reaches the publishing step it inspects the
                      target contract configuration and sees that none was
                      provided.</li>
                  <li>The runtime raises an error, records the failure reason,
                      and keeps the filesystem unchanged.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_no_contract(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Resolve inputs and fail fast when no output contract is supplied.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2025-10-05"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    _ = orders_df.join(customers_df, "customer_id", "left")

    target_contract = None  # Scenario intentionally omits the contract id.
    if not target_contract:
        raise RuntimeError("Output contract must be configured before publishing data.")
                """,
                "<p>The demo job stops before publishing because the contract id is missing. Guard the write step explicitly so orchestration halts before any files are materialised.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>During onboarding sessions to explain why contract
                      registration matters.</li>
                  <li>When troubleshooting unexpected "no contract" errors –
                      compare the metadata in this scenario with your run.</li>
                  <li>As a baseline before exploring
                      <a href="/pipeline-runs/ok">successful contract runs</a>
                      or
                      <a href="/pipeline-runs/contract-draft-block">status-related issues</a>.
                  </li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Trigger the scenario, observe that no dataset version is
                  produced, and then repeat the run after registering a
                  contract to see how the behaviour changes. The contrast with
                  the <a href="/pipeline-runs/ok">Existing contract OK</a>
                  walkthrough helps cement the happy-path checklist.
                </p>
                """,
            ),
        ],
    },
    "ok": {
        "label": "Existing contract OK",
        "description": (
            "<p>Happy path using contract <code>orders_enriched:1.0.0</code>.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> then aligns to the target schema.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:1.0.0</code>"
            " with no draft changes.</li>"
            "<li><strong>Writes:</strong> Persists dataset <code>orders_enriched</code>"
            " tagged with the run timestamp so repeated runs never collide.</li>"
            "<li><strong>Status:</strong> Post-write validation reports OK.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Validate[Align to contract orders_enriched:1.0.0]
                    Validate --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.0.0"]
                    Write --> Status[Run status: OK]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE, orders="2025-10-05"),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.0.0",
            "run_type": "enforce",
            "inputs": {
                "orders": {
                    "dataset_version": "2025-10-05__pinned",
                }
            },
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  This is the reference "green" pipeline execution. It takes
                  the curated <code>orders</code> and <code>customers</code>
                  slices, aligns them to the <code>orders_enriched</code>
                  contract, and publishes a timestamped version with no
                  warnings. Every other scenario can be compared against this
                  baseline.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Understanding the successful path helps new users recognise
                  how contract metadata, run configuration, and DQ feedback look
                  when everything lines up. It emphasises:
                </p>
                <ul>
                  <li>The relationship between input contract versions and the
                      chosen output contract.</li>
                  <li>How timestamped dataset versions avoid collisions between
                      reruns.</li>
                  <li>The governance signal that confirms enforcement finished
                      cleanly.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Schema alignment helpers</strong> – the pipeline
                      uses the demo transformations to match the
                      <code>orders_enriched</code> schema before persisting.</li>
                  <li><strong>Automatic version stamping</strong> – versions are
                      suffixed with the run timestamp, so multiple successful
                      runs remain visible in history.</li>
                  <li><strong>Governance handshake</strong> – enforcement mode
                      registers an OK verdict with the stub governance service.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Load the configured inputs and standardise the columns.</li>
                  <li>Apply enrichment logic (join customers, compute
                      aggregates) and conform to the target contract.</li>
                  <li>Write the governed dataset and record the validation
                      status in the workspace registry.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_orders_enriched_ok(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Execute the happy-path pipeline for orders_enriched:1.0.0.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2025-10-05__pinned"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")

    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=enriched_df,
        contract_id="orders_enriched",
        expected_contract_version="==1.0.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(
            dataset_id="orders_enriched",
            dataset_version=run_version,
        ),
        pipeline_context={"scenario": "existing-contract-ok"},
    )
    if result.status != "ok":
        raise RuntimeError(f"Validation failed: {result.details}")
                """,
                "<p>Replicate the successful run by resolving curated inputs, writing with <code>orders_enriched:1.0.0</code>, and checking the validation result before completing the job.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>As the starting tour for the demo – run this before any
                      other scenario to understand the layout of the results.</li>
                  <li>To validate that your local workspace is prepared: a
                      failure here usually signals misconfigured data or
                      missing demo assets.</li>
                  <li>To compare DQ payloads against
                      <a href="/pipeline-runs/dq">failing enforcement</a>
                      runs.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Inspect the generated dataset folder under
                  <code>orders_enriched/</code>, then open the corresponding DQ
                  record. After that, explore
                  <a href="/pipeline-runs/dq">Existing contract fails DQ</a> to
                  see how the same pipeline reacts when expectations fail.
                </p>
                """,
            ),
        ],
    },
    "dq": {
        "label": "Existing contract fails DQ",
        "description": (
            "<p>Demonstrates a data quality failure.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Validates against"
            " <code>orders_enriched:1.1.0</code> and prepares draft"
            " <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Writes:</strong> Persists"
            " <code>orders_enriched</code> with the run timestamp before"
            " governance flips the outcome to <code>block</code> and records"
            " draft <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Status:</strong> The enforcement run errors when rule"
            " <code>amount &gt; 100</code> is violated.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Post-write validation]
                    Governance --> Draft[Draft orders_enriched 1.2.0]
                    Governance -->|violations| Block["DQ verdict: block"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "collect_examples": True,
            "examples_limit": 3,
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  The pipeline honours the existing
                  <code>orders_enriched:1.1.0</code> contract, but the published
                  slice violates a rule (<code>amount &gt; 100</code>). The
                  governance client blocks the run, captures failure metadata,
                  and drafts the proposed <code>1.2.0</code> contract revision.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Real-world data pipelines must communicate when expectations
                  drift. This scenario highlights:
                </p>
                <ul>
                  <li>How enforcement surfaces failed expectations in the run
                      summary.</li>
                  <li>The automatic production of contract drafts documenting
                      proposed schema or rule updates.</li>
                  <li>The governance decision to block downstream consumers
                      until issues are resolved.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Expectation evaluation</strong> – the failed rule
                      lists the offending expression, counts, and sample rows.</li>
                  <li><strong>Draft creation</strong> – violations trigger the
                      creation of <code>orders_enriched:1.2.0</code>, showing the
                      incremental contract workflow.</li>
                  <li><strong>Run metadata</strong> – the <code>dq_details</code>
                      payload records auxiliary datasets and governance context.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Read and join input datasets exactly like the happy path.</li>
                  <li>Apply the same transformations but deliberately emit
                      values that break a quality rule.</li>
                  <li>Submit the slice to governance, which flips the outcome to
                      <code>block</code> and records violation examples.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_orders_enriched_dq_failure(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Intentionally break a quality rule to surface a blocking verdict.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")
    failing_df = enriched_df.withColumn("amount", F.col("amount") - F.lit(120))

    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=failing_df,
        contract_id="orders_enriched",
        expected_contract_version="==1.1.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        pipeline_context={"scenario": "dq"},
    )
    if result.status == "block":
        raise RuntimeError(f"DQ failure detected: {result.details}")
                """,
                "<p>The demo forces <code>amount</code> below the expectation to show how enforcement surfaces a blocking verdict and draft metadata.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>To explain how failed expectations are visualised in the
                      UI and surfaced through the API.</li>
                  <li>As a template for building alerting or incident response
                      workflows on top of DQ payloads.</li>
                  <li>When comparing lenient strategies such as
                      <a href="/pipeline-runs/split-lenient">Split invalid rows</a>
                      against strict enforcement.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Expand the accordion in the run history to inspect failure
                  examples, then open the draft contract in the contracts app.
                  Follow up with the
                  <a href="/pipeline-runs/schema-dq">Contract fails schema and DQ</a>
                  scenario to see combined drift handling.
                </p>
                """,
            ),
        ],
    },
    "schema-dq": {
        "label": "Contract fails schema and DQ",
        "description": (
            "<p>Shows combined schema and data quality issues.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:2.0.0</code>"
            " and proposes draft <code>orders_enriched:2.1.0</code>.</li>"
            "<li><strong>Writes:</strong> Persists"
            " <code>orders_enriched</code> with the run timestamp, then"
            " validation downgrades the outcome to <code>block</code> while"
            " recording draft <code>orders_enriched:2.1.0</code>.</li>"
            "<li><strong>Status:</strong> Schema drift plus failed expectations"
            " produce an error outcome.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Align[Schema align to contract orders_enriched:2.0.0]
                    Align --> Write["orders_enriched «timestamp»\ncontract orders_enriched:2.0.0"]
                    Write --> Governance[Post-write validation]
                    Governance --> Draft[Draft orders_enriched 2.1.0]
                    Governance -->|violations| Block["DQ verdict: block"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "2.0.0",
            "run_type": "enforce",
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  A tougher governance scenario where the pipeline targets
                  <code>orders_enriched:2.0.0</code>, but both schema alignment
                  and expectation checks fail. The run records schema drift and
                  data quality violations while drafting
                  <code>orders_enriched:2.1.0</code>.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Changes often land in batches – columns are added while rules
                  evolve. This scenario demonstrates how DC43 provides full
                  context for multi-dimensional failures:
                </p>
                <ul>
                  <li>Schema mismatches are listed alongside expectation
                      breaches.</li>
                  <li>The failure still creates a draft contract so data model
                      discussions start from the observed drift.</li>
                  <li>Downstream teams receive an explicit <code>block</code>
                      verdict, avoiding silent data corruption.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Schema alignment diagnostics</strong> – the run
                      summarises missing/extra columns and type conflicts.</li>
                  <li><strong>Combined DQ payload</strong> – schema errors and
                      failed expectations live in the same payload, making it
                      easier to triage.</li>
                  <li><strong>Draft propagation</strong> – the demo stores the
                      suggested contract changes for future approvals.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Produce an output that intentionally diverges from the
                      <code>2.0.0</code> schema.</li>
                  <li>Run enforcement, which flags the schema drift before
                      evaluating expectations.</li>
                  <li>Surface both failure sets to the operator and draft the
                      <code>2.1.0</code> contract for review.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_orders_enriched_schema_and_dq(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Trigger combined schema and DQ failures for orders_enriched:2.0.0.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")
    broken_df = (
        enriched_df.drop("customer_id")
        .withColumn("amount", F.col("amount") - F.lit(200))
    )

    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=broken_df,
        contract_id="orders_enriched",
        expected_contract_version="==2.0.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        pipeline_context={"scenario": "schema-dq"},
    )
    if result.status == "block":
        raise RuntimeError(f"Schema or DQ drift detected: {result.details}")
                """,
                "<p>The code intentionally drops <code>customer_id</code> and reduces <code>amount</code> so both schema alignment and quality checks fail under <code>orders_enriched:2.0.0</code>.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>To train data producers on how schema evolution is
                      reported and negotiated.</li>
                  <li>When demonstrating that DQ results remain available even
                      when schema checks fail first.</li>
                  <li>As a comparison point for
                      <a href="/pipeline-runs/dq">single-rule failures</a>.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Review the schema error list in the run history, then inspect
                  the generated draft contract. Follow up with
                  <a href="/pipeline-runs/contract-draft-block">Draft contract blocked</a>
                  to understand how contract status ties back into enforcement.
                </p>
                """,
            ),
        ],
    },
    "contract-draft-block": {
        "label": "Draft contract blocked",
        "description": (
            "<p>Highlights the default guardrails that reject non-active contracts.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> as usual.</li>"
            "<li><strong>Contract:</strong> Targets draft"
            " <code>orders_enriched:3.0.0</code>.</li>"
            "<li><strong>Writes:</strong> Aborted before materialising the dataset because"
            " the draft status is not allowed when enforcing.</li>"
            "<li><strong>Status:</strong> Run exits with an error explaining the"
            " contract status failure.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Draft["orders_enriched draft\ncontract orders_enriched:3.0.0"] --> Guard["contract status guard"]
                    Guard -->|status=draft| Block["Run blocked"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "3.0.0",
            "run_type": "enforce",
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  Even though <code>orders_enriched:3.0.0</code> exists, it is in
                  <em>draft</em> status. Enforcement mode refuses to use it and
                  aborts the run before materialising data, highlighting the
                  importance of contract status in change management.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Draft contracts should not be shipped to production without an
                  explicit review. This walkthrough demonstrates:
                </p>
                <ul>
                  <li>How the runtime inspects contract status metadata before
                      proceeding.</li>
                  <li>The clear error message that points at the offending
                      status.</li>
                  <li>Why change approval processes map neatly into contract
                      lifecycle states.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Status enforcement</strong> – the policy defaults
                      to <code>active</code>-only contracts in enforcement mode.</li>
                  <li><strong>Fast failure</strong> – the pipeline stops before
                      writing any files, keeping the workspace clean.</li>
                  <li><strong>Operator guidance</strong> – the recorded reason
                      explains which contract must be promoted or overridden.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>The pipeline looks up <code>orders_enriched:3.0.0</code>
                      from the contract store.</li>
                  <li>The guard checks the contract status, sees it is draft, and
                      raises a blocking error.</li>
                  <li>The failure is recorded along with the status metadata so
                      teams can request an approval or switch scenarios.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from dc43_service_clients.contracts.client.interface import ContractServiceClient


def enforce_active_contract_only(
    *,
    contract_service: ContractServiceClient,
) -> None:
    '''Abort publishing when the target contract remains in draft status.'''

    contract = contract_service.get("orders_enriched", "3.0.0")
    status = (getattr(contract, "status", "") or "").lower()
    if status != "active":
        raise RuntimeError(
            "orders_enriched:3.0.0 must be promoted to 'active' before enforcement",
        )
                """,
                "<p>Check the contract metadata up front – enforcement refuses to proceed while <code>orders_enriched:3.0.0</code> stays in <code>draft</code>.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>To discuss release management between platform and
                      analytics teams.</li>
                  <li>While documenting why production pipelines need explicit
                      overrides before consuming drafts.</li>
                  <li>As a precursor to the
                      <a href="/pipeline-runs/contract-draft-override">Allow draft contract</a>
                      scenario.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Compare the failure message here with the override strategy in
                  <a href="/pipeline-runs/contract-draft-override">Allow draft contract</a>.
                  The contrast shows how policy changes are intentional and
                  auditable.
                </p>
                """,
            ),
        ],
    },
    "contract-draft-override": {
        "label": "Allow draft contract",
        "description": (
            "<p>Demonstrates relaxing the guardrails when drafts are acceptable.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Curated"
            " <code>orders::valid latest__valid → 2025-09-28</code> alongside"
            " <code>customers:1.0.0</code>.</li>"
            "<li><strong>Contract:</strong> Uses draft"
            " <code>orders_enriched:3.0.0</code> but overrides the status policy to"
            " include drafts.</li>"
            "<li><strong>Writes:</strong> Persists"
            " <code>orders_enriched</code> with the run timestamp while boosting low"
            " amounts and stamping a placeholder <code>customer_segment</code> value.</li>"
            "<li><strong>Status:</strong> Run succeeds while recording the override in"
            " the run metadata.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Draft["orders_enriched draft\ncontract orders_enriched:3.0.0"] --> Override["status policy allows draft"]
                    Override --> Write["orders_enriched «timestamp»\ncontract orders_enriched:3.0.0"]
                    Write --> Status["Run status: OK"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "3.0.0",
            "run_type": "enforce",
            "violation_strategy": {
                "name": "default",
                "contract_status": {
                    "allowed_contract_statuses": ["active", "draft"],
                    "allow_missing_contract_status": False,
                },
            },
            "output_adjustment": "boost-amounts",
            "inputs": {
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  A deliberate override of the draft status policy. The run uses
                  <code>orders_enriched:3.0.0</code> despite it being a draft and
                  documents the policy change in the run metadata.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Sometimes teams need to test draft contracts in lower
                  environments or run controlled experiments. The scenario shows
                  how to do this responsibly:
                </p>
                <ul>
                  <li>Overrides are explicit and auditable in the run summary.</li>
                  <li>Inputs can point at curated datasets (the
                      <code>orders::valid</code> slice) so you can validate the
                      draft with safe data.</li>
                  <li>Governance metadata tracks that a non-default policy was
                      applied.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Status policy overrides</strong> –
                      <code>violation_strategy</code> declares the allowed status
                      set.</li>
                  <li><strong>Input substitution</strong> – the scenario swaps in
                      <code>orders::valid</code> to mirror real mitigation
                      tactics.</li>
                  <li><strong>Output adjustment</strong> – it uses the
                      <code>boost-amounts</code> helper to keep the data within
                      expectation thresholds.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Read the curated valid input slice and apply enrichment.</li>
                  <li>Adjust the amounts upward so quality rules still pass.</li>
                  <li>Write the dataset under the draft contract while recording
                      the override note.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_integrations.spark.violation_strategy import NoOpWriteViolationStrategy
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_allow_draft_contract(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Accept a draft contract while recording the override decision.'''

    status_strategy = DefaultReadStatusStrategy(
        allowed_contract_statuses=("active", "draft"),
        allow_missing_contract_status=False,
    )
    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(
            dataset_id="orders::valid",
            dataset_version="latest__valid",
        ),
        status_strategy=status_strategy,
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=status_strategy,
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")
    boosted_df = (
        enriched_df.withColumn("amount", F.col("amount") * F.lit(1.1))
        .withColumn(
            "customer_segment",
            F.coalesce(F.col("customer_segment"), F.lit("placeholder")),
        )
    )

    violation_strategy = NoOpWriteViolationStrategy(
        allowed_contract_statuses=("active", "draft"),
        allow_missing_contract_status=False,
    )
    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=boosted_df,
        contract_id="orders_enriched",
        expected_contract_version="==3.0.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        violation_strategy=violation_strategy,
        pipeline_context={"scenario": "contract-draft-override"},
    )
    if result.status != "ok":
        raise RuntimeError(f"Draft override failed: {result.details}")
                """,
                "<p>The override widens the allowed contract statuses to include <code>draft</code> and boosts amounts so the draft contract still satisfies the quality rules.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>When preparing sandboxes or pre-production rehearsals with
                      draft contracts.</li>
                  <li>To illustrate how override requests should be justified in
                      the run metadata.</li>
                  <li>As a complement to
                      <a href="/pipeline-runs/read-override-full">Force blocked slice</a>
                      for comparing override types.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Inspect the run metadata to see the stored override strategy
                  and follow up by promoting the draft contract in the contracts
                  app. Re-run the scenario to confirm it succeeds without the
                  override once the status flips to active.
                </p>
                """,
            ),
        ],
    },
    "read-invalid-block": {
        "label": "Invalid input blocked",
        "description": (
            "<p>Attempts to process the latest slice (→2025-09-28) flagged as invalid.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Governance records mark"
            " <code>orders latest → 2025-09-28</code> as <code>block</code> while pointing"
            " at curated <code>valid</code> and <code>reject</code> slices.</li>"
            "<li><strong>Contract:</strong> Targets <code>orders_enriched:1.1.0</code>"
            " but enforcement aborts before writes.</li>"
            "<li><strong>Outputs:</strong> None; the job fails fast.</li>"
            "<li><strong>Governance:</strong> Stub DQ client returns the stored"
            " `block` verdict and its auxiliary dataset hints.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Invalid["orders latest → 2025-09-28\ncontract orders:1.1.0\nDQ status: block"] -->|default enforcement| Halt[Read aborted]
                    Invalid -.-> Valid["orders::valid latest__valid → 2025-09-28\ncontract orders:1.1.0"]
                    Invalid -.-> Reject["orders::reject latest__reject → 2025-09-28\ncontract orders:1.1.0"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "inputs": {
                "orders": {
                    "dataset_version": "latest",
                }
            },
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  The governance layer marks the most recent
                  <code>orders</code> slice as <code>block</code>. The pipeline
                  honours that verdict and fails fast, highlighting the
                  safeguards around upstream data quality statuses.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Pipelines should respect domain teams' verdicts on input
                  readiness. This scenario explains:
                </p>
                <ul>
                  <li>How dataset-level DQ statuses influence read decisions.</li>
                  <li>The link between governance metadata and the read
                      strategies you configure.</li>
                  <li>The diagnostic hints pointing toward curated alternatives
                      (<code>valid</code> and <code>reject</code> slices).</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Input status enforcement</strong> – the default
                      read policy refuses blocked slices.</li>
                  <li><strong>Auxiliary dataset hints</strong> – DQ metadata lists
                      the curated subsets that remain available.</li>
                  <li><strong>Fast feedback</strong> – failure occurs before any
                      transformation costs accumulate.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>The runtime asks the governance client for the latest
                      <code>orders</code> slice verdict.</li>
                  <li>Because the status is <code>block</code>, the read strategy
                      prevents the dataset from being loaded.</li>
                  <li>The run exits with an error and lists the recommended
                      curated slices to consider instead.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def fail_on_blocked_input(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Raise immediately when the latest orders slice is blocked.'''

    try:
        read_from_contract(
            spark,
            contract_id="orders",
            contract_service=contract_service,
            expected_contract_version="1.1.0",
            data_quality_service=dq_service,
            dataset_locator=StaticDatasetLocator(dataset_version="latest"),
            status_strategy=DefaultReadStatusStrategy(),
            return_status=False,
        )
    except ValueError as exc:  # default enforcement rejects the slice
        raise RuntimeError(
            "orders latest → 2025-09-28 is blocked – switch to a curated subset",
        ) from exc
                """,
                "<p>The default read status strategy raises when the governance verdict is <code>block</code>, so the job aborts before any downstream processing occurs.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>During runbook discussions about how pipelines react to
                      blocked datasets.</li>
                  <li>When building automated remediation that switches to the
                      <a href="/pipeline-runs/read-valid-subset">valid subset</a>
                      scenario.</li>
                  <li>To show stakeholders what happens before manual overrides
                      are approved.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Compare this failure with
                  <a href="/pipeline-runs/read-override-full">Force blocked slice</a>
                  to see the documented override required to bypass the
                  governance verdict.
                </p>
                """,
            ),
        ],
    },
    "read-valid-subset": {
        "label": "Prefer valid subset",
        "description": (
            "<p>Steers reads toward the curated valid slice.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Uses <code>orders::valid</code>"
            " <code>latest__valid → 2025-09-28</code> alongside"
            " <code>customers latest → 2024-01-01</code> to satisfy governance.</li>"
            "<li><strong>Contract:</strong> Applies <code>orders_enriched:1.1.0</code>"
            " and keeps draft creation disabled.</li>"
            "<li><strong>Outputs:</strong> Writes <code>orders_enriched</code>"
            " stamped with the run timestamp under contract"
            " <code>orders_enriched:1.1.0</code> with a clean DQ verdict.</li>"
            "<li><strong>Governance:</strong> Stub evaluates post-write metrics"
            " and records an OK status.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Valid["orders::valid latest__valid → 2025-09-28\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Governance verdict ok]
                    Governance --> Status["DQ status: ok"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  The pipeline reacts to the blocked default slice by switching
                  to the curated <code>orders::valid</code> subset. It keeps the
                  run in observe mode while collecting violation examples.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Governance verdicts often come with remediation guidance. This
                  scenario demonstrates how to programmatically follow that
                  guidance:
                </p>
                <ul>
                  <li>Inputs can be re-pointed to curated subsets without
                      modifying the pipeline code.</li>
                  <li>Observe mode lets you experiment safely before promoting a
                      new contract version.</li>
                  <li>DQ payloads include examples to help teams confirm the
                      subset behaves as expected.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Input overrides</strong> – the scenario specifies
                      an alternate dataset identifier and version.</li>
                  <li><strong>Observation runs</strong> – no blocking enforcement
                      occurs, but full validation telemetry is recorded.</li>
                  <li><strong>Example collection</strong> – the run captures
                      failing and passing rows for quick inspection.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Read <code>orders::valid latest__valid</code> alongside the
                      standard customers slice.</li>
                  <li>Join, enrich, and align to the
                      <code>orders_enriched:1.1.0</code> contract.</li>
                  <li>Write the dataset and record the observe-mode governance
                      verdict.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_prefer_valid_subset(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Switch reads to the curated valid slice while running in observe mode.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(
            dataset_id="orders::valid",
            dataset_version="latest__valid",
        ),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")

    run_version = datetime.now(timezone.utc).isoformat()
    write_with_contract_id(
        df=enriched_df,
        contract_id="orders_enriched",
        expected_contract_version="==1.1.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        enforce=False,  # observe mode – do not raise on warnings
        pipeline_context={
            "mode": "observe",
            "collect_examples": True,
            "examples_limit": 3,
        },
    )
                """,
                "<p>Repoint the read locator to <code>orders::valid/latest__valid</code> and run in observe mode so the pipeline records telemetry without blocking downstream tasks.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>As part of playbooks for reacting to blocked inputs.</li>
                  <li>To showcase how curated subsets keep data products running
                      while remediation happens.</li>
                  <li>To compare with
                      <a href="/pipeline-runs/read-valid-subset-violation">Valid subset, invalid output</a>
                      where the output still fails.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Inspect the DQ payload to review collected examples, then
                  rerun in enforcement mode by toggling the run type to understand
                  the trade-offs between observe and enforce.
                </p>
                """,
            ),
        ],
    },
    "read-valid-subset-violation": {
        "label": "Valid subset, invalid output",
        "description": (
            "<p>Highlights when clean inputs still breach the output contract.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Same curated"
            " <code>orders::valid</code> <code>latest__valid → 2025-09-28</code> slice.</li>"
            "<li><strong>Contract:</strong> Writes to"
            " <code>orders_enriched</code> under <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Outputs:</strong> Produces <code>orders_enriched</code>"
            " (timestamped under contract <code>1.1.0</code>) but post-write checks fail because"
            " the demo purposely lowers one amount below the"
            " <code>&gt; 100</code> expectation.</li>"
            "<li><strong>Governance:</strong> Stub DQ client records a blocking"
            " verdict and drafts <code>orders_enriched:1.2.0</code>.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Valid["orders::valid latest__valid → 2025-09-28\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Join --> Adjust[Lower amount to 60]
                    Adjust --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Governance verdict block]
                    Governance --> Draft["Draft orders_enriched 1.2.0"]
                    Governance --> Status["DQ status: block"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "enforce",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
            "output_adjustment": "valid-subset-violation",
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  Even with curated <code>orders::valid</code> inputs, the
                  pipeline can still create an output that fails enforcement. A
                  deliberate transformation lowers an amount so the DQ rule is
                  breached.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Clean inputs do not guarantee compliant outputs. This run
                  illustrates:
                </p>
                <ul>
                  <li>How transformation logic itself can introduce
                      regressions.</li>
                  <li>The value of post-write validation even when upstream data
                      is trusted.</li>
                  <li>Draft creation for output-side issues so contract updates
                      remain traceable.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Custom output adjustments</strong> – the scenario
                      uses the <code>valid-subset-violation</code> helper to
                      simulate a bug.</li>
                  <li><strong>Post-write governance</strong> – the failure is
                      caught after the dataset is written.</li>
                  <li><strong>Draft propagation</strong> – enforcement proposes
                      <code>orders_enriched:1.2.0</code> just like the strict DQ
                      scenario.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Read the curated subset and apply the usual enrichments.</li>
                  <li>Intentionally degrade one amount to 60 via the adjustment
                      hook.</li>
                  <li>Write the slice, run validation, and record the blocking
                      verdict along with draft metadata.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_valid_subset_violation(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Demonstrate that clean inputs can still produce a blocking output.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(
            dataset_id="orders::valid",
            dataset_version="latest__valid",
        ),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")
    broken_df = enriched_df.withColumn(
        "amount",
        F.when(F.col("order_id") == F.lit(1), F.lit(60.0)).otherwise(F.col("amount")),
    )

    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=broken_df,
        contract_id="orders_enriched",
        expected_contract_version="==1.1.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        pipeline_context={"scenario": "read-valid-subset-violation"},
    )
    if result.status == "block":
        raise RuntimeError(f"Post-write validation failed: {result.details}")
                """,
                "<p>Even though the pipeline reads <code>orders::valid/latest__valid</code>, lowering one amount to 60 triggers a blocking verdict and drafts <code>orders_enriched:1.2.0</code>.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>When teaching teams to debug pipeline logic errors.</li>
                  <li>To motivate unit tests or assertions around transformation
                      code.</li>
                  <li>To contrast with
                      <a href="/pipeline-runs/read-valid-subset">Prefer valid subset</a>,
                      which succeeds.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Open the DQ payload to review the captured examples, then fix
                  the adjustment helper (set it to
                  <code>boost-amounts</code>) and rerun to validate your change.
                </p>
                """,
            ),
        ],
    },
    "data-product-roundtrip": {
        "label": "Data product roundtrip",
        "category": "data-product",
        "description": (
            "<p>End-to-end orchestration that reads a published data product, stages an intermediate contract, "
            "and republishes the slice through a different data product output.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Resolves the <code>dp.orders</code> <code>orders-latest</code> output port "
            "(contract <code>orders:1.2.0</code>) and looks up the latest customer dimensions.</li>"
            "<li><strong>Intermediate:</strong> Persists the joined dataset under contract "
            "<code>dp.analytics.stage:1.0.0</code> so downstream steps can re-read a governed representation.</li>"
            "<li><strong>Outputs:</strong> Writes to the <code>dp.analytics</code> <code>orders-enriched</code> port, "
            "capturing registration metadata and validation runs in the registry.</li>"
            "<li><strong>Status:</strong> Enforcement keeps the staging artefacts and the published output in lockstep, "
            "highlighting how contract-only helpers complement data product bindings.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    DPIn["dp.orders orders-latest\ncontract orders:1.2.0"] --> JoinStage[Join with customers]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> JoinStage
                    JoinStage --> StageWrite["dp.analytics.stage «timestamp»\ncontract dp.analytics.stage:1.0.0"]
                    StageWrite --> StageRead[Read governed stage]
                    StageRead --> Publish["dp.analytics orders-enriched\nport orders-enriched"]
                    Publish --> Governance[Governance verdict recorded]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE, orders="2025-10-05"),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "dataset_name": "orders_enriched",
            "run_type": "enforce",
            "data_product_flow": {
                "input": {
                    "binding": {
                        "data_product": "dp.orders",
                        "port_name": "orders-latest",
                        "source_data_product": "dp.orders",
                        "source_output_port": "orders-latest",
                    },
                    "dataset_version": "latest",
                    "expected_contract_version": "==1.2.0",
                    "contract_version": "1.2.0",
                    "dataset_id": "orders",
                },
                "customers": {
                    "contract_id": "customers",
                    "expected_contract_version": "==1.0.0",
                    "contract_version": "1.0.0",
                },
                "intermediate_contract": {
                    "contract_id": "dp.analytics.stage",
                    "expected_contract_version": "==1.0.0",
                    "contract_version": "1.0.0",
                    "dataset_name": "dp.analytics.stage",
                },
                "output": {
                    "data_product": "dp.analytics",
                    "port_name": "orders-enriched",
                    "contract_id": "orders_enriched",
                    "expected_contract_version": "==1.1.0",
                    "contract_version": "1.1.0",
                    "dataset_name": "orders_enriched",
                },
                "output_adjustment": "boost-amounts",
            },
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  A complete data product lifecycle: consume a published port,
                  stage governed intermediates, and publish a new port under the
                  analytics data product. It demonstrates how contracts and data
                  products interact.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Data products often depend on each other. This scenario shows
                  how DC43 provides consistent governance signals across that
                  dependency chain:
                </p>
                <ul>
                  <li>Input resolution uses the registry binding to ensure the
                      right port and contract combination.</li>
                  <li>An intermediate governed dataset (<code>dp.analytics.stage</code>)
                      keeps transformations transparent.</li>
                  <li>The published output records metadata back into the data
                      product catalogue.
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Data product bindings</strong> – the configuration
                      maps ports to concrete dataset versions.</li>
                  <li><strong>Multi-contract enforcement</strong> – the run
                      validates both the stage contract and the final output.</li>
                  <li><strong>Governance metadata</strong> – the DQ payload shows
                      how data product identifiers flow through validation.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Resolve <code>dp.orders/orders-latest</code> via the data
                      product registry.</li>
                  <li>Join with customers, persist the stage dataset under its
                      own contract, then re-read it.</li>
                  <li>Publish the <code>dp.analytics</code> output port and store
                      the validation outcome.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_data_product,
    read_with_contract,
    write_with_contract,
    write_to_data_product,
)
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_products import DataProductServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_data_product_roundtrip(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    data_products: DataProductServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Consume a published port, persist a governed stage, and publish a new port.'''

    orders_df = read_from_data_product(
        spark,
        data_product_service=data_products,
        data_product_input={
            "data_product": "dp.orders",
            "port_name": "orders-latest",
            "source_data_product": "dp.orders",
            "source_output_port": "orders-latest",
        },
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="latest"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_with_contract(
        spark,
        contract_id="customers",
        expected_contract_version="==1.0.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")

    run_version = datetime.now(timezone.utc).isoformat()
    stage_result = write_with_contract(
        df=enriched_df,
        contract_id="dp.analytics.stage",
        expected_contract_version="==1.0.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        pipeline_context={"step": "stage"},
    )
    if stage_result.status != "ok":
        raise RuntimeError(f"Stage validation failed: {stage_result.details}")

    stage_df = read_with_contract(
        spark,
        contract_id="dp.analytics.stage",
        expected_contract_version="==1.0.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        return_status=False,
    )

    publish_result = write_to_data_product(
        df=stage_df,
        data_product_service=data_products,
        data_product_output={
            "data_product": "dp.analytics",
            "port_name": "orders-enriched",
        },
        contract_id="orders_enriched",
        contract_service=contract_service,
        expected_contract_version="==1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        pipeline_context={"step": "publish"},
    )
    if publish_result.status != "ok":
        raise RuntimeError(f"Publish validation failed: {publish_result.details}")
                """,
                "<p>The end-to-end flow resolves the upstream data product binding, persists a governed stage contract, then republishes through the analytics data product while checking both validation results.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>To demonstrate cross-product dependencies during platform
                      walkthroughs.</li>
                  <li>When designing governance workflows for federated teams.</li>
                  <li>As a template for roundtrip orchestration that keeps stage
                      artefacts governed.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Inspect the data product metadata recorded in the run history
                  and follow the links to the contracts and data product detail
                  pages. Experiment by changing the expected contract versions to
                  see how strict bindings protect consumers.
                </p>
                """,
            ),
        ],
    },
    "read-override-full": {
        "label": "Force blocked slice (manual override)",
        "description": (
            "<p>Documents what happens when the blocked data is forced through.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reuses the blocked"
            " <code>orders latest → 2025-09-28</code> and downgrades the read status to"
            " <code>warn</code>.</li>"
            "<li><strong>Override strategy:</strong> Uses"
            " <code>allow-block</code> to document that the blocked slice was"
            " manually forced through despite the governance verdict.</li>"
            "<li><strong>Contract:</strong> Applies"
            " <code>orders_enriched:1.1.0</code> and captures draft"
            " <code>orders_enriched:1.2.0</code>.</li>"
            "<li><strong>Outputs:</strong> Writes <code>orders_enriched</code>"
            " (timestamped under contract <code>1.1.0</code>) while surfacing the manual override"
            " note alongside the reject-row metrics.</li>"
            "<li><strong>Governance:</strong> Stub records the downgrade in the"
            " run summary alongside violation counts and the explicit override"
            " note.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Invalid["orders latest → 2025-09-28\ncontract orders:1.1.0\nDQ status: block"] --> Override[Downgrade to warn]
                    Override --> Write["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Write --> Governance[Governance verdict warn]
                    Governance --> Draft["Draft orders_enriched 1.2.0"]
                    Governance --> Status["DQ status: warn"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_INVALID_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "inputs": {
                "orders": {
                    "dataset_version": "latest",
                    "status_strategy": {
                        "name": "allow-block",
                        "note": "Manual override: forced latest slice (→2025-09-28)",
                        "target_status": "warn",
                    },
                }
            },
            "output_adjustment": "amplify-negative",
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  A manual override that downgrades a blocked input slice to
                  <code>warn</code>. The pipeline proceeds but records a warning
                  verdict and the override note.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Occasionally teams must ship despite governance blocks. This
                  scenario illustrates the safeguards around such decisions:
                </p>
                <ul>
                  <li>Overrides require an explicit note that documents the
                      rationale.</li>
                  <li>Governance status is downgraded, not cleared, so consumers
                      understand the residual risk.</li>
                  <li>Validation still runs, capturing draft metadata for
                      follow-up work.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Status strategy overrides</strong> – the
                      <code>allow-block</code> strategy forces the read while
                      recording the decision.</li>
                  <li><strong>Observe mode</strong> – chosen to avoid double
                      enforcement while still recording warnings.</li>
                  <li><strong>Example capture</strong> – keeps traces of
                      problematic rows for future triage.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Request the latest <code>orders</code> slice even though it
                      is blocked.</li>
                  <li>Apply the override policy to downgrade the verdict to
                      <code>warn</code>.</li>
                  <li>Run the pipeline, which writes the dataset, records
                      warnings, and keeps the override note in metadata.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from dataclasses import dataclass
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    ReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


@dataclass
class DowngradeBlockingReadStrategy(ReadStatusStrategy):
    '''Custom read strategy that downgrades blocking verdicts to a warning.'''

    note: str
    target_status: str = "warn"

    def apply(self, *, dataframe, status, enforce, context):  # type: ignore[override]
        if status and status.status == "block":
            details = dict(status.details)
            overrides = list(details.get("overrides", []))
            overrides.append(self.note)
            details["overrides"] = overrides
            details["status_before_override"] = status.status
            return dataframe, ValidationResult(
                status=self.target_status,
                reason=status.reason,
                details=details,
            )
        return dataframe, status


def run_force_blocked_slice(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Force a blocked slice through the pipeline while recording the override.'''

    override_strategy = DowngradeBlockingReadStrategy(
        note="Manual override: forced latest slice (→2025-09-28)",
        target_status="warn",
    )
    orders_df, orders_status = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="latest"),
        status_strategy=override_strategy,
        return_status=True,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")

    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=enriched_df,
        contract_id="orders_enriched",
        expected_contract_version="==1.1.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        enforce=False,
        pipeline_context={
            "override_note": override_strategy.note,
            "status_before_override": orders_status.status if orders_status else None,
        },
    )
    if result.status not in {"ok", "warn"}:
        raise RuntimeError(f"Override run failed: {result.details}")
                """,
                "<p>Implement a small read-status strategy that downgrades <code>block</code> to <code>warn</code> while annotating the governance payload so the manual override is fully auditable.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>To train operators on the governance exceptions process.</li>
                  <li>When designing approval flows that require justification
                      before forcing blocked data.</li>
                  <li>To compare with
                      <a href="/pipeline-runs/read-invalid-block">Invalid input blocked</a>
                      (no override) and
                      <a href="/pipeline-runs/contract-draft-override">Allow draft contract</a>
                      (different override type).</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Review the recorded override note in the run history, then
                  remove the override configuration and rerun to observe the hard
                  failure. This emphasises why overrides should be temporary.
                </p>
                """,
            ),
        ],
    },
    "split-lenient": {
        "label": "Split invalid rows",
        "description": (
            "<p>Routes violations to dedicated datasets using the split strategy.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Reads <code>orders:1.1.0</code> and"
            " <code>customers:1.0.0</code> before aligning to"
            " <code>orders_enriched:1.1.0</code>.</li>"
            "<li><strong>Contract:</strong> Validates against"
            " <code>orders_enriched:1.1.0</code> and stores draft"
            " <code>orders_enriched:1.2.0</code> when rejects exist.</li>"
            "<li><strong>Writes:</strong> Persists three datasets sharing the same"
            " timestamp: the contracted"
            " <code>orders_enriched</code> (full slice),"
            " <code>orders_enriched::valid</code>, and"
            " <code>orders_enriched::reject</code>.</li>"
            "<li><strong>Status:</strong> Run finishes with a warning because"
            " validation finds violations, and the UI links the auxiliary"
            " datasets.</li>"
            "</ul>"
        ),
        "diagram": (
            "<div class=\"mermaid\">"
            + dedent(
                """
                flowchart TD
                    Orders["orders latest → 2024-01-01\ncontract orders:1.1.0"] --> Join[Join datasets]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> Join
                    Join --> Validate[Validate contract orders_enriched:1.1.0]
                    Validate --> Strategy[Split strategy]
                    Strategy --> Full["orders_enriched «timestamp»\ncontract orders_enriched:1.1.0"]
                    Strategy --> Valid["orders_enriched::valid «timestamp»\ncontract orders_enriched:1.1.0"]
                    Strategy --> Reject["orders_enriched::reject «timestamp»\ncontract orders_enriched:1.1.0"]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.1.0",
            "run_type": "observe",
            "collect_examples": True,
            "examples_limit": 3,
            "violation_strategy": {
                "name": "split",
                "include_valid": True,
                "include_reject": True,
                "write_primary_on_violation": True,
            },
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  A lenient enforcement mode that keeps the main dataset while
                  routing invalid rows to auxiliary outputs
                  (<code>::valid</code> and <code>::reject</code>). The run ends
                  with a warning instead of a block.
                </p>
                """,
            ),
            _section(
                "Why it matters",
                """
                <p>
                  Not every data quality issue justifies halting the pipeline.
                  This scenario demonstrates a middle ground:
                </p>
                <ul>
                  <li>Consumers can continue using the main dataset with full
                      awareness of issues.</li>
                  <li>Curated auxiliary datasets capture clean and problematic
                      rows for downstream remediation.</li>
                  <li>The run status is <code>warn</code>, signalling attention
                      is needed without discarding all results.</li>
                </ul>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li><strong>Split violation strategy</strong> – demonstrates the
                      configuration knobs for lenient handling.</li>
                  <li><strong>Auxiliary dataset registration</strong> – links to
                      the generated <code>::valid</code> and <code>::reject</code>
                      slices are recorded in the run metadata.</li>
                  <li><strong>Governance signalling</strong> – the warning status
                      keeps monitoring tooling informed.</li>
                </ul>
                """,
            ),
            _section(
                "How it works",
                """
                <ol>
                  <li>Read and align inputs exactly like the standard
                      enforcement run.</li>
                  <li>Apply the split strategy so violations are written to
                      dedicated datasets.</li>
                  <li>Record the warning verdict and auxiliary dataset metadata
                      for traceability.</li>
                </ol>
                """,
            ),
            _code_section(
                "Pipeline example",
                """
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from dc43_integrations.spark.io import (
    DefaultReadStatusStrategy,
    StaticDatasetLocator,
    read_from_contract,
    write_with_contract_id,
)
from dc43_integrations.spark.violation_strategy import SplitWriteViolationStrategy
from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import (
    DataQualityServiceClient,
)


def run_split_invalid_rows(
    spark: SparkSession,
    *,
    contract_service: ContractServiceClient,
    dq_service: DataQualityServiceClient,
) -> None:
    '''Write primary, valid, and reject datasets when violations occur.'''

    orders_df = read_from_contract(
        spark,
        contract_id="orders",
        contract_service=contract_service,
        expected_contract_version="1.1.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    customers_df = read_from_contract(
        spark,
        contract_id="customers",
        contract_service=contract_service,
        expected_contract_version="1.0.0",
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version="2024-01-01"),
        status_strategy=DefaultReadStatusStrategy(),
        return_status=False,
    )
    enriched_df = orders_df.join(customers_df, "customer_id", "left")

    strategy = SplitWriteViolationStrategy(
        include_valid=True,
        include_reject=True,
        write_primary_on_violation=True,
    )
    run_version = datetime.now(timezone.utc).isoformat()
    result = write_with_contract_id(
        df=enriched_df,
        contract_id="orders_enriched",
        expected_contract_version="==1.1.0",
        contract_service=contract_service,
        data_quality_service=dq_service,
        dataset_locator=StaticDatasetLocator(dataset_version=run_version),
        violation_strategy=strategy,
        enforce=False,
        pipeline_context={"strategy": "split"},
    )
    if result.status not in {"ok", "warn"}:
        raise RuntimeError(f"Split strategy run failed: {result.details}")
                """,
                "<p>The split strategy keeps the contracted dataset while emitting <code>::valid</code> and <code>::reject</code> companions so remediation teams can analyse each portion independently.</p>",
            ),
            _section(
                "When to use it",
                """
                <ul>
                  <li>To discuss service-level agreements where partial delivery
                      is acceptable.</li>
                  <li>When designing pipelines that feed quarantine workflows.</li>
                  <li>As a contrast to
                      <a href="/pipeline-runs/dq">strict enforcement</a>, which
                      blocks the run outright.</li>
                </ul>
                """,
            ),
            _section(
                "What to explore next",
                """
                <p>
                  Inspect the generated auxiliary datasets in the workspace and
                  use them to rehearse remediation strategies. Consider switching
                  <code>include_valid</code> to <code>false</code> to observe how
                  the outputs change.
                </p>
                """,
            ),
        ],
    },
    "streaming-valid": {
        "label": "Streaming: healthy pipeline",
        "category": "streaming",
        "description": (
            "<p>Run a continuous integration-style job that keeps <code>demo.streaming.events_processed</code>"
            " in sync with the synthetic <code>demo.streaming.events</code> feed.</p>"
            "<ul>"
            "<li><strong>Source:</strong> <code>demo.streaming.events</code> (0.1.0) emits 6 timestamp/value rows per second</li>"
            "<li><strong>Processing:</strong> adds a constant <code>quality_flag='valid'</code> and validates every micro-batch</li>"
            "<li><strong>Output:</strong> <code>demo.streaming.events_processed</code> (0.1.0) records a fresh dataset version with metrics</li>"
            "<li><strong>Run length:</strong> roughly eight seconds so at least one non-empty batch is observed</li>"
            "</ul>"
        ),
        "params": {
            "mode": "streaming",
            "seconds": 8,
            "dataset_name": "demo.streaming.events_processed",
            "contract_id": "demo.streaming.events_processed",
            "contract_version": "0.1.0",
            "run_type": "observe",
        },
        "guide": [
            _section(
                "What this example shows",
                """
                <p>
                  The scenario reads from <code>demo.streaming.events</code>,
                  writes to the processed contract, and lets the observation
                  writer collect contract metrics for every micro-batch. The
                  returned validation exposes the dataset version and the
                  <code>StreamingQuery</code> handles that produced it.
                </p>
                """,
            ),
            _section(
                "Feature focus",
                """
                <ul>
                  <li>Contract-aware <code>readStream</code> with schema checks
                      and governance registration.</li>
                  <li>Streaming observation writer feeding contract metrics
                      without blocking the sink.</li>
                  <li>Dataset versions surfaced in the governance response so
                      downstream systems can poll status for each batch.</li>
                  <li>Timeline visualisation summarising the streaming source,
                      validation heartbeat, and captured metrics.</li>
                </ul>
                """,
            ),
            _code_section(
                "Run it programmatically",
                """
from dc43_demo_app.streaming import run_streaming_scenario

dataset, version = run_streaming_scenario(
    "streaming-valid",
    seconds=8,
    run_type="observe",
)
print(f"published {dataset}@{version}")
                """,
            ),
        ],
    },
    "streaming-dq-rejects": {
        "label": "Streaming: rejects without blocking",
        "category": "streaming",
        "description": (
            "<p>Stress the validators by flipping every fourth row negative so governance tracks warnings while the "
            "pipeline keeps publishing.</p>"
            "<ul>"
            "<li><strong>Source:</strong> <code>demo.streaming.events</code> (0.1.0) keeps emitting 6 rows per second</li>"
            "<li><strong>Processing:</strong> negative values are labelled <code>quality_flag='warning'</code> and routed to rejects</li>"
            "<li><strong>Outputs:</strong> <code>demo.streaming.events_processed</code> (warn) plus <code>demo.streaming.events_rejects</code> with reasons</li>"
            "<li><strong>Run length:</strong> around eight seconds to capture at least one violating batch</li>"
            "</ul>"
        ),
        "params": {
            "mode": "streaming",
            "seconds": 8,
            "dataset_name": "demo.streaming.events_processed",
            "contract_id": "demo.streaming.events_processed",
            "contract_version": "0.1.0",
            "run_type": "observe",
        },
        "guide": [
            _section(
                "Why it matters",
                """
                <p>
                  Even when enforcement is relaxed, the helpers keep posting
                  observations so governance can highlight the broken batches.
                  Meanwhile the reject sink captures the failed rows for
                  remediation.
                </p>
                """,
            ),
            _section(
                "What to look at",
                """
                <ul>
                  <li>The processed dataset carries a <code>warn</code> status
                      with violation metrics.</li>
                  <li>The reject dataset lands under
                      <code>demo.streaming.events_rejects</code> with a reason
                      column.</li>
                  <li>The validation payload summarises both sinks so operators
                      see where the rows went.</li>
                  <li>The scenario timeline highlights when the rejects kicked
                      in and how many rows were quarantined.</li>
                </ul>
                """,
            ),
            _code_section(
                "Route rejects programmatically",
                """
from dc43_demo_app.streaming import run_streaming_scenario

dataset, version = run_streaming_scenario(
    "streaming-dq-rejects",
    seconds=8,
    run_type="observe",
)
print(f"latest governed run: {dataset}@{version}")
                """,
            ),
        ],
    },
    "streaming-schema-break": {
        "label": "Streaming: schema break blocks the run",
        "category": "streaming",
        "description": (
            "<p>Simulate schema drift by removing the <code>value</code> column so the streaming helper blocks the publish.</p>"
            "<ul>"
            "<li><strong>Source:</strong> <code>demo.streaming.events</code> (0.1.0) still delivers timestamp/value pairs</li>"
            "<li><strong>Processing:</strong> the transformation drops <code>value</code>, violating the processed contract</li>"
            "<li><strong>Outcome:</strong> validation raises an error immediately and no dataset version is recorded</li>"
            "<li><strong>Run length:</strong> a short three-second burst to showcase the failure</li>"
            "</ul>"
        ),
        "params": {
            "mode": "streaming",
            "seconds": 3,
            "dataset_name": "demo.streaming.events_processed",
            "contract_id": "demo.streaming.events_processed",
            "contract_version": "0.1.0",
            "run_type": "enforce",
        },
        "guide": [
            _section(
                "Key takeaways",
                """
                <ul>
                  <li>Contract alignment happens before the query starts, so the
                      failure surfaces instantly.</li>
                  <li>No dataset version is recorded, keeping the catalogue in
                      sync with the actual materialised data.</li>
                  <li>The dataset record captures the failure reason so the UI
                      can highlight the drift.</li>
                </ul>
                """,
            ),
            _code_section(
                "Expect a failure",
                """
from dc43_demo_app.streaming import run_streaming_scenario

dataset, version = run_streaming_scenario(
    "streaming-schema-break",
    seconds=3,
    run_type="enforce",
)
print(f"stream blocked: {dataset}@{version or '<no version>'}")
                """,
                "<p>The demo app records the error in the dataset registry so the "
                "details page explains why nothing new was published.</p>",
            ),
        ],
    },
}

__all__ = ["SCENARIOS", "_DEFAULT_SLICE", "_INVALID_SLICE"]
