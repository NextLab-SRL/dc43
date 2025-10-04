from __future__ import annotations

from textwrap import dedent
from typing import Any, Dict

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
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": None,
            "contract_version": None,
            "dataset_name": "result-no-existing-contract",
            "run_type": "enforce",
        },
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
        "activate_versions": dict(_DEFAULT_SLICE),
        "params": {
            "contract_id": "orders_enriched",
            "contract_version": "1.0.0",
            "run_type": "enforce",
        },
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
    },
    "data-product-roundtrip": {
        "label": "Data product roundtrip",
        "category": "data-product",
        "description": (
            "<p>End-to-end orchestration that reads a published data product, stages an intermediate contract, "
            "and republishes the slice through a different data product output.</p>"
            "<ul>"
            "<li><strong>Inputs:</strong> Resolves the <code>dp.orders</code> <code>orders-latest</code> output port "
            "and looks up the latest customer dimensions.</li>"
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
                    DPIn["dp.orders orders-latest\ncontract orders:1.1.0"] --> JoinStage[Join with customers]
                    Customers["customers latest → 2024-01-01\ncontract customers:1.0.0"] --> JoinStage
                    JoinStage --> StageWrite["dp.analytics.stage «timestamp»\ncontract dp.analytics.stage:1.0.0"]
                    StageWrite --> StageRead[Read governed stage]
                    StageRead --> Publish["dp.analytics orders-enriched\nport orders-enriched"]
                    Publish --> Governance[Governance verdict recorded]
                """
            ).strip()
            + "</div>"
        ),
        "activate_versions": dict(_DEFAULT_SLICE),
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
                    "expected_contract_version": "==1.1.0",
                    "contract_version": "1.1.0",
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
    },
}

__all__ = ["SCENARIOS", "_DEFAULT_SLICE", "_INVALID_SLICE"]
