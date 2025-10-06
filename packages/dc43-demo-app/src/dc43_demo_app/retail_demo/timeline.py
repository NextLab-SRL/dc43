"""Timeline simulation helpers for the Altair Retail demo."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping

from .data import RETAIL_DATASETS
from .pipeline import RetailDemoRun


@dataclass(slots=True)
class TimelineStep:
    """Structured description of a pipeline step in the walkthrough."""

    label: str
    message: str
    status: str
    status_label: str
    duration_ms: int = 1400
    rule: Mapping[str, str] | None = None
    outputs: Mapping[str, Any] | None = None


def _timeline_insight(label: str, value: str, description: str | None = None) -> dict[str, str]:
    """Compose a structured insight for the timeline detail panel."""

    data = {"label": label, "value": value}
    if description:
        data["description"] = description
    return data


def _timeline_link_for_dataset(identifier: str, label: str | None = None) -> dict[str, str]:
    """Return a link dictionary that targets a dataset locally and in the catalog."""

    dataset_label = label or f"{identifier} dataset"
    return {
        "label": dataset_label,
        "href": f"/datasets/{identifier}",
        "anchor": f"#dataset-{identifier}",
    }


def _timeline_link_for_product(identifier: str, label: str | None = None) -> dict[str, str]:
    """Return a link dictionary that points at a data product record."""

    product_label = label or identifier
    return {
        "label": product_label,
        "href": f"/data-products/{identifier}",
        "anchor": f"#product-{identifier}",
    }


def _timeline_link_for_contract(identifier: str, label: str | None = None) -> dict[str, str]:
    """Return a link dictionary that targets a contract entry."""

    contract_label = label or f"Contract {identifier}"
    return {
        "label": contract_label,
        "href": f"/contracts/{identifier}",
        "anchor": f"#contract-{identifier}",
    }


def _parse_iso8601(value: str) -> datetime | None:
    """Parse an ISO-8601 timestamp into a timezone-aware datetime if possible."""

    if not value:
        return None
    cleaned = value
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def _store_lookup(run: RetailDemoRun) -> dict[str, Mapping[str, Any]]:
    """Map store identifiers to their dimension attributes."""

    lookup: dict[str, Mapping[str, Any]] = {}
    for row in run.star_schema.store_dimension:
        store_id = str(row.get("store_id", ""))
        if store_id:
            lookup[store_id] = row
    return lookup


def _category_counts(rows: Iterable[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key, ""))
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _quality_reject_sample(run: RetailDemoRun) -> dict[str, Any]:
    """Generate illustrative valid and reject slices for the resilience milestone."""

    facts = list(run.star_schema.sales_fact)
    valid_sample = [dict(facts[0])] if facts else []
    reject_sample: list[dict[str, Any]] = []
    if facts:
        problematic = dict(facts[min(len(facts) - 1, 1)])
        problematic.pop("gross_margin", None)
        problematic["error"] = "Missing required column `gross_margin`"
        problematic["action"] = "Row diverted to ::reject slice until schema fix ships."
        reject_sample.append(problematic)
    total_rows = len(facts)
    reject_rows = max(min(total_rows // 3, 3), 1) if facts else 0
    valid_rows = max(total_rows - reject_rows, 0)
    dataset_version = RETAIL_DATASETS["retail_sales_fact"].dataset_version
    return {
        "dataset": "retail_sales_fact::reject",
        "contract_version": dataset_version,
        "valid_rows": valid_rows,
        "reject_rows": reject_rows,
        "valid_sample": valid_sample,
        "reject_sample": reject_sample,
    }


def simulate_retail_timeline(run: RetailDemoRun) -> list[dict[str, Any]]:
    """Simulate the retail walkthrough timeline with pipeline step metadata."""

    store_lookup = _store_lookup(run)
    store_count = len(store_lookup)
    product_count = len({str(row.get("product_id", "")) for row in run.catalog if row.get("product_id")})
    transaction_count = len(run.transactions)
    transaction_days = len(
        {
            str(row.get("transaction_ts", ""))[:10]
            for row in run.transactions
            if row.get("transaction_ts")
        }
    )
    inventory_count = len(run.inventory)
    expected_inventory = store_count * product_count
    missing_inventory = max(expected_inventory - inventory_count, 0)
    latest_snapshot_ts = max(
        (
            str(row.get("snapshot_ts", ""))
            for row in run.inventory
            if row.get("snapshot_ts")
        ),
        default="",
    )
    snapshot_dt = _parse_iso8601(latest_snapshot_ts)
    freshness_delay_hours = 36
    if snapshot_dt:
        freshness_delay_hours = max(int((datetime(2024, 2, 12, 6, 0) - snapshot_dt).total_seconds() // 3600 * -1), 36)
    feature_rows = len(run.demand_features)
    feature_versions = sorted(
        {
            str(row.get("feature_version", ""))
            for row in run.demand_features
            if row.get("feature_version")
        }
    )
    forecast_rows = len(run.forecasts)
    model_versions = sorted(
        {
            str(row.get("model_version", ""))
            for row in run.forecasts
            if row.get("model_version")
        }
    )
    forecast_feature_versions = sorted(
        {
            str(row.get("feature_version", ""))
            for row in run.forecasts
            if row.get("feature_version")
        }
    )
    offer_rows = len(run.offers)
    offer_store_count = len({str(row.get("store_id", "")) for row in run.offers if row.get("store_id")})
    max_discount = max((float(row.get("recommended_discount", 0.0)) for row in run.offers), default=0.0)
    min_discount = min((float(row.get("recommended_discount", 0.0)) for row in run.offers), default=0.0)
    kpi_rows = len(run.kpis)
    bias_metric = next((metric for metric in run.kpis if metric.get("metric_id") == "forecast_bias"), None)
    bias_value = float(bias_metric.get("value", 0.0)) if bias_metric else 0.0
    bias_percent = f"{bias_value * 100:.0f}%" if bias_metric else "N/A"
    sales_fact_rows = len(run.star_schema.sales_fact)
    sales_fact_columns = len(run.star_schema.sales_fact[0]) if run.star_schema.sales_fact else 0
    foundation_versions = sorted(
        {
            RETAIL_DATASETS[identifier].dataset_version
            for identifier in (
                "retail_pos_transactions",
                "retail_inventory_snapshot",
                "retail_product_catalog",
            )
        }
    )
    inventory_version = RETAIL_DATASETS["retail_inventory_snapshot"].dataset_version
    features_version = RETAIL_DATASETS["retail_demand_features"].dataset_version
    forecast_version = RETAIL_DATASETS["retail_demand_forecast"].dataset_version
    offers_version = RETAIL_DATASETS["retail_personalized_offers"].dataset_version
    kpi_version = RETAIL_DATASETS["retail_kpi_mart"].dataset_version
    sales_fact_version = RETAIL_DATASETS["retail_sales_fact"].dataset_version
    reject_details = _quality_reject_sample(run)

    store_regions = _category_counts(run.star_schema.store_dimension, "region")
    top_region = max(store_regions, key=store_regions.get) if store_regions else "N/A"
    product_categories = _category_counts(run.catalog, "category")
    top_category = max(product_categories, key=product_categories.get) if product_categories else "N/A"

    events: list[dict[str, Any]] = [
        {
            "date": "2024-01-08",
            "title": "Foundation go-live",
            "summary": "Launch the operational data product with source-aligned contracts.",
            "narrative": (
                "Altair Retail stands up the foundational data product so operations "
                "teams can consume POS, inventory, and product master feeds with "
                "governed contracts and freshness monitoring."
            ),
            "milestone": "Launch",
            "severity": "success",
            "state_summary": "All systems ready",
            "callouts": [
                "Activate the `retail_pos_transactions` and `retail_inventory_snapshot` contracts to demonstrate source ownership.",
                "Highlight the `dp.retail-foundation` product card to explain how multiple ports live under one boundary.",
            ],
            "links": [
                _timeline_link_for_dataset("retail_pos_transactions"),
                _timeline_link_for_dataset("retail_inventory_snapshot"),
                _timeline_link_for_dataset("retail_product_catalog"),
                _timeline_link_for_product("dp.retail-foundation", "dp.retail-foundation data product"),
            ],
            "insights": [
                _timeline_insight(
                    "Transactions ingested",
                    f"{transaction_count} rows",
                    f"{store_count} stores captured across {transaction_days or 1} business day(s).",
                ),
                _timeline_insight(
                    "Catalog coverage",
                    f"{product_count} active SKUs",
                    "Product master joins inventory and POS feeds inside the foundation product.",
                ),
                _timeline_insight(
                    "Contract versions",
                    ", ".join(foundation_versions),
                    "Source contracts promoted for the launch.",
                ),
            ],
            "replay": {
                "label": "Foundation pipelines",
                "steps": [
                    asdict(TimelineStep(
                        label="Ingest POS transactions",
                        message=f"Loaded {transaction_count} records across {store_count} stores.",
                        status="success",
                        status_label="Completed",
                        outputs={"dataset": "retail_pos_transactions", "rows": transaction_count},
                    )),
                    asdict(TimelineStep(
                        label="Load inventory snapshot",
                        message=(
                            f"Ingested {inventory_count} rows; {missing_inventory} slots awaiting late files."
                        ),
                        status="success",
                        status_label="Completed",
                        outputs={
                            "dataset": "retail_inventory_snapshot",
                            "version": inventory_version,
                            "rows": inventory_count,
                        },
                    )),
                    asdict(TimelineStep(
                        label="Publish product catalog",
                        message=f"Catalog promotes {product_count} SKUs and {top_category} leads demand planning.",
                        status="success",
                        status_label="Completed",
                        outputs={"dataset": "retail_product_catalog", "rows": len(run.catalog)},
                    )),
                    asdict(TimelineStep(
                        label="Assemble star schema",
                        message=f"Snowflake fact table exposes {sales_fact_rows} rows with {sales_fact_columns} attributes.",
                        status="success",
                        status_label="Completed",
                        outputs={"dataset": "retail_sales_fact", "rows": sales_fact_rows},
                    )),
                ],
            },
            "ui_state": {
                "offers": {
                    "status": "steady",
                    "label": "Ready",
                    "message": "Source pipelines are green. Marketing can prep simulations before launch.",
                },
                "activation": {
                    "status": "steady",
                    "label": "Planning",
                    "message": "Activation workspace is seeded with baseline KPIs for planners.",
                },
                "dashboard": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Executive dashboard is ready to subscribe to curated metrics.",
                },
            },
        },
        {
            "date": "2024-02-12",
            "title": "Freshness incident",
            "summary": "Inventory snapshots stall and trigger a pipeline warning.",
            "narrative": (
                "The nightly load skips an inventory file which causes the freshness SLA "
                "on `retail_inventory_snapshot` to breach. The demo pauses here to show how "
                "contracts surface data quality incidents and how downstream products remain "
                "blocked until the issue is fixed."
            ),
            "milestone": "Incident",
            "severity": "danger",
            "state_summary": "Investigating freshness breach",
            "callouts": [
                "Jump to the inventory contract to show the failed freshness check and breach badge.",
                "Use the timeline replay to explain the halt in downstream publications while ops respond.",
            ],
            "links": [
                _timeline_link_for_dataset("retail_inventory_snapshot"),
                _timeline_link_for_contract("retail_inventory_snapshot"),
            ],
            "insights": [
                _timeline_insight(
                    "Freshness delay",
                    f"{freshness_delay_hours} hours",
                    "SLA breach blocks the marketing and analytics zones until resolved.",
                ),
                _timeline_insight(
                    "Missing inventory",
                    f"{missing_inventory} row(s)",
                    "Coverage gap appears because one store feed failed validation.",
                ),
            ],
            "replay": {
                "label": "Nightly incident",
                "steps": [
                    asdict(TimelineStep(
                        label="Schedule nightly ingest",
                        message="Pipeline dispatcher kicks off the standard POS and inventory load.",
                        status="success",
                        status_label="Dispatched",
                        outputs={"jobs": 3},
                        duration_ms=1100,
                    )),
                    asdict(TimelineStep(
                        label="Validate inventory freshness",
                        message="Freshness SLA breached after missing `NY-001` inventory feed.",
                        status="failed",
                        status_label="Failed",
                        duration_ms=1700,
                        rule={
                            "name": "Inventory snapshot freshness",
                            "expected": "≤ 24 hours",
                            "actual": f"{freshness_delay_hours} hours",
                            "impact": "Store NY-001 missing snapshot; downstream ports paused.",
                        },
                        outputs={"dataset": "retail_inventory_snapshot", "status": "stale"},
                    )),
                    asdict(TimelineStep(
                        label="Freeze downstream ports",
                        message="Activation and analytics pin to the prior valid slice while ops investigate.",
                        status="blocked",
                        status_label="Paused",
                        duration_ms=1200,
                        outputs={"ports": ["retail_personalized_offers", "retail_kpi_mart"]},
                    )),
                    asdict(TimelineStep(
                        label="Escalate incident",
                        message="On-call team alerted with contract violation context and remediation checklist.",
                        status="investigating",
                        status_label="Escalated",
                        duration_ms=1200,
                        outputs={"pager": "Merchandising ops"},
                    )),
                ],
            },
            "ui_state": {
                "offers": {
                    "status": "paused",
                    "label": "Paused",
                    "message": "Offers frozen while inventory freshness recovers. Last good run remains visible.",
                },
                "activation": {
                    "status": "blocked",
                    "label": "Blocked",
                    "message": "Activation planner locked until contract SLA returns to green.",
                },
                "dashboard": {
                    "status": "stale",
                    "label": "Stale",
                    "message": "Dashboard pinned to 2024-01-08 run while ops resolve the breach.",
                },
            },
        },
        {
            "date": "2024-03-18",
            "title": "Model refresh",
            "summary": "Recovered inventory powers a fresh feature store and model version.",
            "narrative": (
                "Ops backfills the missing file, reruns quality checks, and signs off the "
                "feature store so the demand forecaster can refresh its coefficients."
            ),
            "milestone": "Model update",
            "severity": "info",
            "state_summary": "Recovering with new model",
            "callouts": [
                "Show the refreshed feature and forecast dataset versions in the catalog.",
                "Use the replay to narrate the model retrain and bias monitoring instrumentation.",
            ],
            "links": [
                _timeline_link_for_dataset("retail_demand_features"),
                _timeline_link_for_dataset("retail_demand_forecast"),
                _timeline_link_for_product("dp.retail-intelligence", "dp.retail-intelligence data product"),
            ],
            "insights": [
                _timeline_insight(
                    "Feature rows",
                    f"{feature_rows} records",
                    "Feature store rebuilt after freshness incident.",
                ),
                _timeline_insight(
                    "Model version",
                    ", ".join(model_versions) or "n/a",
                    "Demand forecaster redeployed with updated coefficients.",
                ),
            ],
            "replay": {
                "label": "Recovery run",
                "steps": [
                    asdict(TimelineStep(
                        label="Load recovered snapshot",
                        message="Replacement inventory file passes freshness checks and restores coverage.",
                        status="success",
                        status_label="Recovered",
                        duration_ms=1300,
                        outputs={"dataset": "retail_inventory_snapshot", "status": "fresh"},
                    )),
                    asdict(TimelineStep(
                        label="Refresh feature store",
                        message=f"Regenerated {feature_rows} feature rows for demand modelling.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1500,
                        outputs={"dataset": "retail_demand_features", "version": features_version},
                    )),
                    asdict(TimelineStep(
                        label="Retrain demand model",
                        message=f"Promoted model version {model_versions[-1] if model_versions else '1.0.0'} with calibrated bias controls.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1600,
                        outputs={"model_version": model_versions[-1] if model_versions else "1.0.0"},
                    )),
                    asdict(TimelineStep(
                        label="Publish forecasts",
                        message=f"Released {forecast_rows} forecasts backed by feature version {forecast_feature_versions[-1] if forecast_feature_versions else '1.0.0'}.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1300,
                        outputs={"dataset": "retail_demand_forecast", "version": forecast_version},
                    )),
                ],
            },
            "ui_state": {
                "offers": {
                    "status": "recovering",
                    "label": "QA",
                    "message": "Offers regenerate with the latest forecast but stay under review for an hour.",
                },
                "activation": {
                    "status": "recovering",
                    "label": "Review",
                    "message": "Activation planners validate the recovered inventory before resuming edits.",
                },
                "dashboard": {
                    "status": "recovering",
                    "label": "Refreshing",
                    "message": "Dashboard recalculates KPIs once new demand curves arrive.",
                },
            },
        },
        {
            "date": "2024-04-15",
            "title": "Consumer launch",
            "summary": "Marketing orchestrates omnichannel offers from the ML output.",
            "narrative": (
                "The consumer team uses the refreshed model output to activate a curated "
                "set of offers across the flagship stores, demonstrating how activation "
                "consumes the ML data product."
            ),
            "milestone": "Consumer launch",
            "severity": "success",
            "state_summary": "Offers live",
            "callouts": [
                "Use the offer highlights tab to showcase the personalised recommendations per store.",
                "Walk through the activation planner tab to show KPI mixes per region.",
            ],
            "links": [
                _timeline_link_for_dataset("retail_personalized_offers"),
                _timeline_link_for_product("dp.retail-experience", "dp.retail-experience data product"),
            ],
            "insights": [
                _timeline_insight(
                    "Offers published",
                    f"{offer_rows} records",
                    f"Activation spans {offer_store_count} stores with discounts from {min_discount:.1f}% to {max_discount:.1f}%.",
                ),
                _timeline_insight(
                    "Top region",
                    top_region,
                    "Region leading offer volume in the launch cohort.",
                ),
            ],
            "replay": {
                "label": "Activation launch",
                "steps": [
                    asdict(TimelineStep(
                        label="Package offer segments",
                        message="Segmentation selects loyalty cohorts by store and priority metric.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1200,
                        outputs={"stores": offer_store_count},
                    )),
                    asdict(TimelineStep(
                        label="Score personalised offers",
                        message=f"Generated {offer_rows} offers with discount band {min_discount:.1f}%–{max_discount:.1f}%.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1500,
                        outputs={"dataset": "retail_personalized_offers", "version": offers_version},
                    )),
                    asdict(TimelineStep(
                        label="Publish activation brief",
                        message="Activation workspace updates KPI mix and share of plan per store.",
                        status="success",
                        status_label="Published",
                        duration_ms=1300,
                        outputs={"regions": len(store_regions)},
                    )),
                ],
            },
            "ui_state": {
                "offers": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Offer cards refresh with the latest discount and confidence metrics.",
                },
                "activation": {
                    "status": "steady",
                    "label": "Open",
                    "message": "Marketing planners can tweak KPI emphasis with fresh demand scores.",
                },
                "dashboard": {
                    "status": "steady",
                    "label": "Monitoring",
                    "message": "Dashboard tracks uplift and bias alongside live offers.",
                },
            },
        },
        {
            "date": "2024-05-20",
            "title": "Analytics launch",
            "summary": "BI teams subscribe to the curated KPI mart with semantic definitions.",
            "narrative": (
                "Finance and merchandising stakeholders adopt the analytics product, "
                "consuming metrics via BI tools backed by the semantic layer."
            ),
            "milestone": "Analytics launch",
            "severity": "info",
            "state_summary": "KPI mart live",
            "callouts": [
                "Highlight the semantic layer table to show curated measure expressions.",
                "Point the audience to the dashboard tab that renders the same metrics in app.",
            ],
            "links": [
                _timeline_link_for_dataset("retail_kpi_mart"),
                _timeline_link_for_contract("retail_kpi_mart"),
                _timeline_link_for_product("dp.retail-analytics", "dp.retail-analytics data product"),
            ],
            "insights": [
                _timeline_insight(
                    "KPI measures",
                    f"{kpi_rows} metrics",
                    "Semantic layer ensures shared definitions across tools.",
                ),
                _timeline_insight(
                    "Forecast bias",
                    bias_percent,
                    "New KPI surfaces model accuracy across the fleet.",
                ),
                _timeline_insight(
                    "Contract version",
                    kpi_version,
                    "Analytics zone signs off expanded schema.",
                ),
            ],
            "replay": {
                "label": "Analytics publish",
                "steps": [
                    asdict(TimelineStep(
                        label="Curate KPI mart",
                        message=f"Materialised {kpi_rows} KPI rows with aligned business date.",
                        status="success",
                        status_label="Published",
                        duration_ms=1400,
                        outputs={"dataset": "retail_kpi_mart", "version": kpi_version},
                    )),
                    asdict(TimelineStep(
                        label="Attach semantic layer",
                        message="Measures expose consistent expressions and formats for BI tools.",
                        status="success",
                        status_label="Published",
                        duration_ms=1300,
                        outputs={"measures": kpi_rows},
                    )),
                    asdict(TimelineStep(
                        label="Refresh executive dashboard",
                        message="Dashboard pulls latest sell-through and margin metrics for finance.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1400,
                        outputs={"widgets": 6},
                    )),
                ],
            },
            "ui_state": {
                "offers": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Offers remain in market while analytics monitors outcomes.",
                },
                "activation": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Activation teams watch KPI deltas alongside finance.",
                },
                "dashboard": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Executive dashboard refreshed with curated semantics.",
                },
            },
        },
        {
            "date": "2024-06-10",
            "title": "Quality regression",
            "summary": "Schema drift creates rejects while consumers fall back to last valid slice.",
            "narrative": (
                "A merchandising attribute rename slips into a release candidate and contract "
                "validation flags the change. Reject rows are written for diagnostics while the "
                "consumer apps downgrade gracefully to the last approved dataset."
            ),
            "milestone": "Regression",
            "severity": "warning",
            "state_summary": "Serving fallback datasets",
            "callouts": [
                "Show the reject sample in the detail pane to explain how validation isolates bad rows.",
                "Point to the sales fact contract history that records the failed schema check.",
            ],
            "links": [
                _timeline_link_for_dataset("retail_sales_fact"),
                _timeline_link_for_contract("retail_sales_fact"),
            ],
            "insights": [
                _timeline_insight(
                    "Reject rows",
                    f"{reject_details['reject_rows']} row(s)",
                    "Failed schema rows isolated for remediation teams.",
                ),
                _timeline_insight(
                    "Valid rows",
                    f"{reject_details['valid_rows']} row(s)",
                    "Consumers continue using the trusted ::valid slice.",
                ),
            ],
            "replay": {
                "label": "Contract guardrail",
                "steps": [
                    asdict(TimelineStep(
                        label="Deploy schema change",
                        message="Release candidate introduces column rename for gross margin.",
                        status="warning",
                        status_label="Pending",
                        duration_ms=1100,
                        outputs={"change_request": "MERCH-482"},
                    )),
                    asdict(TimelineStep(
                        label="Run contract regression",
                        message="Validation fails: required column `gross_margin` missing in payload.",
                        status="failed",
                        status_label="Failed",
                        duration_ms=1700,
                        rule={
                            "name": "Sales fact schema",
                            "expected": "Columns include gross_margin",
                            "actual": "Column dropped in draft feed",
                            "impact": "Reject slice created; consumers pinned to ::valid.",
                        },
                        outputs={"dataset": "retail_sales_fact", "status": "reject"},
                    )),
                    asdict(TimelineStep(
                        label="Emit reject slice",
                        message="Problem rows copied to `retail_sales_fact::reject` for remediation.",
                        status="success",
                        status_label="Captured",
                        duration_ms=1400,
                        outputs={"reject_rows": reject_details["reject_rows"]},
                    )),
                    asdict(TimelineStep(
                        label="Serve last valid dataset",
                        message="Dashboards and activation fall back to the approved ::valid slice.",
                        status="recovering",
                        status_label="Fallback",
                        duration_ms=1500,
                        outputs={"dataset": "retail_sales_fact::valid"},
                    )),
                ],
            },
            "rejects": reject_details,
            "ui_state": {
                "offers": {
                    "status": "unstable",
                    "label": "Fallback",
                    "message": "Offers temporarily use last trusted metrics while rejects are triaged.",
                },
                "activation": {
                    "status": "paused",
                    "label": "Paused",
                    "message": "Activation workspace locked to prevent publishing inconsistent data.",
                },
                "dashboard": {
                    "status": "stale",
                    "label": "Fallback",
                    "message": "Executive dashboard pinned to ::valid slice until fix deploys.",
                },
            },
        },
        {
            "date": "2024-06-24",
            "title": "Pipeline hardening",
            "summary": "Ops enables automated rollbacks after a schema regression test fails.",
            "narrative": (
                "A merchandising attribute rename is caught in pre-production, so the team "
                "demonstrates how automated contract validation prevents bad schemas from reaching "
                "the consumer zones. The demo ends on the stabilised run that replays the full chain successfully."
            ),
            "milestone": "Resilience",
            "severity": "success",
            "state_summary": "Guardrails verified",
            "callouts": [
                "Toggle the demo player to replay the incident and explain how devs patched the contract before re-running.",
                "Re-run the demo pipeline live to show how caching refreshes once validation passes.",
            ],
            "links": [
                {
                    "label": "View recent pipeline runs",
                    "href": "/pipeline-runs",
                    "anchor": "#retail-run-card",
                },
                _timeline_link_for_dataset("retail_sales_fact"),
                _timeline_link_for_contract("retail_sales_fact"),
            ],
            "insights": [
                _timeline_insight(
                    "Sales fact rows",
                    f"{sales_fact_rows}",
                    "Regression suite validates the snowflake schema before publication.",
                ),
                _timeline_insight(
                    "Schema columns",
                    f"{sales_fact_columns}",
                    "Automated checks guard against unintended contract changes.",
                ),
                _timeline_insight(
                    "Contract version",
                    sales_fact_version,
                    "Rollback automation restores the approved interface before rerun.",
                ),
            ],
            "replay": {
                "label": "Stabilised rerun",
                "steps": [
                    asdict(TimelineStep(
                        label="Rollback schema change",
                        message="Deployment reverted; contract restored expected columns.",
                        status="success",
                        status_label="Rolled back",
                        duration_ms=1300,
                        outputs={"change_request": "MERCH-482"},
                    )),
                    asdict(TimelineStep(
                        label="Re-run regression suite",
                        message="Contract tests pass for sales fact, valid slice promoted.",
                        status="success",
                        status_label="Passed",
                        duration_ms=1500,
                        outputs={"tests": 42},
                    )),
                    asdict(TimelineStep(
                        label="Publish consumer ports",
                        message="Offers, activation, and dashboards repoint to the refreshed dataset.",
                        status="success",
                        status_label="Completed",
                        duration_ms=1400,
                        outputs={"ports": 3},
                    )),
                    asdict(TimelineStep(
                        label="Close incident",
                        message="Runbook closed with automated rollback lessons captured for SRE.",
                        status="success",
                        status_label="Resolved",
                        duration_ms=1200,
                        outputs={"postmortem": "INC-204"},
                    )),
                ],
            },
            "ui_state": {
                "offers": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Offers resume dynamic updates after the schema fix.",
                },
                "activation": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Activation workspace reopens with validated metrics.",
                },
                "dashboard": {
                    "status": "steady",
                    "label": "Live",
                    "message": "Dashboard stability restored; rejects cleared.",
                },
            },
        },
    ]

    return events


__all__ = ["simulate_retail_timeline"]
