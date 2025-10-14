from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlencode
from uuid import uuid4

from collections import Counter, defaultdict
from dataclasses import asdict

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from importlib import resources

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape

from .contracts_api import set_active_version
from .contracts_records import (
    load_records,
    pop_flash,
    queue_flash,
    save_records,
    scenario_history,
    scenario_run_rows,
)
from .contracts_workspace import prepare_demo_workspace
from .scenarios import SCENARIOS
from .retail_demo import (
    RETAIL_DATA_PRODUCTS,
    RETAIL_DATASETS,
    RetailDataProduct,
    RetailDemoRun,
    run_retail_demo,
    simulate_retail_timeline,
)
from . import streaming as streaming_demo

CATEGORY_LABELS = {
    "contract": "Contract-focused pipelines",
    "data-product": "Data product pipelines",
    "streaming": "Streaming pipelines",
}

STATUS_BADGES = {
    "ok": "bg-success",
    "success": "bg-success",
    "warn": "bg-warning text-dark",
    "warning": "bg-warning text-dark",
    "block": "bg-danger",
    "error": "bg-danger",
}

prepare_demo_workspace()

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CONTRACTS_TEMPLATE_DIR = resources.files("dc43_contracts_app") / "templates"


def _normalise_streaming_batches(
    batches: Iterable[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not isinstance(batches, Iterable):
        return []
    normalised: list[dict[str, Any]] = []
    for batch in batches:
        if not isinstance(batch, Mapping):
            continue
        entry = dict(batch)
        timestamp = entry.get("timestamp")
        if isinstance(timestamp, str):
            try:
                stamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                pass
            else:
                entry.setdefault("time", stamp.strftime("%H:%M:%S"))
        status = entry.get("status")
        if isinstance(status, str):
            entry["status"] = status.lower()
        row_count = entry.get("row_count")
        if isinstance(row_count, (int, float)):
            entry["row_count"] = int(row_count)
        violations = entry.get("violations")
        if isinstance(violations, (int, float)):
            entry["violations"] = int(violations)
        normalised.append(entry)
    return normalised
PIPELINE_TEMPLATE_DIR = BASE_DIR / "templates"

_RETAIL_RUN: RetailDemoRun | None = None

_ZONE_STYLES: Mapping[str, str] = {
    "source": "fill:#E3F2FD,stroke:#1E88E5,color:#0D47A1,stroke-width:1px",
    "modelled": "fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20,stroke-width:1px",
    "ml": "fill:#FFF3E0,stroke:#FB8C00,color:#E65100,stroke-width:1px",
    "consumer": "fill:#F3E5F5,stroke:#8E24AA,color:#4A148C,stroke-width:1px",
    "aggregated": "fill:#E0F7FA,stroke:#00838F,color:#006064,stroke-width:1px",
    "shared": "fill:#ECEFF1,stroke:#607D8B,color:#37474F,stroke-width:1px",
}

_ZONE_BADGES: Mapping[str, str] = {
    "source": "text-bg-primary",
    "modelled": "text-bg-success",
    "ml": "text-bg-warning text-dark",
    "consumer": "text-bg-danger",
    "aggregated": "text-bg-info text-dark",
    "shared": "text-bg-secondary",
}

_PRODUCT_ZONE_LOOKUP: Mapping[str, tuple[str, str]] = {
    "dp.retail-foundation": ("source", "Source"),
    "dp.retail-insights": ("modelled", "Modelled"),
    "dp.retail-intelligence": ("ml", "ML"),
    "dp.retail-experience": ("consumer", "Consumer"),
    "dp.retail-analytics": ("aggregated", "Aggregated"),
}

_TIMELINE_BADGES: Mapping[str, str] = {
    "success": "text-bg-success",
    "info": "text-bg-primary",
    "warning": "text-bg-warning text-dark",
    "danger": "text-bg-danger",
}

_env_loader = ChoiceLoader(
    [
        FileSystemLoader(str(PIPELINE_TEMPLATE_DIR)),
        FileSystemLoader(str(CONTRACTS_TEMPLATE_DIR)),
    ]
)

template_env = Environment(loader=_env_loader, autoescape=select_autoescape(["html", "xml"]))
templates = Jinja2Templates(env=template_env)


class StreamingRunState:
    """Track live progress for asynchronous streaming runs."""

    def __init__(
        self,
        *,
        scenario_key: str,
        seconds: int,
        run_type: str,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.run_id = uuid4().hex
        self.scenario_key = scenario_key
        self.seconds = seconds
        self.run_type = run_type
        self.loop = loop
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.finished = False
        self.dataset_name: str | None = None
        self.dataset_version: str | None = None

    def publish(self, event: Mapping[str, Any]) -> None:
        payload = dict(event)
        payload.setdefault("run_id", self.run_id)
        self.loop.call_soon_threadsafe(self.queue.put_nowait, payload)

    def finish(self) -> None:
        self.finished = True


class StreamingRunProgress:
    """Adapter that exposes the streaming progress protocol to scenarios."""

    def __init__(self, state: StreamingRunState) -> None:
        self.state = state

    def emit(self, event: Mapping[str, Any]) -> None:
        self.state.publish(event)


_STREAMING_RUNS: dict[str, StreamingRunState] = {}


def _retail_demo_run_cached() -> RetailDemoRun:
    """Return the cached Altair Retail run used for the faux applications."""

    global _RETAIL_RUN
    if _RETAIL_RUN is None:
        _RETAIL_RUN = run_retail_demo()
    return _RETAIL_RUN


def _retail_store_lookup(run: RetailDemoRun) -> dict[str, Mapping[str, Any]]:
    """Map store identifiers to their dimension attributes."""

    lookup: dict[str, Mapping[str, Any]] = {}
    for row in run.star_schema.store_dimension:
        store_id = str(row.get("store_id", ""))
        if store_id:
            lookup[store_id] = row
    return lookup


def _format_metric_card(metric: Mapping[str, Any]) -> dict[str, Any]:
    """Prepare KPI metadata for display in the dashboard."""

    semantic_raw = metric.get("semantic_layer", {})
    semantic = semantic_raw if isinstance(semantic_raw, Mapping) else {}
    fmt = str(semantic.get("format", "")) if semantic else ""
    unit = str(metric.get("unit", ""))
    value = float(metric.get("value", 0.0))
    if fmt == "currency" or unit == "USD":
        value_display = f"${value:,.0f}"
    elif fmt == "percent":
        value_display = f"{value * 100:.1f}%"
    elif unit == "units":
        value_display = f"{value:,.0f} units"
    else:
        value_display = f"{value:,.2f}"
    return {
        "metric_id": metric.get("metric_id"),
        "label": metric.get("metric_label", metric.get("metric_id")),
        "value_display": value_display,
        "raw_value": value,
        "unit": unit,
        "business_date": metric.get("business_date"),
        "semantic": {
            "aggregation": semantic.get("aggregation", ""),
            "expression": semantic.get("expression", ""),
            "description": semantic.get("description", ""),
            "format": fmt,
        },
    }


def _product_cards() -> list[dict[str, Any]]:
    """Expose the Altair Retail data product catalogue for the overview page."""

    cards: list[dict[str, Any]] = []
    for product in sorted(RETAIL_DATA_PRODUCTS.values(), key=lambda item: item.name):
        data = asdict(product)
        data["inputs"] = list(product.inputs)
        data["outputs"] = list(product.outputs)
        data["tags"] = list(product.tags)
        data["anchor"] = f"product-{product.identifier}"
        data["url"] = f"/data-products/{product.identifier}"
        cards.append(data)
    return cards


def _zone_metadata(product: RetailDataProduct | None) -> dict[str, str]:
    """Return display and styling information for a product's zone."""

    if product and product.identifier in _PRODUCT_ZONE_LOOKUP:
        zone_key, zone_label = _PRODUCT_ZONE_LOOKUP[product.identifier]
        product_label = product.name
    else:
        zone_key, zone_label = ("shared", "Shared")
        product_label = product.name if product else "Shared assets"
    style = _ZONE_STYLES.get(zone_key, _ZONE_STYLES["shared"])
    return {
        "key": zone_key,
        "label": zone_label,
        "product_label": product_label,
        "style": style,
        "badge": _ZONE_BADGES.get(zone_key, _ZONE_BADGES["shared"]),
    }


def _dataset_lineage_diagram() -> str:
    """Build a Mermaid graph that highlights dataset lineage and product zones."""

    datasets = list(RETAIL_DATASETS.values())
    datasets.sort(key=lambda item: item.identifier)
    grouped: defaultdict[str | None, list] = defaultdict(list)
    for dataset in datasets:
        grouped[dataset.data_product_id].append(dataset)

    lines: list[str] = ["graph LR"]
    for zone_key, style in _ZONE_STYLES.items():
        lines.append(f"  classDef zone-{zone_key} {style};")

    for product_id, members in grouped.items():
        product = RETAIL_DATA_PRODUCTS.get(product_id) if product_id else None
        zone = _zone_metadata(product)
        subgraph_id = (product_id or "shared").replace(".", "_").replace("-", "_")
        title = zone["product_label"]
        zone_label = zone["label"]
        lines.append(f"  subgraph {subgraph_id}[\"{title}<br/>{zone_label} zone\"]")
        for dataset in sorted(members, key=lambda item: item.identifier):
            node_id = dataset.identifier
            kind_label = str(dataset.kind or "").replace("_", " ").title()
            label_lines = [dataset.identifier, f"{zone_label} · {kind_label}"]
            if dataset.output_port:
                port_line = f"Port: {dataset.output_port}"
                if dataset.internal:
                    port_line += " (internal)"
                label_lines.append(port_line)
            elif dataset.internal:
                label_lines.append("Internal asset")
            label = "<br/>".join(label_lines)
            lines.append(f"    {node_id}[\"{label}\"]:::zone-{zone['key']}")
        lines.append("  end")

    for dataset in datasets:
        for dep in dataset.dependencies:
            lines.append(f"  {dep} --> {dataset.identifier}")

    return "\n".join(lines)


def _data_product_flow_diagram() -> str:
    """Render a Mermaid diagram showing product hand-offs across the walkthrough."""

    products = list(RETAIL_DATA_PRODUCTS.values())
    products.sort(key=lambda item: item.identifier)
    datasets = list(RETAIL_DATASETS.values())
    dataset_to_product = {
        dataset.identifier: dataset.data_product_id
        for dataset in datasets
        if dataset.data_product_id
    }

    lines: list[str] = ["graph LR"]
    for zone_key, style in _ZONE_STYLES.items():
        lines.append(f"  classDef zone-{zone_key} {style};")

    node_ids: dict[str, str] = {}
    for product in products:
        zone = _zone_metadata(product)
        node_id = product.identifier.replace(".", "_").replace("-", "_")
        node_ids[product.identifier] = node_id
        input_count = len(product.inputs)
        output_count = len(product.outputs)
        input_label = (
            ", ".join(product.inputs)
            if product.inputs
            else "None"
        )
        output_label = ", ".join(product.outputs) if product.outputs else "None"
        label_lines = [
            product.name,
            f"{zone['label']} zone · {output_count} port{'s' if output_count != 1 else ''}",
            f"Inputs: {input_label}",
            f"Outputs: {output_label}",
        ]
        label = "<br/>".join(label_lines).replace('"', "&quot;")
        lines.append(f"  {node_id}[\"{label}\"]:::zone-{zone['key']}")

    edges: set[tuple[str, str]] = set()
    for dataset in datasets:
        target_product = dataset.data_product_id
        if not target_product:
            continue
        for dependency in dataset.dependencies:
            source_product = dataset_to_product.get(dependency)
            if not source_product or source_product == target_product:
                continue
            edges.add((source_product, target_product))

    for source_id, target_id in sorted(edges):
        source_node = node_ids.get(source_id)
        target_node = node_ids.get(target_id)
        if not source_node or not target_node:
            continue
        lines.append(f"  {source_node} --> {target_node}")

    return "\n".join(lines)


_DATASET_EXTRACTORS: Mapping[str, Callable[[RetailDemoRun], Iterable[Mapping[str, Any]]]] = {
    "retail_pos_transactions": lambda run: run.transactions,
    "retail_inventory_snapshot": lambda run: run.inventory,
    "retail_product_catalog": lambda run: run.catalog,
    "retail_sales_fact": lambda run: run.star_schema.sales_fact,
    "retail_store_dimension": lambda run: run.star_schema.store_dimension,
    "retail_product_dimension": lambda run: run.star_schema.product_dimension,
    "retail_date_dimension": lambda run: run.star_schema.date_dimension,
    "retail_demand_features": lambda run: run.demand_features,
    "retail_demand_forecast": lambda run: run.forecasts,
    "retail_personalized_offers": lambda run: run.offers,
    "retail_kpi_mart": lambda run: run.kpis,
}


def _dataset_payload(run: RetailDemoRun, identifier: str) -> list[Mapping[str, Any]]:
    """Return the in-memory records for a dataset from the cached run."""

    extractor = _DATASET_EXTRACTORS.get(identifier)
    if not extractor:
        return []
    payload = extractor(run)
    if isinstance(payload, list):
        return payload
    return list(payload)


def _retail_dataset_catalog(run: RetailDemoRun) -> list[dict[str, Any]]:
    """Summarise dataset metadata, lineage, and quick stats for the overview page."""

    catalog: list[dict[str, Any]] = []
    for dataset in RETAIL_DATASETS.values():
        product = RETAIL_DATA_PRODUCTS.get(dataset.data_product_id or "")
        zone = _zone_metadata(product)
        if product:
            zone_display = f"{zone['label']} — {product.name}"
        else:
            zone_display = zone["label"]
        payload = _dataset_payload(run, dataset.identifier)
        dependencies = [
            {
                "identifier": dep,
                "anchor": f"dataset-{dep}",
                "url": f"/datasets/{dep}",
            }
            for dep in dataset.dependencies
        ]
        catalog.append(
            {
                "identifier": dataset.identifier,
                "anchor": f"dataset-{dataset.identifier}",
                "url": f"/datasets/{dataset.identifier}",
                "kind": dataset.kind,
                "description": dataset.description,
                "data_product": (
                    {
                        "identifier": product.identifier,
                        "name": product.name,
                        "anchor": f"product-{product.identifier}",
                        "url": f"/data-products/{product.identifier}",
                    }
                    if product
                    else None
                ),
                "output_port": dataset.output_port,
                "contract_id": dataset.contract_id,
                "contract_anchor": f"contract-{dataset.contract_id}",
                "contract_url": f"/contracts/{dataset.contract_id}",
                "dependencies": dependencies,
                "internal": dataset.internal,
                "row_count": len(payload),
                "zone_label": zone_display,
                "zone_key": zone["key"],
                "zone_badge": zone["badge"],
            }
        )
    catalog.sort(key=lambda item: (item.get("data_product", {}).get("name", ""), item["identifier"]))
    return catalog


def _retail_contract_cards() -> list[dict[str, Any]]:
    """Aggregate contracts touched by the retail demo with dataset references."""

    grouped: defaultdict[str, list[str]] = defaultdict(list)
    for dataset in RETAIL_DATASETS.values():
        grouped[dataset.contract_id].append(dataset.identifier)
    cards: list[dict[str, Any]] = []
    for contract_id, dataset_ids in grouped.items():
        product_ids = sorted(
            {
                RETAIL_DATASETS[dataset_id].data_product_id
                for dataset_id in dataset_ids
                if RETAIL_DATASETS[dataset_id].data_product_id
            }
        )
        cards.append(
            {
                "contract_id": contract_id,
                "anchor": f"contract-{contract_id}",
                "url": f"/contracts/{contract_id}",
                "dataset_ids": sorted(dataset_ids),
                "dataset_count": len(dataset_ids),
                "kinds": sorted(
                    {RETAIL_DATASETS[dataset_id].kind for dataset_id in dataset_ids}
                ),
                "product_refs": [
                    {
                        "identifier": pid,
                        "name": RETAIL_DATA_PRODUCTS.get(pid).name
                        if pid and RETAIL_DATA_PRODUCTS.get(pid)
                        else pid,
                        "anchor": f"product-{pid}" if pid else None,
                        "url": f"/data-products/{pid}" if pid else None,
                    }
                    for pid in product_ids
                ],
            }
        )
    cards.sort(key=lambda item: item["contract_id"])
    return cards


def _store_offer_cards(run: RetailDemoRun) -> list[dict[str, Any]]:
    """Summarise the personalised offers by store for the overview page."""

    store_lookup = _retail_store_lookup(run)
    grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for offer in run.offers:
        store_id = str(offer.get("store_id", ""))
        if store_id:
            grouped[store_id].append(offer)
    cards: list[dict[str, Any]] = []
    for store_id, offers in grouped.items():
        info = store_lookup.get(store_id, {})
        top_offer = max(offers, key=lambda row: float(row.get("forecast_units", 0.0)))
        cards.append(
            {
                "store_id": store_id,
                "store_name": str(info.get("store_name", store_id)),
                "region": str(info.get("region", "Unknown")),
                "format": str(info.get("format", "standard")),
                "offer_count": len(offers),
                "headline_category": str(top_offer.get("category", "")),
                "headline_metric": str(top_offer.get("primary_metric", "")),
                "discount_pct": float(top_offer.get("recommended_discount", 0.0)) * 100,
                "confidence_pct": float(top_offer.get("confidence", 0.0)) * 100,
                "sell_through_pct": float(top_offer.get("sell_through_rate", 0.0)) * 100,
            }
        )
    cards.sort(key=lambda item: item["store_name"])
    return cards


def _store_activation_rows(run: RetailDemoRun) -> list[dict[str, Any]]:
    """Group offers by store with campaign planning metadata."""

    store_lookup = _retail_store_lookup(run)
    grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for offer in run.offers:
        store_id = str(offer.get("store_id", ""))
        if store_id:
            grouped[store_id].append(offer)

    stores: list[dict[str, Any]] = []
    for store_id, offers in grouped.items():
        info = store_lookup.get(store_id, {})
        ordered = sorted(offers, key=lambda row: float(row.get("forecast_units", 0.0)), reverse=True)
        max_forecast = max((float(row.get("forecast_units", 0.0)) for row in ordered), default=0.0)
        metric_counts = Counter(str(row.get("primary_metric", "")) for row in ordered if row.get("primary_metric"))
        metric_mix = [
            {
                "metric": metric,
                "metric_label": metric.replace("_", " ").title(),
                "count": count,
                "share": (count / len(ordered)) * 100 if ordered else 0.0,
            }
            for metric, count in metric_counts.most_common()
        ]
        offer_rows = []
        for row in ordered:
            forecast_units = float(row.get("forecast_units", 0.0))
            offer_rows.append(
                {
                    "offer_id": row.get("offer_id"),
                    "customer_id": row.get("customer_id"),
                    "category": row.get("category"),
                    "primary_metric": row.get("primary_metric"),
                    "primary_metric_label": str(row.get("primary_metric", "")).replace("_", " ").title(),
                    "discount_pct": float(row.get("recommended_discount", 0.0)) * 100,
                    "forecast_units": forecast_units,
                    "forecast_share": (forecast_units / max_forecast) * 100 if max_forecast else 0.0,
                    "confidence_pct": float(row.get("confidence", 0.0)) * 100,
                    "sell_through_pct": float(row.get("sell_through_rate", 0.0)) * 100,
                    "business_date": row.get("business_date"),
                }
            )
        stores.append(
            {
                "store_id": store_id,
                "store_name": str(info.get("store_name", store_id)),
                "region": str(info.get("region", "Unknown")),
                "format": str(info.get("format", "standard")),
                "square_feet": info.get("square_feet"),
                "offers": offer_rows,
                "metric_mix": metric_mix,
            }
        )
    stores.sort(key=lambda item: item["store_name"])
    return stores


def _dashboard_breakdowns(run: RetailDemoRun) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Aggregate sales fact data into region and category breakdowns."""

    store_lookup = _retail_store_lookup(run)
    region_sales: defaultdict[str, float] = defaultdict(float)
    category_sales: defaultdict[str, float] = defaultdict(float)
    for row in run.star_schema.sales_fact:
        store_id = str(row.get("store_id", ""))
        region = str(store_lookup.get(store_id, {}).get("region", "Unknown"))
        region_sales[region] += float(row.get("gross_sales", 0.0))
        category = str(row.get("category", "Unknown")) or "Unknown"
        category_sales[category] += float(row.get("gross_sales", 0.0))
    total_region = sum(region_sales.values()) or 1.0
    total_category = sum(category_sales.values()) or 1.0
    region_rows = [
        {
            "label": region,
            "value": value,
            "share": (value / total_region) * 100,
        }
        for region, value in sorted(region_sales.items(), key=lambda item: item[1], reverse=True)
    ]
    category_rows = [
        {
            "label": category,
            "value": value,
            "share": (value / total_category) * 100,
        }
        for category, value in sorted(category_sales.items(), key=lambda item: item[1], reverse=True)
    ]
    return region_rows, category_rows


def _retail_business_date(run: RetailDemoRun) -> str:
    """Extract the business date shared by the retail fixtures."""

    if run.kpis:
        return str(run.kpis[0].get("business_date", ""))
    return ""


def _retail_demo_timeline(run: RetailDemoRun) -> list[dict[str, Any]]:
    """Simulate a multi-month operational timeline for the walkthrough."""

    events = simulate_retail_timeline(run)
    enriched: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        severity = event.get("severity", "info")
        badge = _TIMELINE_BADGES.get(severity, _TIMELINE_BADGES["info"])
        enriched.append(
            {
                **event,
                "index": index,
                "badge_class": badge,
                "milestone_label": event.get("milestone") or severity.title(),
            }
        )
    return enriched


app = FastAPI(title="DC43 Demo Pipeline")

CONTRACTS_APP_URL = os.getenv("DC43_CONTRACTS_APP_URL")
@app.get("/")
async def redirect_to_pipeline() -> RedirectResponse:
    return RedirectResponse(url="/pipeline-runs", status_code=307)


@app.get("/contracts")
async def redirect_contracts() -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    return RedirectResponse(url=CONTRACTS_APP_URL, status_code=307)


@app.get("/datasets")
async def redirect_datasets() -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    return RedirectResponse(url=f"{CONTRACTS_APP_URL.rstrip('/')}/datasets", status_code=307)


@app.get("/data-products")
async def redirect_data_products() -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    return RedirectResponse(url=f"{CONTRACTS_APP_URL.rstrip('/')}/data-products", status_code=307)


@app.get("/retail-demo", response_class=HTMLResponse)
async def retail_demo_overview(request: Request) -> HTMLResponse:
    run = _retail_demo_run_cached()
    dashboard_metrics = [_format_metric_card(metric) for metric in run.kpis]
    kpi_preview = dashboard_metrics[:3]
    region_rows, category_rows = _dashboard_breakdowns(run)
    semantic_rows = [
        {
            "label": card["label"],
            "aggregation": card["semantic"].get("aggregation", ""),
            "expression": card["semantic"].get("expression", ""),
            "description": card["semantic"].get("description", ""),
        }
        for card in dashboard_metrics
    ]
    message = request.query_params.get("msg")
    context = {
        "request": request,
        "products": _product_cards(),
        "store_cards": _store_offer_cards(run),
        "activation_stores": _store_activation_rows(run),
        "kpi_preview": kpi_preview,
        "dashboard_metrics": dashboard_metrics,
        "dashboard_region_rows": region_rows,
        "dashboard_category_rows": category_rows,
        "dashboard_semantic_rows": semantic_rows,
        "business_date": _retail_business_date(run),
        "dataset_catalog": _retail_dataset_catalog(run),
        "contract_cards": _retail_contract_cards(),
        "data_product_flow": _data_product_flow_diagram(),
        "dataset_lineage": _dataset_lineage_diagram(),
        "timeline_events": _retail_demo_timeline(run),
        "message": message,
    }
    return templates.TemplateResponse("retail_overview.html", context)


@app.post("/retail-demo/run", response_class=HTMLResponse)
async def retail_demo_rerun() -> HTMLResponse:
    global _RETAIL_RUN
    _RETAIL_RUN = await asyncio.to_thread(run_retail_demo)
    params = urlencode({"msg": "Retail demo data refreshed."})
    return RedirectResponse(url=f"/retail-demo?{params}", status_code=303)


@app.get("/retail-demo/activation", response_class=HTMLResponse)
async def retail_demo_activation(request: Request) -> HTMLResponse:
    run = _retail_demo_run_cached()
    stores = _store_activation_rows(run)
    context = {
        "request": request,
        "stores": stores,
        "business_date": _retail_business_date(run),
    }
    return templates.TemplateResponse("retail_activation.html", context)


@app.get("/retail-demo/dashboard", response_class=HTMLResponse)
async def retail_demo_dashboard(request: Request) -> HTMLResponse:
    run = _retail_demo_run_cached()
    metrics = [_format_metric_card(metric) for metric in run.kpis]
    region_rows, category_rows = _dashboard_breakdowns(run)
    semantic_rows = [
        {
            "label": card["label"],
            "aggregation": card["semantic"].get("aggregation", ""),
            "expression": card["semantic"].get("expression", ""),
            "description": card["semantic"].get("description", ""),
        }
        for card in metrics
    ]
    context = {
        "request": request,
        "metrics": metrics,
        "region_rows": region_rows,
        "category_rows": category_rows,
        "semantic_rows": semantic_rows,
        "business_date": _retail_business_date(run),
    }
    return templates.TemplateResponse("retail_dashboard.html", context)


@app.get("/contracts/{path:path}")
async def redirect_contract_pages(path: str) -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    url = f"{CONTRACTS_APP_URL.rstrip('/')}/contracts/{path}" if path else f"{CONTRACTS_APP_URL.rstrip('/')}/contracts"
    return RedirectResponse(url=url, status_code=307)


@app.get("/datasets/{path:path}")
async def redirect_dataset_pages(path: str) -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    url = f"{CONTRACTS_APP_URL.rstrip('/')}/datasets/{path}" if path else f"{CONTRACTS_APP_URL.rstrip('/')}/datasets"
    return RedirectResponse(url=url, status_code=307)


@app.get("/data-products/{path:path}")
async def redirect_data_product_pages(path: str) -> RedirectResponse:
    if not CONTRACTS_APP_URL:
        raise HTTPException(status_code=404, detail="Contracts app not configured")
    url = f"{CONTRACTS_APP_URL.rstrip('/')}/data-products/{path}" if path else f"{CONTRACTS_APP_URL.rstrip('/')}/data-products"
    return RedirectResponse(url=url, status_code=307)


@app.get("/pipeline-runs", response_class=HTMLResponse)
async def list_pipeline_runs(request: Request) -> HTMLResponse:
    records = load_records()
    recs = [r.__dict__.copy() for r in records]
    scenario_rows = scenario_run_rows(records, SCENARIOS)
    order = ["contract", "data-product"]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in scenario_rows:
        grouped.setdefault(row.get("category", "contract"), []).append(row)
    scenario_groups = []
    for key in order:
        if key in grouped:
            scenario_groups.append(
                {
                    "key": key,
                    "label": CATEGORY_LABELS.get(key, key.replace("-", " ").title()),
                    "rows": grouped[key],
                }
            )
    for key, rows in grouped.items():
        if key not in order:
            scenario_groups.append(
                {
                    "key": key,
                    "label": CATEGORY_LABELS.get(key, key.replace("-", " ").title()),
                    "rows": rows,
                }
            )
    flash_token = request.query_params.get("flash")
    flash_message: str | None = None
    flash_error: str | None = None
    if flash_token:
        flash_message, flash_error = pop_flash(flash_token)
    else:
        flash_message = request.query_params.get("msg")
        flash_error = request.query_params.get("error")
    context = {
        "request": request,
        "records": recs,
        "scenarios": SCENARIOS,
        "scenario_rows": scenario_rows,
        "scenario_groups": scenario_groups,
        "message": flash_message,
        "error": flash_error,
    }
    return templates.TemplateResponse("pipeline_runs.html", context)


@app.get("/pipeline-runs/{scenario_key}", response_class=HTMLResponse)
async def pipeline_run_detail(request: Request, scenario_key: str) -> HTMLResponse:
    scenario_cfg = SCENARIOS.get(scenario_key)
    if not scenario_cfg:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {scenario_key}")

    records = load_records()
    history_records, dataset_name = scenario_history(records, scenario_key, scenario_cfg)
    row_candidates = scenario_run_rows(records, {scenario_key: scenario_cfg})
    scenario_row = row_candidates[0] if row_candidates else {
        "key": scenario_key,
        "label": scenario_cfg.get("label", scenario_key.replace("-", " ").title()),
        "description": scenario_cfg.get("description"),
        "diagram": scenario_cfg.get("diagram"),
        "category": scenario_cfg.get("category", "contract"),
        "dataset_name": dataset_name,
        "contract_id": scenario_cfg.get("params", {}).get("contract_id"),
        "contract_version": scenario_cfg.get("params", {}).get("contract_version"),
        "run_type": scenario_cfg.get("params", {}).get("run_type", "infer"),
        "run_count": 0,
        "latest": None,
    }

    history_entries = []
    for record in history_records:
        record_payload = record.__dict__.copy()
        dq_details = record_payload.get("dq_details")
        if not isinstance(dq_details, Mapping):
            dq_details = {}
        output_details = (
            dq_details.get("output", {}) if isinstance(dq_details, Mapping) else {}
        )
        if not isinstance(output_details, Mapping):
            output_details = {}
        streaming_batches = _normalise_streaming_batches(
            output_details.get("streaming_batches")
        )
        failed_expectations = output_details.get("failed_expectations", {})
        if not isinstance(failed_expectations, Mapping):
            failed_expectations = {}
        schema_errors = output_details.get("errors", [])
        if not isinstance(schema_errors, list):
            schema_errors = []
        dq_aux = output_details.get("dq_auxiliary_statuses", [])
        if not isinstance(dq_aux, list):
            dq_aux = []
        input_payloads = []
        if isinstance(dq_details, Mapping):
            for key, payload in dq_details.items():
                if key in {"output", "rejects"}:
                    continue
                input_payloads.append({"name": key, "payload": payload})
        input_refs: list[dict[str, Any]] = []
        for payload in input_payloads:
            payload_details = payload.get("payload")
            if not isinstance(payload_details, Mapping):
                continue
            dataset_id = payload_details.get("dataset_id")
            if not dataset_id:
                continue
            ref: dict[str, Any] = {
                "name": payload.get("name"),
                "dataset": dataset_id,
            }
            version = payload_details.get("dataset_version")
            if isinstance(version, str) and version:
                ref["version"] = version
            input_refs.append(ref)
        reject_refs: list[dict[str, Any]] = []
        if isinstance(dq_details, Mapping):
            reject_section = dq_details.get("rejects")
            if isinstance(reject_section, Mapping):
                dataset_id = reject_section.get("dataset_id")
                if dataset_id:
                    reject_ref: dict[str, Any] = {
                        "dataset": dataset_id,
                        "governed": bool(reject_section.get("governed", True)),
                    }
                    version = reject_section.get("dataset_version")
                    if isinstance(version, str) and version:
                        reject_ref["version"] = version
                    row_count = reject_section.get("row_count")
                    if isinstance(row_count, (int, float)):
                        reject_ref["row_count"] = int(row_count)
                    path = reject_section.get("path")
                    if isinstance(path, str) and path:
                        reject_ref["path"] = path
                    reject_refs.append(reject_ref)
        timeline: list[Mapping[str, Any]] = []
        if isinstance(dq_details, Mapping):
            candidate = dq_details.get("timeline")
            if isinstance(candidate, list):
                timeline = [
                    item for item in candidate if isinstance(item, Mapping)
                ]

        history_entries.append(
            {
                "record": record_payload,
                "output_details": output_details,
                "failed_expectations": failed_expectations,
                "schema_errors": schema_errors,
                "dq_aux": dq_aux,
                "input_payloads": input_payloads,
                "input_refs": input_refs,
                "reject_refs": reject_refs,
                "timeline": timeline,
                "streaming_batches": streaming_batches,
            }
        )

    params_cfg = scenario_cfg.get("params", {})
    latest_record = scenario_row.get("latest")
    latest_timeline: list[Mapping[str, Any]] = []
    latest_batches: list[dict[str, Any]] = []
    if isinstance(latest_record, Mapping):
        latest_dq = latest_record.get("dq_details")
        if isinstance(latest_dq, Mapping):
            candidate = latest_dq.get("timeline")
            if isinstance(candidate, list):
                latest_timeline = [
                    item for item in candidate if isinstance(item, Mapping)
                ]
            output_details = latest_dq.get("output")
            if isinstance(output_details, Mapping):
                latest_batches = _normalise_streaming_batches(
                    output_details.get("streaming_batches")
                )
    category_key = scenario_row.get("category", "contract")
    category_label = CATEGORY_LABELS.get(
        category_key, category_key.replace("-", " ").title()
    )

    flash_token = request.query_params.get("flash")
    flash_message: str | None = None
    flash_error: str | None = None
    if flash_token:
        flash_message, flash_error = pop_flash(flash_token)
    else:
        flash_message = request.query_params.get("msg")
        flash_error = request.query_params.get("error")

    context = {
        "request": request,
        "scenario_key": scenario_key,
        "scenario": scenario_cfg,
        "scenario_row": scenario_row,
        "mode_options": list(scenario_cfg.get("mode_options", [])),
        "default_mode": scenario_cfg.get("params", {}).get("mode"),
        "latest_record": latest_record,
        "history_entries": history_entries,
        "has_history": bool(history_entries),
        "dataset_name": dataset_name,
        "latest_timeline": latest_timeline,
        "latest_batches": latest_batches,
        "category_label": category_label,
        "guide_sections": scenario_cfg.get("guide", []),
        "scenario_params": params_cfg,
        "activate_versions": scenario_cfg.get("activate_versions", {}),
        "status_badges": STATUS_BADGES,
        "message": flash_message,
        "error": flash_error,
    }
    return templates.TemplateResponse("pipeline_run_detail.html", context)


@app.post("/pipeline/run", response_class=HTMLResponse)
async def run_pipeline_endpoint(
    request: Request,
    scenario: str = Form(...),
    mode: str | None = Form(None),
) -> HTMLResponse:
    from .pipeline import run_pipeline

    cfg = SCENARIOS.get(scenario)
    if not cfg:
        params = urlencode({"error": f"Unknown scenario: {scenario}"})
        return RedirectResponse(url=f"/datasets?{params}", status_code=303)
    params_cfg = dict(cfg.get("params", {}))
    mode_options = [
        option
        for option in cfg.get("mode_options", [])
        if isinstance(option, Mapping) and option.get("mode")
    ]
    default_mode = params_cfg.get("mode") or (mode_options[0]["mode"] if mode_options else "pipeline")
    selected_mode = (mode or default_mode or "pipeline").lower()
    if selected_mode == "spark":
        selected_mode = "pipeline"
    for dataset, version in cfg.get("activate_versions", {}).items():
        try:
            set_active_version(dataset, version)
        except FileNotFoundError:
            continue
    try:
        if selected_mode == "streaming":
            seconds = params_cfg.get("seconds", 5)
            try:
                seconds_int = int(seconds)
            except (TypeError, ValueError):
                seconds_int = 5
            dataset_name, new_version = await asyncio.to_thread(
                streaming_demo.run_streaming_scenario,
                scenario,
                seconds=seconds_int,
                run_type=params_cfg.get("run_type", "observe"),
                progress=None,
            )
        elif selected_mode == "dlt":
            from .dlt_pipeline import run_dlt_pipeline

            call_params = dict(params_cfg)
            call_params.pop("mode", None)
            dataset_name, new_version = await asyncio.to_thread(
                run_dlt_pipeline,
                call_params.get("contract_id"),
                call_params.get("contract_version"),
                call_params.get("dataset_name"),
                call_params.get("dataset_version"),
                call_params.get("run_type", "infer"),
                collect_examples=call_params.get("collect_examples", False),
                examples_limit=call_params.get("examples_limit", 5),
                violation_strategy=call_params.get("violation_strategy"),
                enforce_contract_status=call_params.get("enforce_contract_status"),
                inputs=call_params.get("inputs"),
                output_adjustment=call_params.get("output_adjustment"),
                data_product_flow=call_params.get("data_product_flow"),
                scenario_key=scenario,
            )
        else:
            call_params = dict(params_cfg)
            call_params.pop("mode", None)
            dataset_name, new_version = await asyncio.to_thread(
                run_pipeline,
                call_params.get("contract_id"),
                call_params.get("contract_version"),
                call_params.get("dataset_name"),
                call_params.get("dataset_version"),
                call_params.get("run_type", "infer"),
                collect_examples=call_params.get("collect_examples", False),
                examples_limit=call_params.get("examples_limit", 5),
                violation_strategy=call_params.get("violation_strategy"),
                enforce_contract_status=call_params.get("enforce_contract_status"),
                inputs=call_params.get("inputs"),
                output_adjustment=call_params.get("output_adjustment"),
                data_product_flow=call_params.get("data_product_flow"),
                scenario_key=scenario,
            )
        label = (
            dataset_name
            or params_cfg.get("dataset_name")
            or params_cfg.get("contract_id")
            or "dataset"
        )
        message = f"Run succeeded: {label} {new_version}"
        token = queue_flash(message=message)
        params_qs = urlencode({"flash": token})
        detail_url = f"/pipeline-runs/{scenario}?{params_qs}"
        wants_json = "application/json" in request.headers.get("accept", "").lower()
        if wants_json:
            return JSONResponse(
                {
                    "status": "success",
                    "message": message,
                    "dataset_name": dataset_name,
                    "dataset_version": new_version,
                    "detail_url": detail_url,
                    "mode": selected_mode,
                }
            )
        return RedirectResponse(url=detail_url, status_code=303)
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        logger.exception("Pipeline run failed for scenario %s", scenario)
        error_message = str(exc)
        token = queue_flash(error=error_message)
        params_qs = urlencode({"flash": token})
        detail_url = f"/pipeline-runs/{scenario}?{params_qs}"
        wants_json = "application/json" in request.headers.get("accept", "").lower()
        if wants_json:
            return JSONResponse(
                {
                    "status": "error",
                    "message": error_message,
                    "detail_url": detail_url,
                    "mode": selected_mode,
                },
                status_code=500,
            )
        return RedirectResponse(url=detail_url, status_code=303)


@app.post("/pipeline/run/streaming", response_class=JSONResponse)
async def run_streaming_endpoint(scenario: str = Form(...)) -> JSONResponse:
    cfg = SCENARIOS.get(scenario)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {scenario}")
    params_cfg = cfg.get("params", {})
    if params_cfg.get("mode") != "streaming":
        raise HTTPException(status_code=400, detail="Scenario is not configured for streaming")

    for dataset, version in cfg.get("activate_versions", {}).items():
        try:
            set_active_version(dataset, version)
        except FileNotFoundError:
            continue

    try:
        seconds = int(params_cfg.get("seconds", 5))
    except (TypeError, ValueError):
        seconds = 5

    loop = asyncio.get_running_loop()
    state = StreamingRunState(
        scenario_key=scenario,
        seconds=seconds,
        run_type=params_cfg.get("run_type", "observe"),
        loop=loop,
    )
    _STREAMING_RUNS[state.run_id] = state

    progress = StreamingRunProgress(state)
    state.publish(
        {
            "type": "started",
            "scenario": scenario,
            "seconds": seconds,
            "run_type": params_cfg.get("run_type", "observe"),
        }
    )

    async def _runner() -> None:
        try:
            dataset_name, dataset_version = await asyncio.to_thread(
                streaming_demo.run_streaming_scenario,
                scenario,
                seconds=seconds,
                run_type=params_cfg.get("run_type", "observe"),
                progress=progress,
            )
            state.dataset_name = dataset_name
            state.dataset_version = dataset_version
        except Exception as exc:  # pragma: no cover - surfaced to client via progress
            logger.exception("Streaming scenario failed for %s", scenario)
            state.publish(
                {
                    "type": "error",
                    "scenario": scenario,
                    "message": str(exc),
                }
            )
        finally:
            state.finish()

    asyncio.create_task(_runner())
    return JSONResponse({"run_id": state.run_id})


@app.get("/pipeline/streaming-progress/{run_id}", response_class=StreamingResponse)
async def streaming_progress_endpoint(run_id: str) -> StreamingResponse:
    state = _STREAMING_RUNS.get(run_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Unknown streaming run")

    async def _event_stream() -> Iterable[str]:
        try:
            while True:
                event = await state.queue.get()
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in {"complete", "error"}:
                    break
        finally:
            _STREAMING_RUNS.pop(run_id, None)

    return StreamingResponse(_event_stream(), media_type="text/event-stream")


def run() -> None:  # pragma: no cover - convenience runner
    from .runner import main as _main

    _main()
