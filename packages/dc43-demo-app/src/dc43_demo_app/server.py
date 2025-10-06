from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode

from collections import Counter, defaultdict
from dataclasses import asdict

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
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
from .retail_demo import RETAIL_DATA_PRODUCTS, RetailDemoRun, run_retail_demo

CATEGORY_LABELS = {
    "contract": "Contract-focused pipelines",
    "data-product": "Data product pipelines",
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
PIPELINE_TEMPLATE_DIR = BASE_DIR / "templates"

_RETAIL_RUN: RetailDemoRun | None = None

_env_loader = ChoiceLoader(
    [
        FileSystemLoader(str(PIPELINE_TEMPLATE_DIR)),
        FileSystemLoader(str(CONTRACTS_TEMPLATE_DIR)),
    ]
)

template_env = Environment(loader=_env_loader, autoescape=select_autoescape(["html", "xml"]))
templates = Jinja2Templates(env=template_env)


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
    for product in RETAIL_DATA_PRODUCTS.values():
        data = asdict(product)
        data["inputs"] = list(product.inputs)
        data["outputs"] = list(product.outputs)
        data["tags"] = list(product.tags)
        cards.append(data)
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
    metrics = [_format_metric_card(metric) for metric in run.kpis[:3]]
    context = {
        "request": request,
        "products": _product_cards(),
        "store_cards": _store_offer_cards(run),
        "kpi_preview": metrics,
        "business_date": _retail_business_date(run),
    }
    return templates.TemplateResponse("retail_overview.html", context)


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
        output_details = dq_details.get("output", {}) if isinstance(dq_details, Mapping) else {}
        if not isinstance(output_details, Mapping):
            output_details = {}
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
                if key == "output":
                    continue
                input_payloads.append({"name": key, "payload": payload})

        history_entries.append(
            {
                "record": record_payload,
                "output_details": output_details,
                "failed_expectations": failed_expectations,
                "schema_errors": schema_errors,
                "dq_aux": dq_aux,
                "input_payloads": input_payloads,
            }
        )

    params_cfg = scenario_cfg.get("params", {})
    latest_record = scenario_row.get("latest")
    category_key = scenario_row.get("category", "contract")
    category_label = CATEGORY_LABELS.get(
        category_key, category_key.replace("-", " ").title()
    )

    context = {
        "request": request,
        "scenario_key": scenario_key,
        "scenario": scenario_cfg,
        "scenario_row": scenario_row,
        "latest_record": latest_record,
        "history_entries": history_entries,
        "has_history": bool(history_entries),
        "dataset_name": dataset_name,
        "category_label": category_label,
        "guide_sections": scenario_cfg.get("guide", []),
        "scenario_params": params_cfg,
        "activate_versions": scenario_cfg.get("activate_versions", {}),
        "status_badges": STATUS_BADGES,
    }
    return templates.TemplateResponse("pipeline_run_detail.html", context)


@app.post("/pipeline/run", response_class=HTMLResponse)
async def run_pipeline_endpoint(scenario: str = Form(...)) -> HTMLResponse:
    from .pipeline import run_pipeline

    cfg = SCENARIOS.get(scenario)
    if not cfg:
        params = urlencode({"error": f"Unknown scenario: {scenario}"})
        return RedirectResponse(url=f"/datasets?{params}", status_code=303)
    params_cfg = cfg["params"]
    for dataset, version in cfg.get("activate_versions", {}).items():
        try:
            set_active_version(dataset, version)
        except FileNotFoundError:
            continue
    try:
        dataset_name, new_version = await asyncio.to_thread(
            run_pipeline,
            params_cfg.get("contract_id"),
            params_cfg.get("contract_version"),
            params_cfg.get("dataset_name"),
            params_cfg.get("dataset_version"),
            params_cfg.get("run_type", "infer"),
            collect_examples=params_cfg.get("collect_examples", False),
            examples_limit=params_cfg.get("examples_limit", 5),
            violation_strategy=params_cfg.get("violation_strategy"),
            enforce_contract_status=params_cfg.get("enforce_contract_status"),
            inputs=params_cfg.get("inputs"),
            output_adjustment=params_cfg.get("output_adjustment"),
            data_product_flow=params_cfg.get("data_product_flow"),
            scenario_key=scenario,
        )
        label = (
            dataset_name
            or params_cfg.get("dataset_name")
            or params_cfg.get("contract_id")
            or "dataset"
        )
        token = queue_flash(message=f"Run succeeded: {label} {new_version}")
        params_qs = urlencode({"flash": token})
    except Exception as exc:  # pragma: no cover - surface pipeline errors
        logger.exception("Pipeline run failed for scenario %s", scenario)
        token = queue_flash(error=str(exc))
        params_qs = urlencode({"flash": token})
    return RedirectResponse(url=f"/pipeline-runs?{params_qs}", status_code=303)


def run() -> None:  # pragma: no cover - convenience runner
    from .runner import main as _main

    _main()
