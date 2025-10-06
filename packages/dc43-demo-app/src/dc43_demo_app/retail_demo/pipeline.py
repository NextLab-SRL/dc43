"""High-level orchestration helpers for the retail demo scenario."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from .data import (
    RETAIL_SEMANTIC_MEASURES,
    load_dataset,
)
from .model import DemandForecaster


@dataclass(slots=True)
class RetailStarSchema:
    """Snowflake model assembled for analytics and ML."""

    sales_fact: list[Mapping[str, Any]]
    store_dimension: list[Mapping[str, Any]]
    product_dimension: list[Mapping[str, Any]]
    date_dimension: list[Mapping[str, Any]]


@dataclass(slots=True)
class RetailDemoRun:
    """Container for the artefacts produced by the demo pipeline."""

    transactions: list[Mapping[str, Any]]
    inventory: list[Mapping[str, Any]]
    catalog: list[Mapping[str, Any]]
    star_schema: RetailStarSchema
    demand_features: list[Mapping[str, Any]]
    forecasts: list[Mapping[str, Any]]
    offers: list[Mapping[str, Any]]
    kpis: list[Mapping[str, Any]]


def load_demo_inputs() -> dict[str, list[Mapping[str, Any]]]:
    """Load the raw source datasets bundled with the application."""

    return {
        key: load_dataset(key)
        for key in (
            "retail_pos_transactions",
            "retail_inventory_snapshot",
            "retail_product_catalog",
        )
    }


_STORE_PROFILES: Mapping[str, Mapping[str, Any]] = {
    "NY-001": {
        "store_name": "Altair Manhattan Flagship",
        "region": "Northeast",
        "format": "flagship",
        "square_feet": 28000,
    },
    "SF-001": {
        "store_name": "Altair Market Street",
        "region": "West Coast",
        "format": "urban",
        "square_feet": 18000,
    },
    "CHI-010": {
        "store_name": "Altair Chicago River",
        "region": "Midwest",
        "format": "suburban",
        "square_feet": 22000,
    },
}


def _build_store_dimension(
    transactions: Iterable[Mapping[str, Any]],
    inventory: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    store_ids: set[str] = set()
    for row in transactions:
        store_id = str(row.get("store_id", ""))
        if store_id:
            store_ids.add(store_id)
    for row in inventory:
        store_id = str(row.get("store_id", ""))
        if store_id:
            store_ids.add(store_id)

    dimension: list[dict[str, Any]] = []
    for index, store_id in enumerate(sorted(store_ids), start=1):
        profile = dict(_STORE_PROFILES.get(store_id, {}))
        profile.setdefault("store_name", f"Altair {store_id}")
        profile.setdefault("region", "Unknown")
        profile.setdefault("format", "standard")
        profile.setdefault("square_feet", 15000)
        dimension.append(
            {
                "store_key": f"STORE-{index:02d}",
                "store_id": store_id,
                **profile,
            }
        )
    return dimension


def _build_product_dimension(catalog: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    dimension: list[dict[str, Any]] = []
    for index, row in enumerate(sorted(catalog, key=lambda item: str(item.get("product_id"))), start=1):
        dimension.append(
            {
                "product_key": f"PROD-{index:02d}",
                "product_id": str(row.get("product_id", "")),
                "product_name": str(row.get("product_name", "")),
                "category": str(row.get("category", "")),
                "subcategory": str(row.get("subcategory", "")),
                "department": str(row.get("department", row.get("category", ""))),
                "unit_cost": float(row.get("unit_cost", 0.0)),
            }
        )
    return dimension


def _build_date_dimension(transactions: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    dates: set[str] = set()
    for row in transactions:
        ts = str(row.get("transaction_ts", ""))
        if ts:
            dates.add(ts[:10])
    dimension: list[dict[str, Any]] = []
    for date_str in sorted(dates):
        dt = datetime.fromisoformat(date_str)
        iso_calendar = dt.isocalendar()
        dimension.append(
            {
                "date_key": date_str.replace("-", ""),
                "business_date": date_str,
                "day_of_week": dt.strftime("%A"),
                "week_of_year": iso_calendar.week,
                "month": dt.month,
                "quarter": (dt.month - 1) // 3 + 1,
                "year": dt.year,
            }
        )
    return dimension


def _inventory_lookup(inventory: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], int]:
    lookup: dict[tuple[str, str], int] = {}
    for row in inventory:
        store_id = str(row.get("store_id", ""))
        product_id = str(row.get("product_id", ""))
        lookup[(store_id, product_id)] = int(row.get("on_hand", 0))
    return lookup


def build_star_schema(
    transactions: Iterable[Mapping[str, Any]],
    inventory: Iterable[Mapping[str, Any]],
    catalog: Iterable[Mapping[str, Any]],
) -> RetailStarSchema:
    """Assemble a snowflake schema from operational sources."""

    store_dimension = _build_store_dimension(transactions, inventory)
    product_dimension = _build_product_dimension(catalog)
    date_dimension = _build_date_dimension(transactions)

    store_by_id = {row["store_id"]: row for row in store_dimension}
    store_by_key = {row["store_key"]: row for row in store_dimension}
    product_by_id = {row["product_id"]: row for row in product_dimension}
    product_by_key = {row["product_key"]: row for row in product_dimension}
    date_by_date = {row["business_date"]: row for row in date_dimension}

    inventory_lookup = _inventory_lookup(inventory)
    fact: dict[tuple[str, str, str], dict[str, Any]] = {}

    for row in transactions:
        product_id = str(row.get("product_id", ""))
        store_id = str(row.get("store_id", ""))
        ts = str(row.get("transaction_ts", ""))
        business_date = ts[:10] if ts else ""
        if not (product_id and store_id and business_date):
            continue
        product_info = product_by_id.get(product_id)
        store_info = store_by_id.get(store_id)
        date_info = date_by_date.get(business_date)
        if not (product_info and store_info and date_info):
            continue
        key = (date_info["date_key"], store_info["store_key"], product_info["product_key"])
        bucket = fact.setdefault(
            key,
            {
                "date_key": key[0],
                "business_date": date_info["business_date"],
                "store_key": key[1],
                "product_key": key[2],
                "store_id": store_id,
                "product_id": product_id,
                "units_sold": 0,
                "gross_sales": 0.0,
                "gross_margin": 0.0,
            },
        )
        quantity = float(row.get("quantity", 0.0))
        price = float(row.get("unit_price", 0.0))
        cost = float(product_info.get("unit_cost", 0.0))
        bucket["units_sold"] += int(quantity)
        bucket["gross_sales"] += quantity * price
        bucket["gross_margin"] += quantity * (price - cost)

    sales_fact: list[dict[str, Any]] = []
    for (date_key, store_key, product_key), bucket in sorted(fact.items()):
        store_info = store_by_key[store_key]
        product_info = product_by_key[product_key]
        store_id = store_info["store_id"]
        product_id = product_info["product_id"]
        on_hand = inventory_lookup.get((store_id, product_id), 0)
        units = int(bucket["units_sold"])
        sales = float(bucket["gross_sales"])
        margin = float(bucket["gross_margin"])
        denom = units + on_hand
        sell_through = units / denom if denom else 0.0
        margin_pct = margin / sales if sales else 0.0
        sales_fact.append(
            {
                **bucket,
                "category": product_info.get("category", ""),
                "inventory_on_hand": on_hand,
                "sell_through_rate": round(sell_through, 4),
                "gross_margin_pct": round(margin_pct, 4),
            }
        )

    return RetailStarSchema(
        sales_fact=sales_fact,
        store_dimension=store_dimension,
        product_dimension=product_dimension,
        date_dimension=date_dimension,
    )


def prepare_demand_features(star_schema: RetailStarSchema) -> list[dict[str, Any]]:
    """Curate ML-ready features from the analytics star schema."""

    product_by_key = {row["product_key"]: row for row in star_schema.product_dimension}
    store_by_key = {row["store_key"]: row for row in star_schema.store_dimension}
    date_by_key = {row["date_key"]: row for row in star_schema.date_dimension}

    features: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in star_schema.sales_fact:
        date_key = str(row.get("date_key", ""))
        store_key = str(row.get("store_key", ""))
        product_key = str(row.get("product_key", ""))
        product_info = product_by_key.get(product_key)
        store_info = store_by_key.get(store_key)
        date_info = date_by_key.get(date_key)
        if not (product_info and store_info and date_info):
            continue
        category = str(product_info.get("category", ""))
        key = (date_key, store_key, category)
        bucket = features.setdefault(
            key,
            {
                "date_key": date_key,
                "business_date": date_info["business_date"],
                "store_key": store_key,
                "store_id": store_info["store_id"],
                "category": category,
                "region": store_info.get("region", ""),
                "format": store_info.get("format", ""),
                "feature_version": "2024.03.demo",
                "units_sold": 0,
                "gross_sales": 0.0,
                "gross_margin": 0.0,
                "inventory_on_hand": 0,
            },
        )
        bucket["units_sold"] += int(row.get("units_sold", 0))
        bucket["gross_sales"] += float(row.get("gross_sales", 0.0))
        bucket["gross_margin"] += float(row.get("gross_margin", 0.0))
        bucket["inventory_on_hand"] += int(row.get("inventory_on_hand", 0))

    feature_rows: list[dict[str, Any]] = []
    for key, bucket in sorted(features.items()):
        units = int(bucket["units_sold"])
        inventory = int(bucket["inventory_on_hand"])
        sales = float(bucket["gross_sales"])
        margin = float(bucket["gross_margin"])
        denom = units + inventory
        sell_through = units / denom if denom else 0.0
        margin_pct = margin / sales if sales else 0.0
        feature_rows.append(
            {
                **bucket,
                "sell_through_rate": round(sell_through, 4),
                "gross_margin_pct": round(margin_pct, 4),
            }
        )
    return feature_rows


_OFFER_BLUEPRINTS: Mapping[tuple[str, str], Mapping[str, Any]] = {
    ("NY-001", "Electronics"): {
        "customer_id": "CUST-1001",
        "offer_id": "OFF-ELITE-01",
        "recommended_discount": 0.10,
        "primary_metric": "sell_through",
        "rationale": "Boost smartphone sell-through ahead of weekend traffic",
    },
    ("NY-001", "Home"): {
        "customer_id": "CUST-1040",
        "offer_id": "OFF-HOME-05",
        "recommended_discount": 0.08,
        "primary_metric": "margin_guardrail",
        "rationale": "Clear lamp inventory while staying above 35% margin",
    },
    ("SF-001", "Outdoors"): {
        "customer_id": "CUST-2005",
        "offer_id": "OFF-OUT-09",
        "recommended_discount": 0.12,
        "primary_metric": "forecast_accuracy",
        "rationale": "Align camping stove promo with demand spike from ML forecast",
    },
    ("CHI-010", "Home"): {
        "customer_id": "CUST-3102",
        "offer_id": "OFF-HOME-05",
        "recommended_discount": 0.07,
        "primary_metric": "inventory_turn",
        "rationale": "Encourage repeat purchase to lift Midwest lighting turn",
    },
}


def generate_offers(
    forecasts: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Construct customer-facing offers using the forecast output."""

    feature_lookup: dict[tuple[str, str], Mapping[str, Any]] = {
        (str(row.get("store_id")), str(row.get("category"))): row for row in feature_rows
    }
    offers: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    by_store: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in forecasts:
        by_store[str(row.get("store_id"))].append(row)
    for store_id, rows in by_store.items():
        ranked = sorted(rows, key=lambda item: float(item.get("forecast_units", 0.0)), reverse=True)
        for row in ranked:
            category = str(row.get("category"))
            key = (store_id, category)
            if key in seen:
                continue
            blueprint = _OFFER_BLUEPRINTS.get(key)
            if not blueprint:
                continue
            feature_row = feature_lookup.get(key, {})
            offers.append(
                {
                    "customer_id": blueprint["customer_id"],
                    "store_key": str(feature_row.get("store_key", "")),
                    "store_id": store_id,
                    "business_date": str(row.get("business_date", "")),
                    "offer_id": blueprint["offer_id"],
                    "category": category,
                    "recommended_discount": blueprint["recommended_discount"],
                    "forecast_units": float(row.get("forecast_units", 0.0)),
                    "primary_metric": blueprint["primary_metric"],
                    "confidence": float(row.get("confidence", 0.0)),
                    "sell_through_rate": float(feature_row.get("sell_through_rate", 0.0)),
                    "region": str(feature_row.get("region", "")),
                    "rationale": blueprint["rationale"],
                }
            )
            seen.add(key)
    offers.sort(key=lambda row: (row["store_id"], row["category"]))
    return offers


def build_kpi_mart(
    star_schema: RetailStarSchema,
    forecasts: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Compute KPI metrics that align with the semantic layer definitions."""

    sales_fact = star_schema.sales_fact
    gross_sales = sum(float(row.get("gross_sales", 0.0)) for row in sales_fact)
    gross_margin = sum(float(row.get("gross_margin", 0.0)) for row in sales_fact)
    sell_through_avg = (
        sum(float(row.get("sell_through_rate", 0.0)) for row in sales_fact)
        / len(sales_fact)
        if sales_fact
        else 0.0
    )
    inventory_at_risk = sum(
        int(row.get("inventory_on_hand", 0))
        for row in sales_fact
        if float(row.get("sell_through_rate", 0.0)) < 0.1
    )
    total_units = sum(int(row.get("units_sold", 0)) for row in sales_fact)
    forecast_units = sum(float(row.get("forecast_units", 0.0)) for row in forecasts)
    bias = (forecast_units - total_units) / total_units if total_units else 0.0

    semantic_lookup = {measure.metric_id: measure for measure in RETAIL_SEMANTIC_MEASURES}
    metrics = {
        "gross_sales_total": gross_sales,
        "gross_margin_total": gross_margin,
        "sell_through_avg": sell_through_avg,
        "inventory_at_risk": inventory_at_risk,
        "forecast_bias": bias,
    }
    business_date = star_schema.date_dimension[0]["business_date"] if star_schema.date_dimension else ""
    results: list[dict[str, Any]] = []
    for metric_id, value in metrics.items():
        measure = semantic_lookup.get(metric_id)
        format_hint = measure.format if measure else None
        results.append(
            {
                "business_date": business_date,
                "metric_id": metric_id,
                "metric_label": measure.label if measure else metric_id.replace("_", " ").title(),
                "value": round(float(value) + 1e-9, 4),
                "unit": measure.unit if measure else "",
                "semantic_layer": {
                    "type": "measure",
                    "aggregation": measure.aggregation if measure else "custom",
                    "expression": measure.expression if measure else metric_id,
                    "description": measure.description if measure else "",
                    **({"format": format_hint} if format_hint else {}),
                },
            }
        )
    return results


def run_retail_demo() -> RetailDemoRun:
    """Execute the end-to-end retail walkthrough using the bundled fixtures."""

    inputs = load_demo_inputs()
    transactions = inputs["retail_pos_transactions"]
    inventory = inputs["retail_inventory_snapshot"]
    catalog = inputs["retail_product_catalog"]
    star_schema = build_star_schema(transactions, inventory, catalog)

    demand_features = prepare_demand_features(star_schema)
    forecaster = DemandForecaster()
    forecasts = forecaster.predict(demand_features)

    offers = generate_offers(forecasts, demand_features)
    kpis = build_kpi_mart(star_schema, forecasts)

    return RetailDemoRun(
        transactions=transactions,
        inventory=inventory,
        catalog=catalog,
        star_schema=star_schema,
        demand_features=demand_features,
        forecasts=forecasts,
        offers=offers,
        kpis=kpis,
    )


__all__ = [
    "RetailDemoRun",
    "RetailStarSchema",
    "build_kpi_mart",
    "build_star_schema",
    "generate_offers",
    "load_demo_inputs",
    "prepare_demand_features",
    "run_retail_demo",
]
