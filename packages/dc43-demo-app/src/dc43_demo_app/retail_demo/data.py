"""Static metadata describing the retail demo datasets and products."""

from __future__ import annotations

import json
from dataclasses import dataclass
from importlib import resources
from typing import Any, Iterable, Mapping, MutableMapping

_DATA_ROOT = resources.files("dc43_demo_app") / "demo_data" / "data"


@dataclass(frozen=True)
class RetailDataset:
    """Definition of a dataset used in the retail walkthrough."""

    identifier: str
    contract_id: str
    dataset_version: str
    file_name: str
    kind: str
    description: str
    data_product_id: str | None = None
    output_port: str | None = None
    dependencies: tuple[str, ...] = ()
    internal: bool = False

    def resource_path(self) -> resources.abc.Traversable:
        """Return the importlib resource pointing at the dataset contents."""

        return _DATA_ROOT.joinpath(self.identifier, self.dataset_version, self.file_name)

    def load_records(self) -> list[Mapping[str, Any]]:
        """Load the JSON payload backing this dataset."""

        resource = self.resource_path()
        try:
            text = resource.read_text(encoding="utf-8")
        except FileNotFoundError as exc:  # pragma: no cover - defensive
            raise FileNotFoundError(f"Sample data for {self.identifier} not found") from exc
        payload = json.loads(text)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, Mapping)]
        if isinstance(payload, Mapping):
            return [payload]
        return []


@dataclass(frozen=True)
class RetailDataProduct:
    """Lightweight summary of a data product published in the demo."""

    identifier: str
    name: str
    summary: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    owner: str
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class RetailSemanticMeasure:
    """Describe a KPI surfaced by the executive mart."""

    metric_id: str
    label: str
    aggregation: str
    expression: str
    unit: str
    description: str
    format: str | None = None


RETAIL_DATASETS: Mapping[str, RetailDataset] = {
    "retail_pos_transactions": RetailDataset(
        identifier="retail_pos_transactions",
        contract_id="retail_pos_transactions",
        dataset_version="2024-03-31",
        file_name="transactions.json",
        kind="source",
        description="Raw register transactions captured from Altair Retail stores.",
        data_product_id="dp.retail-foundation",
        output_port="pos-transactions",
    ),
    "retail_inventory_snapshot": RetailDataset(
        identifier="retail_inventory_snapshot",
        contract_id="retail_inventory_snapshot",
        dataset_version="2024-03-31",
        file_name="inventory.json",
        kind="source",
        description="Start-of-day on-hand inventory counts per store and SKU.",
        data_product_id="dp.retail-foundation",
        output_port="inventory-snapshot",
    ),
    "retail_product_catalog": RetailDataset(
        identifier="retail_product_catalog",
        contract_id="retail_product_catalog",
        dataset_version="2024-03-31",
        file_name="products.json",
        kind="reference",
        description="Merchandising master data with product categories and costs.",
        data_product_id="dp.retail-foundation",
        output_port="product-master",
    ),
    "retail_sales_fact": RetailDataset(
        identifier="retail_sales_fact",
        contract_id="retail_sales_fact",
        dataset_version="2024-03-31",
        file_name="sales_fact.json",
        kind="modeled",
        description="Fact table at the store/product grain that feeds analytics and ML.",
        data_product_id="dp.retail-insights",
        output_port="sales-fact",
        dependencies=(
            "retail_pos_transactions",
            "retail_inventory_snapshot",
            "retail_product_catalog",
        ),
    ),
    "retail_store_dimension": RetailDataset(
        identifier="retail_store_dimension",
        contract_id="retail_store_dimension",
        dataset_version="2024-03-31",
        file_name="store_dimension.json",
        kind="dimension",
        description="Store attributes that describe regions, formats, and footprint.",
        data_product_id="dp.retail-insights",
        output_port="store-dimension",
        dependencies=("retail_pos_transactions", "retail_inventory_snapshot"),
    ),
    "retail_product_dimension": RetailDataset(
        identifier="retail_product_dimension",
        contract_id="retail_product_dimension",
        dataset_version="2024-03-31",
        file_name="product_dimension.json",
        kind="dimension",
        description="Product catalog enriched with merchandising groupings.",
        data_product_id="dp.retail-insights",
        output_port="product-dimension",
        dependencies=("retail_product_catalog",),
    ),
    "retail_date_dimension": RetailDataset(
        identifier="retail_date_dimension",
        contract_id="retail_date_dimension",
        dataset_version="2024-03-31",
        file_name="date_dimension.json",
        kind="dimension",
        description="Calendar attributes for the business date grain.",
        data_product_id="dp.retail-insights",
        output_port="date-dimension",
        dependencies=("retail_pos_transactions",),
    ),
    "retail_demand_features": RetailDataset(
        identifier="retail_demand_features",
        contract_id="retail_demand_features",
        dataset_version="2024-03-31",
        file_name="demand_features.json",
        kind="feature",
        description="Aggregated store and category features powering the demand model.",
        data_product_id="dp.retail-intelligence",
        output_port="demand-features",
        dependencies=("retail_sales_fact", "retail_store_dimension", "retail_product_dimension"),
        internal=True,
    ),
    "retail_demand_forecast": RetailDataset(
        identifier="retail_demand_forecast",
        contract_id="retail_demand_forecast",
        dataset_version="2024-03-31",
        file_name="forecast.json",
        kind="ml",
        description="Machine learning output forecasting demand per store and category.",
        data_product_id="dp.retail-intelligence",
        output_port="demand-forecast",
        dependencies=("retail_demand_features",),
    ),
    "retail_personalized_offers": RetailDataset(
        identifier="retail_personalized_offers",
        contract_id="retail_personalized_offers",
        dataset_version="2024-03-31",
        file_name="offers.json",
        kind="consumer",
        description="Offer recommendations surfaced to digital experience teams.",
        data_product_id="dp.retail-experience",
        output_port="personalized-offers",
        dependencies=(
            "retail_demand_forecast",
            "retail_sales_fact",
            "retail_store_dimension",
        ),
    ),
    "retail_kpi_mart": RetailDataset(
        identifier="retail_kpi_mart",
        contract_id="retail_kpi_mart",
        dataset_version="2024-03-31",
        file_name="kpis.json",
        kind="kpi",
        description="Executive-ready KPI mart with semantic layer metadata.",
        data_product_id="dp.retail-analytics",
        output_port="executive-metrics",
        dependencies=(
            "retail_sales_fact",
            "retail_store_dimension",
            "retail_product_dimension",
            "retail_date_dimension",
            "retail_demand_forecast",
        ),
    ),
}


RETAIL_DATA_PRODUCTS: Mapping[str, RetailDataProduct] = {
    "dp.retail-foundation": RetailDataProduct(
        identifier="dp.retail-foundation",
        name="Retail operational foundation",
        summary="Curated source feeds spanning POS, inventory, and merchandising master data.",
        inputs=(),
        outputs=(
            "retail_pos_transactions",
            "retail_inventory_snapshot",
            "retail_product_catalog",
        ),
        owner="store-operations",
        tags=("bronze", "operational"),
    ),
    "dp.retail-insights": RetailDataProduct(
        identifier="dp.retail-insights",
        name="Retail star schema",
        summary="Snowflake model exposing the sales fact with store, product, and date dimensions.",
        inputs=(
            "retail_pos_transactions",
            "retail_inventory_snapshot",
            "retail_product_catalog",
        ),
        outputs=(
            "retail_sales_fact",
            "retail_store_dimension",
            "retail_product_dimension",
            "retail_date_dimension",
        ),
        owner="retail-intelligence",
        tags=("silver", "analytics"),
    ),
    "dp.retail-intelligence": RetailDataProduct(
        identifier="dp.retail-intelligence",
        name="Retail demand intelligence",
        summary="Feature engineering and forecasting assets that enrich planning workflows.",
        inputs=(
            "retail_sales_fact",
            "retail_store_dimension",
            "retail_product_dimension",
        ),
        outputs=("retail_demand_features", "retail_demand_forecast"),
        owner="retail-ml",
        tags=("ml", "prediction"),
    ),
    "dp.retail-experience": RetailDataProduct(
        identifier="dp.retail-experience",
        name="Customer experience",
        summary="Offer orchestration dataset combining demand and campaign guardrails.",
        inputs=(
            "retail_demand_forecast",
            "retail_sales_fact",
            "retail_store_dimension",
        ),
        outputs=("retail_personalized_offers",),
        owner="retail-marketing",
        tags=("gold", "consumer"),
    ),
    "dp.retail-analytics": RetailDataProduct(
        identifier="dp.retail-analytics",
        name="Executive KPI mart",
        summary="Governed KPIs with semantic metadata layered on the retail star schema.",
        inputs=(
            "retail_sales_fact",
            "retail_store_dimension",
            "retail_product_dimension",
            "retail_date_dimension",
            "retail_demand_forecast",
        ),
        outputs=("retail_kpi_mart",),
        owner="finance-analytics",
        tags=("platinum", "kpi"),
    ),
}


RETAIL_SEMANTIC_MEASURES: tuple[RetailSemanticMeasure, ...] = (
    RetailSemanticMeasure(
        metric_id="gross_sales_total",
        label="Gross Sales",
        aggregation="sum",
        expression="sum(gross_sales)",
        unit="USD",
        description="Aggregated sales revenue across all stores for the business day.",
        format="currency",
    ),
    RetailSemanticMeasure(
        metric_id="gross_margin_total",
        label="Gross Margin",
        aggregation="sum",
        expression="sum(gross_margin)",
        unit="USD",
        description="Margin retained after product costs but before operating expenses.",
        format="currency",
    ),
    RetailSemanticMeasure(
        metric_id="sell_through_avg",
        label="Sell-through",
        aggregation="average",
        expression="avg(units_sold / (units_sold + inventory_on_hand))",
        unit="ratio",
        description="Average sell-through rate at the store/category grain.",
        format="percent",
    ),
    RetailSemanticMeasure(
        metric_id="inventory_at_risk",
        label="Inventory at Risk",
        aggregation="sum",
        expression="sum(case when sell_through_rate < 0.1 then inventory_on_hand else 0 end)",
        unit="units",
        description="Units sitting in slow-moving categories with sell-through below 10%.",
        format=None,
    ),
    RetailSemanticMeasure(
        metric_id="forecast_bias",
        label="Forecast Bias",
        aggregation="calculated",
        expression="(sum(forecast_units) - sum(units_sold)) / nullif(sum(units_sold), 0)",
        unit="ratio",
        description="Directional accuracy of the demand model across the retail footprint.",
        format="percent",
    ),
)


def load_dataset(identifier: str) -> list[Mapping[str, Any]]:
    """Return the parsed JSON payload for ``identifier`` if available."""

    dataset = RETAIL_DATASETS.get(identifier)
    if not dataset:
        raise KeyError(f"Unknown retail dataset: {identifier}")
    return dataset.load_records()


def dataset_dependencies(identifier: str) -> tuple[str, ...]:
    """Expose upstream dependencies for a dataset by identifier."""

    dataset = RETAIL_DATASETS.get(identifier)
    return dataset.dependencies if dataset else ()


__all__ = [
    "RETAIL_DATA_PRODUCTS",
    "RETAIL_DATASETS",
    "RETAIL_SEMANTIC_MEASURES",
    "RetailDataProduct",
    "RetailDataset",
    "RetailSemanticMeasure",
    "dataset_dependencies",
    "load_dataset",
]
