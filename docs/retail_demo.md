# Altair Retail demo module

The demo application now bundles a fictitious retail company named **Altair Retail**.
It showcases how dc43 packages source-aligned data products, modeled layers, a
lightweight machine learning artifact, and downstream consumer products into a
single walkthrough that is ideal for workshops or long-form demos.

## Data products

| Product | Purpose | Key ports |
| --- | --- | --- |
| `dp.retail-foundation` | Curates operational sources spanning POS, inventory, and merchandising master data. | `pos-transactions`, `inventory-snapshot`, `product-master` |
| `dp.retail-insights` | Publishes the retail star schema – sales fact plus store, product, and date dimensions. | `sales-fact`, `store-dimension`, `product-dimension`, `date-dimension` |
| `dp.retail-intelligence` | Hosts the internal feature store and demand forecast outputs. | `demand-features` *(internal)*, `demand-forecast` |
| `dp.retail-experience` | Activates personalized offers for marketing teams. | `personalized-offers` |
| `dp.retail-analytics` | Serves governed KPIs with semantic metadata for BI tooling. | `executive-metrics` |

Each product keeps track of its upstream dependencies via the `RETAIL_DATASETS`
registry in `dc43_demo_app.retail_demo.data`. Internal datasets such as
`retail_demand_features` are flagged so the demo can highlight artefacts that
stay within the product boundary but never expose a public port.

## Layered architecture

The Altair Retail flow emphasises that data products are larger than individual
tables. Each package groups the datasets required for its specific audience:

- **Foundation** ingests point-of-sale transactions, on-hand inventory, and the
  merchandising catalog so operational teams can rely on a single governed
  source.
- **Insights** reshapes those feeds into a snowflake schema, outputting the
  `retail_sales_fact` alongside the store, product, and date dimensions that
  power both analytics and machine learning.
- **Intelligence** consumes the star schema to build the internal
  `retail_demand_features` dataset and publishes the scored
  `retail_demand_forecast` as its exposed port.
- **Experience** combines forecasts with store context to activate
  `retail_personalized_offers` for marketing teams.
- **Analytics** layers business-ready semantics on the star schema plus
  forecast outputs to materialise `retail_kpi_mart` for executive dashboards.

## Sample pipeline

The `dc43_demo_app.retail_demo.pipeline.run_retail_demo` helper stitches together
all datasets:

```python
from dc43_demo_app.retail_demo import run_retail_demo

run = run_retail_demo()
print(f"Sales fact rows: {len(run.star_schema.sales_fact)}")
print(f"Store dimension rows: {len(run.star_schema.store_dimension)}")
print(f"Feature rows: {len(run.demand_features)}")
print(f"Forecast outputs: {len(run.forecasts)}")
for metric in run.kpis:
    print(metric["metric_id"], metric["value"])
```

The helper performs the following steps:

1. Load the source fixtures (POS, inventory, catalog) from the package.
2. Assemble the retail star schema (`retail_sales_fact` plus store/product/date
   dimensions) directly from those sources.
3. Derive the internal `retail_demand_features` dataset and score the
   `DemandForecaster` linear model to produce `retail_demand_forecast` rows.
4. Curate the personalized offers feed and KPI mart, complete with semantic
   metadata that mirrors the JSON files shipped with the demo.

## KPI semantic layer

`dc43_demo_app.retail_demo.data.RETAIL_SEMANTIC_MEASURES` mirrors the semantic
metadata embedded in `retail_kpi_mart`. Each record carries the KPI expression,
aggregation type, default unit, and optional formatting hints. The web UI links
these definitions to the new “Altair Retail end-to-end” scenario.

## Retail demo UI

The FastAPI demo app now exposes a dedicated **Altair Retail** section instead
of surfacing the walkthrough as a standard pipeline scenario. The entry point at
`/retail-demo` summarises the layered data products and links to two sample
applications:

- **Marketing activation** shows a faux campaign planner that groups the
  personalised offers by store, region, and primary KPI so the audience can talk
  through how forecasts drive the activation surface.
- **Executive dashboard** renders the KPI mart metrics with the semantic layer
  details that business intelligence tools would consume.

The pages are backed by the same `run_retail_demo` helper used in code samples,
so the UI stays in sync with the packaged fixtures and contracts.
