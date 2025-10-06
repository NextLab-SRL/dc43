# Altair Retail demo module

The demo application now bundles a fictitious retail company named **Altair Retail**.
It showcases how dc43 packages source-aligned data products, modeled layers, a
lightweight machine learning artifact, and downstream consumer products into a
single walkthrough that is ideal for workshops or long-form demos.

## Data products

| Product | Purpose | Key ports |
| --- | --- | --- |
| `dp.retail-foundation` | Curates operational sources spanning POS, inventory, and merchandising master data. *(Tags: source, operational)* | `pos-transactions`, `inventory-snapshot`, `product-master` |
| `dp.retail-insights` | Publishes the retail star schema – sales fact plus store, product, and date dimensions. *(Tags: modelled, analytics)* | `sales-fact`, `store-dimension`, `product-dimension`, `date-dimension` |
| `dp.retail-intelligence` | Hosts the internal feature store and demand forecast outputs. *(Tags: ml, features)* | `demand-features` *(internal)*, `demand-forecast` |
| `dp.retail-experience` | Activates personalized offers for marketing teams. *(Tags: consumer, activation)* | `personalized-offers` |
| `dp.retail-analytics` | Serves governed KPIs with semantic metadata for BI tooling. *(Tags: aggregated, semantic)* | `executive-metrics` |

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
from dc43_demo_app.retail_demo import run_retail_demo, simulate_retail_timeline

run = run_retail_demo()
for event in simulate_retail_timeline(run):
    print(f"{event['date']} · {event['title']} ({event['state_summary']})")
    for step in event['replay']['steps']:
        status = step['status_label']
        print(f"  - {step['label']}: {status}")
        if step.get('rule'):
            rule = step['rule']
            print(f"    rule {rule['name']} expected {rule['expected']} but saw {rule['actual']}")
    if event.get('rejects'):
        reject = event['rejects']
        print(f"  reject slice {reject['dataset']} captured {reject['reject_rows']} rows")
```

The helpers perform the following steps:

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

The overview page now contains a **Refresh the Altair Retail demo** card that
explains how the cached fixtures are rebuilt and keeps presenters oriented with
the sticky navigation rail. The story tabs now surface catalog quick links so
you can jump directly to the data product, dataset, or contract tables while
walking through the simulated timeline, flow, or lineage. A Mermaid diagram
illustrates how the source, modelled, ML, consumer, and aggregated products hand
off datasets. A second dataset-level lineage view groups every contract by its
owning data product zone so you can point out which ports are public and which
assets remain internal to the product boundary.

New for longer workshops, a **Timeline player** animates six months of Altair
Retail operations. Each milestone now replays the pipeline steps that occurred
during that period, animating contract checks, downstream freezes, and
recoveries so the facilitator can narrate incidents as they unfold. The detail
panel highlights which rule failed, the expected versus observed values, and
the fallback action (for example serving the `::valid` slice when a contract
reject is emitted). UI banners above the offer, activation, and dashboard tabs
mirror the simulated state so audiences see when the experience is paused,
stale, or healthy. The replay also surfaces reject diagnostics—sample rows,
counts, and contract versions—making it easy to demonstrate how the platform
captures bad data while keeping consumers online. As before, the controls drive
catalog anchors and include outbound links that open the underlying dataset,
contract, or product page in a new tab, making it easy to audit the artefacts
that caused the milestone.

A **Catalog crosslinks** tab set lists all data products, datasets, and
contracts involved in the walkthrough. Each entry anchors back to the cards on
the page or opens the authoritative record in the contracts catalog, and the
story quick links make those anchors accessible directly from the timeline,
flow, and lineage tabs. Together they make it easier to trace how a dataset
moves from the operational sources through the consumer surfaces without leaving
the retail demo UI. The dataset catalog
highlights the owning zone for each feed, aligning with the Mermaid lineage
diagram and the tags shown on the data product definitions.

Finally, an **Experience & analytics walkthrough** section uses tabs to keep the
offer highlights, marketing activation planner, and executive dashboard in a
single page. Demo facilitators can flip between the faux applications without
changing routes, reinforcing how one pipeline powers multiple stakeholders.
