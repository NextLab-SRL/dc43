# Capsule 05 – Delta Lakehouse with DC43 Contracts

**Runtime target:** 4 minutes (YouTube primary, LinkedIn secondary)

## Segment 1 – Hook (0:00–0:25)
- **Narration**
  > "If your lakehouse still relies on manual wiki docs for contracts, you're missing automation. I'll show you how DC43 wraps Delta Lake tables in living contracts so quality, lineage, and governance stay synced."
- **Stage directions**
  - {Action: Begin with sweeping shot of lakehouse dashboard, then cut to host.}
  - {Action: Display headline overlay "Contracts + Delta Lake".}

## Segment 2 – Pain Point (0:25–0:55)
- **Narration**
  > "Delta Lake brings ACID to your data lake, but stakeholders still fight over who owns what. Without contracts, SLOs get lost, schema drift spreads, and audits take weeks." 
  > "DC43 lets you embed expectations directly into your Delta tables and propagate them to every consumer."
- **Stage directions**
  - {Action: Show messy wiki screenshot vs. clean contract YAML.}

## Segment 3 – Architecture (0:55–1:45)
- **Narration**
  > "Here's the stack. DC43 contract store defines the schema, policies, and quality checks. A Delta Lake adapter reads the contract and configures table properties, expectations, and data quality notebooks." 
  > "Whenever the contract version increments, the adapter runs a Spark job to apply schema evolution safely and updates Unity Catalog tags." 
  > "This is all open-source code you can run today."
- **Stage directions**
  - {Action: Walk through architecture slide with animated connectors.}
  - {Action: Highlight version bump process.}

## Segment 4 – Demo (1:45–3:20)
- **Narration**
  > "Let's rebuild the retail sales table from the quickstart. We run `dc43 demo lakehouse retail-sales` to seed sample data, then execute the adapter notebook. It reads the contract, creates the Delta table, and registers expectations via `expectations.yaml`." 
  > "When we add a new field `customer_segment`, the contract enforces allowed values and updates the Unity Catalog tag. Observability dashboards show the change in minutes." 
  > "I'll speed up the Spark job in post so you see the full flow."
- **Stage directions**
  - {Action: Screen capture Databricks-like notebook (use OSS environment).}
  - {Action: Overlay annotation pointing to contract version.}
  - {Action: Show Delta table history command output.}

## Segment 5 – CTA (3:20–4:00)
- **Narration**
  > "Grab the notebook, contract, and dashboards from the resource pack. Remix them with your own domain, then share your results with the community." 
  > "And if you want to contribute, help us extend the adapter to other storage engines—issue links are below."
- **Stage directions**
  - {Action: CTA slide with download link, GitHub issues, and QR code.}
  - {Action: End with upbeat nod and background music swell.}

## Safety Notes
- Use only synthetic data in demo recordings.
- Avoid referencing specific vendor SKUs; keep messaging neutral and open-source.
