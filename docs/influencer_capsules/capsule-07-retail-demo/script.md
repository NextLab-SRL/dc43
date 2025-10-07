# Capsule 07 – Retail Demo: Personalized Promotions with DC43

**Runtime target:** 4 minutes 10 seconds (YouTube + LinkedIn)

## Segment 1 – Hook (0:00–0:25)
- **Narration**
  > "Retail teams, imagine launching a personalized promotion in a single afternoon. In this capsule I'll run the full DC43 retail demo—contracts, orchestration, and reporting—so you can copy it for your next campaign."
- **Stage directions**
  - {Action: Start with animated storefront visuals, swipe to host.}
  - {Action: Overlay headline "Retail Promotion in an Afternoon".}

## Segment 2 – Scenario Setup (0:25–1:00)
- **Narration**
  > "We're simulating a back-to-school promo. Marketing needs segmentation, data science needs clean features, and governance needs oversight." 
  > "DC43 coordinates the whole thing: contracts define data, workflows manage approvals, and adapters feed the lakehouse and dashboards."
- **Stage directions**
  - {Action: Show tri-panel of marketing, data science, governance icons.}

## Segment 3 – Contract + Workflow (1:00–2:00)
- **Narration**
  > "Step one: clone the retail promo contract. We tweak the discount rules and update the validity window." 
  > "Step two: trigger the governance workflow. Compliance signs off, marketing reviews copy, and the orchestrator publishes the signed contract." 
  > "All of this is automated with DC43 CLI commands and workflow YAML."
- **Stage directions**
  - {Action: Screen capture YAML edit with highlight.}
  - {Action: Show workflow approval timeline animation.}

## Segment 4 – Data Activation (2:00–3:20)
- **Narration**
  > "Once the contract is live, adapters push it into the Delta Lakehouse and the streaming agent. Spark jobs filter customers based on the new promo rules and write them to a feature table." 
  > "The DC43 observability stack tracks freshness and alerts if the feed stalls. Finally, the marketing dashboard updates with eligible customers by store." 
  > "I'll run the commands at 1.5x speed so you see the flow end-to-end."
- **Stage directions**
  - {Action: Screen capture CLI + notebook, overlay progress bar.}
  - {Action: Show dashboard with segmentation map.}

## Segment 5 – CTA (3:20–4:10)
- **Narration**
  > "The full runbook is in the resource pack—scripts, sample data, dashboards, even email copy templates." 
  > "Remix it for your own retail vertical and share your results in the community. We might feature you in a future livestream."
- **Stage directions**
  - {Action: CTA slide with resource icons, QR, hashtags.}
  - {Action: End with wave and fade into storefront loop.}

## Safety Notes
- All data is synthetic; remind viewers not to use customer PII in demos.
- Encourage sanitizing any exported CSVs before sharing publicly.
