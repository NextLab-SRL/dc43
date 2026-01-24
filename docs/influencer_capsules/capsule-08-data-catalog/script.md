# Capsule 08 – DC43 + Data Catalog Harmony

**Runtime target:** 3 minutes 20 seconds (LinkedIn + conference talk teaser)

## Segment 1 – Hook (0:00–0:20)
- **Narration**
  > "Catalog champions, you're about to see how DC43 turns your catalog from a static inventory into an execution engine. Let's sync contracts, policies, and glossary terms without double entry."
- **Stage directions**
  - {Action: Start in front of digital bookshelf representing catalog.}
  - {Action: Overlay headline "Catalog Harmony" with animated link icon.}

## Segment 2 – Catalog Pain (0:20–0:55)
- **Narration**
  > "Catalogs often become stale because product teams forget to update them, or governance can't enforce policies across platforms." 
  > "DC43 treats the catalog as a first-class destination: every contract update can publish metadata, glossary terms, and lifecycle states automatically."
- **Stage directions**
  - {Action: Show outdated catalog screenshot morphing into live dashboard.}

## Segment 3 – Integration Pattern (0:55–1:45)
- **Narration**
  > "Here's the pattern. DC43 emits signed manifests whenever a contract changes. A lightweight sync service reads the manifest, maps fields to your catalog API—think Atlan, Collibra, OpenMetadata—and updates lineage, stewardship, and policy tags." 
  > "The same service creates glossary entries from contract definitions, ensuring everyone shares the same business language."
- **Stage directions**
  - {Action: Walk through architecture slide showing manifest -> sync -> catalog.}
  - {Action: Highlight glossary section.}

## Segment 4 – Demo (1:45–2:45)
- **Narration**
  > "I'll run the open-source sync script against OpenMetadata today. We publish a contract, the sync service posts updates to the catalog, and within seconds the asset page reflects new policies and owners." 
  > "I'll show the API payloads so you can adapt them to your catalog of choice."
- **Stage directions**
  - {Action: Screen capture terminal running `python integrations/catalog_sync.py`.}
  - {Action: Switch to catalog UI showing updates.}

## Segment 5 – CTA (2:45–3:20)
- **Narration**
  > "Download the integration kit, drop in your catalog credentials, and schedule the sync nightly—or after every contract merge." 
  > "Share your catalog screenshots in the governance forum so others can learn from your setup."
- **Stage directions**
  - {Action: CTA slide with integration kit link and QR.}
  - {Action: End with friendly nod and invite to comment.}

## Safety Notes
- Use mock catalog environments; avoid exposing credentials.
- Mention that adapters for specific vendors may require API tokens or service
  accounts—remind viewers to follow their security policies.
