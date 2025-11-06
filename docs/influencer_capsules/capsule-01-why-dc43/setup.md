# Setup – Capsule 01 "Why DC43 Matters Now"

## Objective
Prepare visuals that contrast legacy governance chaos with the modular DC43
experience. No live coding is required, but you should capture screen loops and
animated overlays that illustrate the modular architecture.

## Prerequisites
1. **Local environment**
   - Python 3.10+
   - Node.js 18+ (for Reveal.js slide export)
   - `pip install dc43 pyspark fastapi uvicorn rich`
   - `npm install --global reveal-md`
2. **Assets**
   - High-resolution screenshots of cluttered spreadsheets or governance boards
     (sanitize or create mock data).
   - Clean DC43 architecture diagram (use provided SVG in `docs/assets/dc43-modular.svg`
     or recreate from the slide deck outline).
   - Brand color palette: Midnight #0B1533, Aqua #47D1C1, Coral #FF6F61, Cloud #F5F7FA.
3. **Recording gear**
   - 4K camera or virtual avatar pipeline.
   - Lavalier or cardioid mic.
   - Key light + fill light.

## Demo Preparation
1. Clone the repository and open the architecture notebook for quick visuals:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   make docs-architecture # optional static assets
   ```
2. Generate a simple contract artifact for B-roll:
   ```bash
   python - <<'PY'
   from dc43.contracts import Contract

   contract = Contract(name="retail_promos", version="1.2.0")
   contract.metadata["owners"] = ["marketing@demo.io", "dataops@demo.io"]
   contract.policies["pii"] = {"masking": "tokenize"}
   print(contract.to_yaml())
   PY
   ```
   Save the YAML output to `artifacts/retail_promos.yaml` for overlay shots.
3. Use the Reveal.js deck to export a branded PDF for reference:
   ```bash
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```

## Scene Blocking
- **Hook visual**: Prepare a timeline animation in your editor (e.g., After
  Effects or Descript) that transitions from messy spreadsheets to the DC43
  modular diagram. Export as MP4 for use at 24fps.
- **Impact vignette**: Create a kanban board in FigJam or Miro, record a screen
  capture moving a card across columns, then speed ramp to emphasize speed.

## Backup Plan
If studio recording is unavailable, capture the narration using Descript or
Adobe Podcast and pair with motion graphics built entirely from the Reveal.js
slides. Maintain the same cues so automated generation tools can sync actions.
