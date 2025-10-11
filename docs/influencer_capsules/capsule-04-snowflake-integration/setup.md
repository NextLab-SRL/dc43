# Setup – Capsule 04 "Preparing for Snowflake"

## Objective
Showcase the in-progress Snowflake adapter concept without implying GA
availability. Capture prototype scripts, design diagrams, and contributor calls.

## Prerequisites
1. **Environment**
   - Python 3.10+
   - Node.js 18+, `npm install --global reveal-md`
   - Optional: Access to a Snowflake trial or dev account (for screenshots only).
2. **Python dependencies**
   ```bash
   pip install dc43 snowflake-connector-python rich typer jinja2
   ```
3. **Assets**
   - Architecture diagram template `docs/templates/adapter-architecture.fig`
   - Prototype script `prototypes/snowflake_adapter.py`
   - Design RFC draft `docs/rfcs/2024-07-snowflake-adapter.md`

## Preparation Steps
1. Clone repository and open prototype folder:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   ```
2. Run the adapter prototype against local sample contract:
   ```bash
   python prototypes/snowflake_adapter.py \
     --contract docs/demo-contracts/warehouse-orders.yaml \
     --output artifacts/snowflake-ddl.sql
   ```
   - This uses a mock translator; it does **not** connect to Snowflake.
3. Generate Reveal.js slide assets:
   ```bash
   cd docs/influencer_capsules/capsule-04-snowflake-integration
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```
4. Prepare optional Snowflake UI screenshots (dev account only). Mask account ID
   and any proprietary data.

## Recording Checklist
- Capture terminal run of prototype script, pausing on generated SQL.
- Record scroll-through of the design RFC highlighting contribution areas.
- Export architecture diagram as animated walkthrough (Figma + screen record).

## Safety Messaging
- Display lower-third disclaimer "Prototype demo • No official connector yet" in
  every screen recording.
- If you must show Snowflake UI, use dummy warehouse named `DC43_SANDBOX`.
