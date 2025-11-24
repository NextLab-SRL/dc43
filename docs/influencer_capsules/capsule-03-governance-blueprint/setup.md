# Setup – Capsule 03 "Governance Blueprint"

## Objective
Equip the influencer to showcase a repeatable governance playbook with DC43,
including whiteboard visuals, blueprint templates, and stakeholder workflows.

## Prerequisites
1. **Workspace**
   - FigJam, Miro, or Figma for blueprint visuals.
   - Google Slides / Keynote for workshop agenda PDF exports.
2. **Software**
   - Python 3.10+, `pip install dc43 rich typer`
   - Node.js 18+, `npm install --global reveal-md`
   - Optional: Microsoft Teams developer tenant for workflow screenshots.
3. **Assets**
   - DC43 branding kit (`docs/assets/brand-pack.zip`).
   - Maturity Radar template (`docs/templates/dc43-maturity-radar.fig`).

## Environment Prep
1. Clone the repo and open governance templates:
   ```bash
   git clone https://github.com/<your-org>/dc43.git
   cd dc43
   open docs/governance
   ```
2. Generate sample governance workflow YAML for screen captures:
   ```bash
   python scripts/governance/sample_blueprint.py \
     --output artifacts/governance-blueprint.yaml
   ```
3. Export Reveal.js deck preview:
   ```bash
   cd docs/influencer_capsules/capsule-03-governance-blueprint
   reveal-md slides.md --static dist/slides --css ../shared/dc43-theme.css
   ```
4. Prepare workshop agenda PDF from template `docs/templates/governance-workshop.pptx`.

## Recording Assets
- **Whiteboard**: Use FigJam template and export animated walkthrough (Figma
  prototype mode recording at 1080p).
- **Workflow YAML**: Capture terminal showing diff when adding a new reviewer.
- **Case study**: Create anonymized metrics dashboard slide; include map graphic.

## Backup Plan
If you cannot access Microsoft Teams, showcase the approval flow using the DC43
web UI mock (`docs/assets/ui-approvals.mp4`). Narration still mentions
"integrate with your chat tool of choice."
