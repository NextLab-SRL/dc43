---
title: "DC43 Governance Blueprint"
author: "DC43 Influencer Series"
revealOptions:
  transition: "fade"
  backgroundTransition: "slide"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-color="#0B1533" -->
# Governance = Product Design
<p style="color:#47D1C1;">Blueprinting with DC43</p>
<p class="fragment fade-up" style="color:#F5F7FA;">Stakeholder alignment in 3 moves</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1527689368864-3a821dbccc34" data-background-size="cover" data-background-opacity="0.4" -->
## Why alignment breaks

- <span class="fragment fade-in">Governance lives in catalogs</span>
- <span class="fragment fade-in">Engineers live in Git</span>
- <span class="fragment fade-in">Product chases launch dates</span>
- <span class="fragment fade-in" style="font-weight:700; color:#FF6F61;">Contracts live everywhere & nowhere</span>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## Blueprint Overview

<div class="columns">
<div>
<ol>
<li class="fragment fade-right">Stakeholder discovery (45 min)</li>
<li class="fragment fade-right">Map contract bundles</li>
<li class="fragment fade-right">Automate workflows</li>
</ol>
</div>
<div>
<img class="fragment zoom-in" src="https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?blend=47D1C1" alt="Blueprint sketch" style="border-radius:16px; box-shadow:0 18px 36px rgba(11,21,51,0.25);"/>
</div>
</div>

<aside class="notes">
Use this slide during Segment 3 while pointing at each step.
</aside>

---

<!-- .slide: data-background-gradient="linear-gradient(135deg,#47D1C1,#0B1533)" -->
### DC43 Workflow YAML

```yaml
workflow: governance-blueprint
intake:
  form: templates/intake.yaml
reviews:
  - team: compliance
    sla_hours: 24
  - team: product
    sla_hours: 12
notifications:
  channel: teams
  template: templates/approval-card.json
```

<p class="fragment fade-in" style="color:#F5F7FA;">Version in Git · Reuse across domains</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1531297484001-80022131f5a1" data-background-size="cover" data-background-opacity="0.35" -->
## Case Study Highlights

- 12 legacy processes unified
- 40% faster policy sign-offs
- Automated attestations via chat
- Audit-ready evidence trail

<aside class="notes">
Overlay global map graphic with highlighted regions.
</aside>

---

<!-- .slide: data-background-color="#0B1533" -->
# Download the Blueprint

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Workshop agenda</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Maturity radar template</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Workflow YAML starter kit</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 20px 40px rgba(0,0,0,0.45);">
<p style="color:#0B1533; font-weight:600;">Call To Action</p>
<p style="color:#0B1533;">Scan to grab the full governance blueprint.</p>
<div style="width:170px; height:170px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
