---
title: "Retail Promotion in an Afternoon"
author: "DC43 Influencer Series"
revealOptions:
  transition: "convex"
  backgroundTransition: "fade"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1441984904996-e0b6ba687e04" data-background-size="cover" data-background-opacity="0.35" -->
# DC43 Retail Demo
<p style="color:#47D1C1;">Personalized promotions made simple</p>
<p class="fragment fade-up" style="color:#F5F7FA;">Contracts • Workflows • Activation</p>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## The Challenge

- <span class="fragment fade-in">Marketing wants segmentation now</span>
- <span class="fragment fade-in">Data science needs trusted features</span>
- <span class="fragment fade-in">Governance demands oversight</span>
- <span class="fragment fade-in" style="color:#FF6F61;">Traditional process = weeks</span>

---

<!-- .slide: data-background-color="#0B1533" -->
## DC43 Flow

1. Clone retail promo contract
2. Run governance workflow
3. Activate adapters (Delta + streaming)
4. Update dashboards & alerts

<p class="fragment fade-in" style="color:#47D1C1;">All automated with reusable scripts</p>

---

<!-- .slide: data-background-gradient="linear-gradient(120deg,#47D1C1,#0B1533)" -->
### Contract Snapshot

```yaml
contract: retail-promo
version: 1.5.0
promo:
  name: Back-to-School Boost
  discount_rules:
    - segment: returning
      discount: 0.15
    - segment: vip
      discount: 0.25
validity:
  start: 2024-07-20
  end: 2024-09-15
```

<p class="fragment fade-in" style="color:#F5F7FA;">Workflow auto-notifies marketing & compliance</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1515169067865-5387ec356754" data-background-size="cover" data-background-opacity="0.35" -->
## Data Activation

- Delta adapter → feature table
- Streaming agent → real-time eligibility
- Observability → freshness & policy alerts
- Dashboard → store-level segmentation map

---

<!-- .slide: data-background-color="#0B1533" -->
# Download the Retail Kit

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Contracts + scripts</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Dashboard JSON</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Campaign messaging templates</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 20px 40px rgba(0,0,0,0.45);">
<p style="color:#0B1533; font-weight:600;">CTA</p>
<p style="color:#0B1533;">Scan to access the retail demo bundle.</p>
<div style="width:170px; height:170px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
