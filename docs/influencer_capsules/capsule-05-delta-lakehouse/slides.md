---
title: "Delta Lakehouse Powered by DC43"
author: "DC43 Influencer Series"
revealOptions:
  transition: "fade"
  backgroundTransition: "convex"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-color="#041226" -->
# Contracts meet Delta Lake
<p style="color:#47D1C1;">Operational governance on your lakehouse</p>
<p class="fragment fade-up" style="color:#F5F7FA;">Automate SLOs • Track lineage • Ship faster</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1451187580459-43490279c0fa" data-background-size="cover" data-background-opacity="0.35" -->
## Without DC43

- <span class="fragment fade-in">Wiki-based ownership</span>
- <span class="fragment fade-in">Schema drift surprises</span>
- <span class="fragment fade-in">Manual audit prep</span>
- <span class="fragment fade-in">Slow policy adoption</span>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## DC43 Lakehouse Stack

<div class="columns">
<div>
<ul>
<li class="fragment fade-right"><strong>Contract Store</strong> → schema, policies, SLOs</li>
<li class="fragment fade-right"><strong>Delta Adapter</strong> → table properties + expectations</li>
<li class="fragment fade-right"><strong>Observability</strong> → Grafana dashboards</li>
</ul>
</div>
<div>
<img class="fragment zoom-in" src="https://images.unsplash.com/photo-1518779578993-ec3579fee39f" alt="Lakehouse analytics" style="border-radius:18px; box-shadow:0 22px 44px rgba(4,18,38,0.35);"/>
</div>
</div>

---

<!-- .slide: data-background-gradient="linear-gradient(120deg,#47D1C1,#041226)" -->
### Contract Snippet

```yaml
contract: delta-retail
version: 2.0.0
ownership:
  domain: retail-insights
  stewards: ["dataops@demo.io"]
quality:
  freshness: 15m
  completeness: 99.9%
  allowed_values:
    customer_segment: ["new", "returning", "vip"]
```

<p class="fragment fade-in" style="color:#F5F7FA;">Adapter syncs schema + policies to Delta tables.</p>

---

<!-- .slide: data-background-color="#0B1533" -->
## Demo Timeline

1. Seed retail sales data
2. Execute Delta adapter notebook
3. Add `customer_segment` field
4. Inspect Delta history & dashboards

<p class="fragment fade-in" style="color:#47D1C1;">All assets available in resource pack</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1500530855697-b586d89ba3ee" data-background-size="cover" data-background-opacity="0.35" -->
# Share Your Remix

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Download notebook + contract</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Tag #DC43Builds</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Contribute adapters</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 18px 42px rgba(0,0,0,0.45);">
<p style="color:#0B1533; font-weight:600;">CTA</p>
<p style="color:#0B1533;">Scan to access the lakehouse starter kit.</p>
<div style="width:170px; height:170px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
