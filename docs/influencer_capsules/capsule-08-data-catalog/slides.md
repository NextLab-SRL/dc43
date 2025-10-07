---
title: "Catalog Harmony with DC43"
author: "DC43 Influencer Series"
revealOptions:
  transition: "fade"
  backgroundTransition: "convex"
  controls: true
  progress: true
  hash: true
---

<!-- .slide: data-background-color="#091A33" -->
# Make Your Catalog Live
<p style="color:#47D1C1;">DC43 → Catalog Sync</p>
<p class="fragment fade-up" style="color:#F5F7FA;">No more double entry</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1523475472560-d2df97ec485c" data-background-size="cover" data-background-opacity="0.35" -->
## Where Catalogs Stall

- <span class="fragment fade-in">Outdated metadata</span>
- <span class="fragment fade-in">Manual policy tagging</span>
- <span class="fragment fade-in">Unclear ownership</span>
- <span class="fragment fade-in">Glossary drift</span>

---

<!-- .slide: data-background-color="#F5F7FA" -->
## Sync Blueprint

<div class="columns">
<div>
<ol>
<li class="fragment fade-right">DC43 publishes manifest</li>
<li class="fragment fade-right">Sync service maps fields</li>
<li class="fragment fade-right">Catalog API updates assets</li>
<li class="fragment fade-right">Glossary terms stay aligned</li>
</ol>
</div>
<div>
<img class="fragment zoom-in" src="https://images.unsplash.com/photo-1523475472560-d2df97ec485c?blend=47D1C1" alt="Linked data" style="border-radius:18px; box-shadow:0 22px 44px rgba(9,26,51,0.35);"/>
</div>
</div>

---

<!-- .slide: data-background-gradient="linear-gradient(130deg,#47D1C1,#091A33)" -->
### Manifest Excerpt

```json
{
  "contract": "delta-retail",
  "version": "2.0.0",
  "owners": ["dataops@demo.io"],
  "glossary": {
    "customer_segment": "Grouping of shoppers based on loyalty and spend"
  },
  "policies": ["pii_masking", "retention_365d"]
}
```

<p class="fragment fade-in" style="color:#F5F7FA;">Sync service converts this into catalog API calls</p>

---

<!-- .slide: data-background-color="#0B1533" -->
## Demo Steps

1. Publish contract & emit manifest
2. Run `catalog_sync.py --dry-run`
3. Post updates to catalog API
4. Review asset page & glossary

<p class="fragment fade-in" style="color:#47D1C1;">Adapters for OpenMetadata, Atlan, Collibra (coming)</p>

---

<!-- .slide: data-background-image="https://images.unsplash.com/photo-1454165205744-3b78555e5572" data-background-size="cover" data-background-opacity="0.35" -->
# Share Your Catalog Glow-Up

<div class="columns">
<div>
<ul>
<li class="fragment fade-in" style="color:#F5F7FA;">Download integration kit</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Schedule sync after merges</li>
<li class="fragment fade-in" style="color:#F5F7FA;">Showcase before/after shots</li>
</ul>
</div>
<div>
<div class="fragment fade-in" style="background:#F5F7FA; padding:24px; border-radius:18px; box-shadow:0 20px 40px rgba(0,0,0,0.45);">
<p style="color:#0B1533; font-weight:600;">CTA</p>
<p style="color:#0B1533;">Scan for integration kit + forum thread.</p>
<div style="width:170px; height:170px; background:#E3E8F7;">QR</div>
</div>
</div>
</div>
