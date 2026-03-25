# Marketplace Product Analytics - Sample Exercise Analysis 

### TL;DR:

- For template instructions and methodology, see README.md
- For a sample product analyst-level presentation, see SLIDE_DECK.pdf


## Results & Interpretation

Example output:
![alt text](data/funnel_chart.png)

- Virtually all users that visit the website view at least one listing; however, only about 5% of them add an item to the cart.
  - Seller contact % is higher, due to the UX workflow (the user can skip the add to cart step)
- A/B test outcome:
  - 7.4% vs 7.8% for Control vs Hypothesis group, respectively
  - Relative uplift of 4.6%
  - Statistically significant at alpha=0.01.

Conclusion: Results are statistically significant, but with a tiny difference. This means we can "trust" the result, but in business terms, it is irrelevant. This is expected in this sample exercise: it is a simulated setup, with arbitrary variants that carry no business value.

**Why do I say the diff is "irrelevant"?** One could argue a 4% uplift in conversion rate is meaningful, but here the context strips the result of meaning. That's because no relevant business measure was taken to cause this; with more data, the diff would probably have been diluted and the result would not have been significant.

**What if we ran an actual test with a real hypothesis and got inconclusive results?** Many factors can cause this: Poor sampling, small dataset, insufficient timeframe... It can also be the case that there's simply no stastically significant diff. But the test stats alone are usually not enough to answer this; you should thoroughly review your test parameters and assumptions within your business context.

**"So what?" factor:** Granted that this is a demo template, any outcome can yield business-relevant conclusions. The "inconclusive" outcome is the most "dangerous", as it likely exposes flaws in test design. Still, it can shed light on poor assumptions that were taken for granted by the team, or on a flawed customer segmentation that requires a review of business logic.


## Extras: add-on analyses can deepen udnerstanding of the data

### 1. Funnel analysis

**Note: Add to Cart step underrepresented in this dataset; in a real marketplace, funnel order may differ by product category.**

TODO

### 2. Segmented A/B Analysis

Segment-level A/B tests (e.g. by brand or category) are supported via the
`generate_ab_segment` dbt macro. Create a view for any dimension in `segments.sql`:

```sql
-- marts/fct_ab_brand.sql
{{ config(materialized='view') }}
{{ generate_ab_segment('brand') }}
```

Then query from the notebook:
` df = con.execute("SELECT * FROM marts.fct_ab_brand").df() `


### 3. Segment‑level view of the experiment

Show variant conversion by one simple segment (e.g., price band or device proxy) and explicitly warn about multiple comparisons / exploratory nature.​

### 4. Early retention or re‑engagement view

Start a second, smaller model/notebook that looks at whether contacted users come back or engage differently later, framed as “work in progress.”