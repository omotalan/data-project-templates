# Marketplace Product Analytics – Sample Exercise Analysis

### TL;DR:
- For template instructions and methodology, see `README.md`
- For a sample business presentation of these results, see `SLIDE_DECK.pdf`

---

## Hypothesis

> *"Our improved listing experience increases the likelihood of users purchasing."*

Note: this hypothesis is illustrative only – no real feature change drives it. Variant assignment is a pseudorandom split on `user_id`. Any observed difference carries no causal business meaning.

---

## Results

### Funnel Overview

![Funnel chart - User](data/funnel_chart_user.png)

![Funnel chart - Session](data/funnel_chart_session.png)


- Virtually all users who visit the site view at least one listing; only ~5% add an item to cart
- Purchase rate is the terminal conversion event tracked here

### A/B Test Outcome

| | Control (A) | Treatment (B) |
|---|---|---|
| Conversion Rate | 7.4% | 7.8% |
| Relative Uplift | — | +4.6% |
| Significant at α=0.01 | — | ✅ |

**Interpretation:** the result is statistically significant but the difference is negligible in business terms. This is expected in a simulated setup with arbitrary variants – the split carries no real treatment effect. With more data the difference would likely dilute further.

> **Why call a 4% uplift irrelevant?** Context matters. No business action caused this difference. In a real test, a 4% relative uplift on a high-volume funnel step could be very meaningful – but here it's noise dressed up as signal.

> **What if a real test came back inconclusive?** Many factors can cause this: underpowered sample, insufficient timeframe, poor variant isolation. The test stats alone rarely answer this – you need to review your design assumptions in full business context. An inconclusive result is often the most valuable outcome: it surfaces flawed assumptions or segmentation logic that the team had taken for granted.

---

## Add-on Analyses

### 1. Segmented A/B Analysis

Segment-level A/B tests are supported via the `generate_ab_segment` dbt macro. Add any valid dimension to `segments.sql`:

```sql
-- marts/fct_ab_brand.sql
{{ config(materialized='view') }}
{{ generate_ab_segment('brand') }}
```

Then query from the notebook:

```python
df = con.execute("SELECT * FROM marts.fct_ab_brand").df()
```

> ⚠️ Segment-level tests are exploratory by nature. Running multiple comparisons increases the risk of false positives – treat findings as directional, not conclusive, unless corrected for (e.g. Bonferroni).

### 2. Re-engagement View

A lightweight follow-on question: do converted users return? A second model tracking repeat engagement post-conversion could frame this as a retention signal. Marked here as a natural extension of the pipeline – not implemented in this demo.

---

## Notes on Real-World Application

- **Add to Cart is underrepresented in this dataset.** In a real marketplace, funnel order and drop-off rates vary significantly by product category and UX flow.
- **Segment balance and SRM checks** are run automatically in `run_ab_aggregation()` – review those outputs before drawing any conclusions from conversion rates.
- **Novelty effect:** week-over-week slicing via the timeframe parameters in `run_ab_aggregation()` is the recommended first check if treatment uplift looks strong early and fades over time.