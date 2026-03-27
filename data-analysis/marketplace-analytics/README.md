# Marketplace Product Analytics Template
#### A Simulated A/B Test (SQL, dbt, Python skills)

### TL;DR:

- QUICK START: run `notebook/quick_start.ipynb`
- For results and follow-up analysis on this demo exercise: see `ANALYSIS.md`

## Intro

This is a template for an A/B testing project on marketplace data (specifically, the XYZ dataset from Kaggle). It can be adapted to most datasets: once you have a basic ELT (Extract-Load-Transform) structure in place, you only need to add context-specific logic.

**Difficulty:** Intermediate <br>
**Intended audience:** Jr/Mid Data Analysts (Senior Product Analysts are likely familiar with this structure)

<details style="border: 4px solid #81969c; border-radius: 8px; background-color: #f0feff; padding: 10px; width: 900px;">
<summary style="color: #000000;">
<strong>Quick note for students</strong>
</summary>
<br>
<p style="color: #000000;">
If you have no coding knowledge, I suggest starting with Codecademy, Datacamp, or free YouTube tutorials. You don't need to finish an entire course to follow the code here, but the basics help.
</p>
<p style="color: #000000;">
For those with some Python/SQL knowledge: go through the code with an LLM on the side. Ask it whenever something is unclear, then try adapting the template to your own context.
</p>
</details>

## Project Goals
**(Note: For pro-level add-on challenges, skip to the end of this readme)**

1. Help product data analysts quickly implement an end-to-end A/B testing routine with a low-friction template
   - 1.1. Proper data warehousing infrastructure included (dbt-based)
   - 1.2. Basic analytics setup, easily expandable to a dashboard
2. Explain A/B testing specifics beyond the basics with a practical example – a quick, real-world reference, not an exhaustive statistics lecture
3. Give data analyst learners a practical, working example to build on

## Data & Pipeline Overview

- **Dataset:** Kaggle's "eCommerce Behavior Data from Multi Category Store" (large-volume, marketplace-style)
- **Storage/engine:** DuckDB
- **dbt models:**
  - `stg_events`: cleaned event stream, variant assignment, event normalisation
  - `int_funnel_flagged`: row-level funnel flags per user/session with `event_date`, feeds runtime aggregation with optional timeframe filtering
  - `fct_funnel`: user and session-level funnel flags (view / add_to_cart / purchase)
  - `fct_ab_test`: per-variant users, conversions, and conversion rate
  - `fct_ab_<dimension>`: per-variant results segmented by any valid dimension (brand used as demo); driven by a dbt macro and easily extended

The transformation layer moves directly from staging to marts – a deliberate choice, as the pipeline scope doesn't justify a full intermediate layer. The one exception is `int_funnel_flagged`, added specifically to support timeframe-based analysis without re-running dbt.

> **Architecture note:** `fct_ab_test_user` and `fct_ab_test_session` are technically a special case of the runtime aggregation introduced with the timeframe feature. In a production system these would be candidates for deprecation – the main argument for keeping them is query caching in a shared warehouse, which is not relevant in a local DuckDB setup.

<br>

<details style="border: 4px solid #81969c; border-radius: 8px; background-color: #f0feff; padding: 10px; width: 900px;">
<summary style="color: #000000;">
<strong>Installation and setup</strong>
</summary>
<h3 style="color: #000000;"> Connecting to Kaggle API for easier file management </h3>
<p>
<ol style="color: #000000;">
<li>Install kaggle API (<code>pip install kaggle</code>), preferably in a virtual environment</li>
<li>Generate an API key: <code>Kaggle → Account → "Create New API Token"</code> and follow the instructions</li>
<li>Download the dataset: <code>kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store</code></li>
   <ol style="color: #000000;">
   <li>Unzip the file: <code>unzip file.zip -d data/</code></li>
   <li>Add your <code>/data</code> directory to <code>.gitignore</code></li>
   <li>Make sure you have enough disk space before downloading</li>
   </ol>
</ol>
</p>
<h3 style="color: #000000;"> Connecting to DuckDB and loading the data </h3>
<p>
<ol style="color: #000000;">
<li>Install DuckDB (<code>pip install duckdb</code>) in your venv</li>
<li>Sample the dataset – the full file is large; more on full-load options in the notebook</li>
<li>Create your DuckDB file and schemas for dbt</li>
<li>Load the sampled dataset into DuckDB</li>
</ol>
</p>
<h3 style="color: #000000;"> Setting up dbt </h3>
<p>
<ol style="color: #000000;">
<li>Install the DuckDB dbt adapter (<code>pip install dbt-duckdb</code>) in your venv</li>
<li>Initialise the dbt project and define your project name when prompted</li>
<li>Run <code>dbt run</code> to materialise all models</li>
<li>Run <code>dbt test</code> to validate the schema</li>
</ol>
</p>
</details>

<br>

### Lineage
![DAG](data/lineage_dag.png)

## A/B Test Methodology

### Experiment Design

This simulated A/B test measures the impact of a product change on user conversion, using event-level data from an online marketplace.

The outcome is binary (converted vs. not), and the metric is a proportion (conversion rate). A two-proportion z-test is used, which standardises the difference between groups and produces a p-value indicating how likely the observed gap is under the null hypothesis of no effect.

### Variant Assignment

Variant assignment is simulated – there is no real feature rollout. Groups are split as follows:

- **Variant A (Control):** users where `user_id` is odd
- **Variant B (Treatment):** users where `user_id` is even

This approximates random assignment, ensures reproducibility, and produces balanced group sizes.

### Hypothesis

*Defined per experiment – see `ANALYSIS.md` for the demo hypothesis.*

#### Assumptions and confounding factors

- **Target population:** global – no pre-filtering applied
  - In the real world, pre-filtering by geography, platform, or subscription tier is standard before random assignment
- **Variant independence:** assumed. In practice, this requires verifying that product mix, price distribution, user origin (ads vs. organic), and device type are balanced across variants
- **SUTVA:** no interference between user outcomes is assumed

### Key Metrics

**Granularity:** `user_id` and `user_session` (both levels supported)

**Timeframe granularity:** event timestamps are truncated to date level in the intermediate layer – sufficient for A/B window analysis. The timeframe parameters in `run_ab_aggregation()` enable week-over-week slicing, supporting manual assessment of novelty effect decay in the treatment group.

**Conversion Rate:** % of users who purchase

**View-to-Conversion Rate:** % of users who purchase, out of those who viewed a listing

> Basic conversion rate doesn't tell the full story. A low conversion rate paired with a low view rate may point to a discovery or targeting problem rather than a funnel problem – which is why view-to-conversion is tracked as a secondary metric.

### Statistical Tests and Validation

- **Two-proportion z-test:** primary significance test on conversion rate difference between variants
- **p-value:** probability of observing this result if the null hypothesis (no effect) is true; threshold set at α=0.05 by default, configurable in `run_ab_aggregation()`
- **Confidence interval on absolute uplift:** range of plausible true effect sizes; more actionable than p-value alone for business decisions
- **Minimum Detectable Effect (MDE):** smallest effect the experiment can reliably detect given sample size and baseline rate; reported alongside an `underpowered` flag
- **Statistical power (1 - β):** probability of detecting a real effect if one exists; standard target is 80%. The `underpowered` flag in `run_ab_aggregation()` serves as a proxy; explicit computation via `statsmodels.stats.power.NormalIndPower().solve_power()` would formalise this
- **Sample Ratio Mismatch (SRM):** chi-square check on whether group sizes match the expected 50/50 allocation. Significant deviation indicates a tracking or assignment issue and invalidates the test

### Validation Checks

- **Segment balance:** verifies that key attributes are evenly distributed across variants
- **Pipeline consistency:** confirms events are tracked identically across variants
- **Novelty effect (manual):** plot per-variant conversion rate over time and check for decay in the treatment group – a common confounder in e-commerce A/B tests that inflates short-term uplift. The timeframe parameters in `run_ab_aggregation()` support week-over-week assessment of this. A production-grade analysis would automate this check and flag decay programmatically.

## Limitations

- Simulated experiment – no real feature rollout
- No explicit segmentation or covariate control (by design – given the naive variant split, adding them would not be meaningful here)

### How it works IRL

- A concrete, well-bounded change (e.g. a new feature, a UI update) drives experiment design – ideally something reducible to a boolean
- Pre-filtering by relevant dimensions (country, platform, user tier) precedes random assignment
- Post-assignment validation confirms group balance and data integrity
- Additional analyses (segmented uplift, regression adjustments) may isolate causal effects

## TODO

**Statistical power – explicit computation:** the `underpowered` flag is currently a proxy based on MDE. Formalise it with `statsmodels.stats.power.NormalIndPower().solve_power()` for a direct power estimate at any target effect size.