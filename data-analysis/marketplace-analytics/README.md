# Marketplace Product Analytics Template 
#### A Simulated A/B Test (SQL, dbt, Python skills)

### (_QUICK START (TL;DR): run notebook/quick_start.ipynb_)

## Intro

This is a template for an A/B testing project on marketplace data (more specifically, the XYZ dataset from Kaggle). It can be adapted to roughly any dataset; when you have a basic ELT (Extract-Load-Transform) structure set, you just need to add the context specifics.

**Difficulty:** Intermediate <br>
**Intended audience:** Jr/Mid Data Analysts (Sr Product Analysts are probably familiar with this structure)

<details style="border: 4px solid #81969c; border-radius: 8px; background-color: #f0feff; padding: 10px; width: 900px;">
<summary style="color: #000000;">
<strong>Quick note for students</strong>
</summary>
<br>
<p style="color: #000000;">
If you have no coding knowledge, I suggest starting with Codecademy/Datacamp, or basic, free Youtube videos (Udemy works too, if you feel like paying). You don´t need to finish the entire course to understand the code here, but the basics certainly help.
</p>
<p style="color: #000000;">
For those with some Python/SQL knowledge: Go through the code here first, with your LLM chat of choice on the side. Whenever you don't understand something, ask it. As you get familiar with the logic, try adapting it to your own context, if applicable.
</p>
</details>


## Project Goals
**(Note: For pro-level "add-on" challenges, skip to the end of this readme)** 

1. Help product data analysts quickly implement an end-to-end A/B testing routine with a low-friction template
  1.1. Proper datawarehousing infrastructure included (based on dbt)
  1.2. Basic analytics setup, easily expansible to an analytics dashboard
2. Explain A/B testing specifics beyond the basics with a practical example
  2.1. This is not meant to be a deep/exhaustive Statistics course lecture, but rather a quick go-to reference applicable to real-world problems. 
3. Give data analyst learners/beginners a practical example to work with


## Data & Pipeline Overview

- Dataset: Kaggle's "eCommerce Behavior Data from Multi Category store" (Large volume, Marketplace-style dataset)
- Storage/engine: DuckDB
- dbt models:
  - stg_events: cleaned event stream + variant assignment + event naming
  - fct_funnel: user‑level funnel presence (view/add_to_cart/contact)
  - fct_ab_test: per‑variant users, conversions, and conversion rate for A/B testing

<br>

<details style="border: 4px solid #81969c; border-radius: 8px; background-color: #f0feff; padding: 10px; width: 900px;">
<summary style="color: #000000;">
<strong>Installation and setup</strong>
</summary>
<h3 style="color: #000000;"> Connecting to Kaggle API for easier file management </h3> 
<p>
<ol style="color: #000000;">
<li>Install kaggle API (<code>pip install kaggle</code>), preferably in a virtual environment (aka venv)</li>
<li>Generate API key from your Kaggle account settings: <code>Kaggle → Account → “Create New API Token”</code>. Follow instructions</li>
<li>Run command to download files <code>kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store</code></li>
   <ol style="color: #000000;">
   <li>It will generate a zip file -> unzip it by running <code>unzip file.zip -d data/</code></li>
   <li>Add your /data directory to .gitignore, so you won't load it to the repo</li>
   <li>Notice this will download the dataset to disk, so make sure you have space</li>
   </ol>
</ol>
</p>
<h3 style="color: #000000;"> Connecting to duck db, loading the data (see notebook for details) </h3>
<p>
<ol style="color: #000000;">
<li>Install duck db (<code>pip install duckdb</code>) in your venv</li>
<li>Sample the dataset (the full file is too large) for optimized processing</li>
   <ul style="color: #000000;">
   <li>Notice that if you want to analyze the whole file, you'll need to load all the data. More on that later</li>
   </ul>
<li>Load duck db, create your db file and schemas for dbt</li>
<li>Load the sampled dataset to duck db</li>
</ol>
</p>
<h3 style="color: #000000;"> Set up dbt, create dbt schema </h3>
<p>
<ol style="color: #000000;">
<li>Install duck db's dbt adapter (<code>pip install dbt-duckdb</code>) in your venv</li>
<li>Initialize dbt project</li>
   <ul style="color: #000000;">
   <li>Define your project name when asked</li>
   </ul>
<li>Sample the dataset (the full file is too large) for optimized processing</li>
   <ul style="color: #000000;">
   <li>Notice that if you want to analyze the whole file, you'll need to load all the data. More on that later</li>
   </ul>
<li>Load duck db, create your db file and schemas for dbt</li>
<li>Load the sampled dataset to duck db</li>
</ol>
</p>
</details>

<br>

## A/B Test Methodology

### Experiment Design

This simulated A/B test measures the impact of a product change on user conversion, using event-level user data from an online marketplace (Kaggle dataset).

Being a comparison of proportions (conversion percentages) of a binary outcome (converted vs not), I use a z-test, which standardizes the differences in each group, so we establish how far (on standard deviations) is the observed diff from the mean diff, and with what confidence (p-value) 

### Variant Assignment

This is simulated, so there are no real testing variables here (e.g. new app feature, coupon code). I split the dataset into variant groups using the following logic:

- Variant A (Control): users where user_id is odd
- Variant B (Treatment): users where user_id is even

which approximates random assignment and makes sure results are reproducible + group variants are balanced.

### Hypothesis

"Our improved listing experience increases the likelihood of users contacting a seller."

**(Note: this hypothesis is illustrative only, as it doesn't relate to any testing variable in the dataset)**

#### Assumptions and "polluting" factors

- Target Population: Global (no need for pre-filtering on geopgraphies or subpopulations)
  - **(Note 1: In the real world we would often segment on e.g. country, platform, subscription tier etc.)**
  - **(Note 2: Random assignment would occur only after pre-filtering, according to test specifics)**
- Variants independent of user behavior + event tracking is consistent across variants
  - Here it's easy, because it's a meaningless, (pseudo)random user_id split. In real life, it requires deeper qualitative understanding of user behavior, and how it is measured through different data points
    - e.g. here we assume product mix is evenly distributed across variants, but IRL this could be an issue, if for instance Variant A has a lot more expensive product funnels (which will naturally have lower conversion) than Variant B
    - Other examples: geographies, time of day, device, user origin (e.g. ads vs recommendation vs repeat buy)
- SUTVA aka Stable Unit Treatment Value Assumption: No interference between user outcomes

### Key Metric 

Granularity level: user_id

Conversion Rate = % of users who perform contact_seller

(Bonus metric) View-to-conversion Rate = % of users who perform contact_seller, out of those who viewed a listing 

#### Side note: why would you look at view-to-conversion and not just conversion?
Basic conversion rate does not "tell the whole story".

For instance, say you have a low basic conversion rate, but also a low view rate (i.e. users who viewed a listing in the first place, out of those who visited the website). Is your sales funnel "bad", or maybe the website is confusing, so people can't see listings they would like in the first place? What if you are advertising to he wrong public, so your visitors are not interested in what you offer?

This ties back to your source datasets, and the staging structure you set up in your data warehouse:
- If your data source is poor, your entire pipeline is limited.
- If the source has good info, but you don't clean it up properly or don't add clean, useful data points then your data marts will be limited.

### Validation Checks (non-exhaustive list)

- Sample Ratio Mismatch (SRM): do group sizes match expected allocation (e.g., 50/50 split)? Significant deviations may indicate tracking or assignment issues.
- Segment Balance Checks: are subject attributes being controlled for?
- Pipeline consistency: are events being tracked identically across variants?


## Limitations

- Simulated experiment, no real feature rollout
- No explicit segmentation (e.g., geography, device) or further controlling (e.g. by product)
  - Why not? Given the naive variant split, it would be pointless to try and do it here. The main purpose of this exercise is provide a simple and effective A/B testing template, particularly for beginners

### How would it work IRL?

- A concrete, well-bounded data point (e.g. a new feature) would drive the experiment design. Preferably something that can be boiled down to a boolean (variant A = True, variant B = False for data point X)
- Well-defined control parameters, followed by pre-filtering before random variant assignment
  - Some of these control parameters (e.g. time of day/year, age groups) are more general and should be applied (almost) always, while others (e.g. geographies, gender) are context-specific

Pre-experiment filtering (e.g., by country or platform) ensures valid comparisons

Post-assignment validation ensures group balance and data integrity

Additional analyses (e.g., segmented uplift, regression adjustments) may be applied to isolate causal effects.


## Results & Interpretation

- (TODO)
- Key funnel numbers (e.g., % of users reaching each step, biggest drop‑off).
- A/B test outcome:
  - Conversion rates for A and B.
  - Test statistic / p‑value.

Conclusion: Results are statistically consistent with a tiny difference. This means we can "trust" the result, but in business terms, it is irrelevant. This is expected in a simulated setup with arbitrary variants carrying no business value.

**Why do I say the diff is "irrelevant"?** One could argue a 4% uplift in conversion rate is meaningful, but here the context strips the result of meaning. That's because no relevant business measure was taken to cause this; with more data, the diff would probably have been diluted and the result would not have been significant.

**What if we ran an actual test with a real hypothesis and got inconclusive results?** Many factors can cause this: Poor sampling, small dataset, insufficient timeframe... It can also be the case that there's simply no stastically significant diff. But the test stats alone are usually not enough to answer this; you should thoroughly review your test parameters and assumptions. 

**"So what?" factor:** Granted that this is a demo template, any outcome can yield business-relevant conclusions. The "inconclusive" outcome is the most "dangerous", as it likely exposes flaws in test design. Still, it can shed light on poor assumptions that were taken for granted by the team, or on a flawed customer segmentation that requires a review of business logic.


## This is too easy! A few "pro-level" addons FTW


Option A – Segment‑level view of the experiment

    Show variant conversion by one simple segment (e.g., price band or device proxy) and explicitly warn about multiple comparisons / exploratory nature.​

Option B – Early retention or re‑engagement view

    Start a second, smaller model/notebook that looks at whether contacted users come back or engage differently later, framed as “work in progress.”