# Retail Transactions Lakehouse

[![ci](https://github.com/criskian/retail-transactions-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/criskian/retail-transactions-lakehouse/actions/workflows/ci.yml)
[![pipeline](https://github.com/criskian/retail-transactions-lakehouse/actions/workflows/pipeline.yml/badge.svg)](https://github.com/criskian/retail-transactions-lakehouse/actions/workflows/pipeline.yml)
[![python](https://img.shields.io/badge/python-3.11%20%7C%203.14-blue)](pyproject.toml)
[![pyspark](https://img.shields.io/badge/pyspark-3.5.5-e25a1c)](requirements-pipeline.txt)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**English** · [Español](README.md)

A medallion lakehouse over **1.1 million real grocery-store baskets**:
distributed ETL with PySpark, customer segmentation and product recommendation
with Spark MLlib, and an analytics dashboard served from DuckDB.

**[→ Live demo](https://retail-transactions-lakehouse.streamlit.app)**
· [Architecture decisions](docs/ADR/)
· [Technical report](docs/informe_tecnico.md)

> The demo sleeps after 12 h without visitors; the first visit wakes it up in a
> few seconds. The UI and the detailed documentation are in Spanish.

![Executive summary](docs/assets/dashboard.png)

<details>
<summary>More screenshots</summary>

**Segmentation** — choice of k, PCA projection as small multiples, radar
profiles and a business reading per segment.

![Segmentation](docs/assets/segmentacion.png)

**Recommender** — association rules and personalised recommendations.

![Recommender](docs/assets/recomendador.png)

</details>

---

## The problem

A supermarket hands over six months of transactions from four stores. Each row
is a basket: date, store, customer, and the list of products concatenated with
spaces. **There are no prices, no transaction ids and no quantities.**

From that, the job is to work out what sells, when, to whom, which products are
bought together, and what to recommend to each customer — and to do it so that
dropping a new file in regenerates every result without manual steps.

## What the data forces you to decide

| Constraint | Design consequence |
|---|---|
| The basket arrives as a string of products | The `explode` takes the table from 1,108,987 rows to **10,591,792** |
| No transaction id | Built as `sha2(date\|store\|customer\|list)`: deterministic, so reprocessing never duplicates |
| No quantity | A product appearing *N* times in a basket counts as *N* units |
| A product maps to several categories | Resolved 1:1 with `min(category_id)`; otherwise the join inflates every count |
| No prices | Every metric is **relative**: volume, frequency, diversity, recency |

## Architecture

```
  data/landing/*.csv                      GitHub Actions (JDK 17)
         │                          ┌──────────────────────────────┐
         ▼                          │  bronze  raw, faithful copy  │
   ┌───────────┐                    │  silver  explode + catalogue │
   │  Bronze   │  Parquet           │  gold    analytical marts    │
   │  Silver   │  partitioned       │  models  KMeans/FPGrowth/ALS │
   │  Gold     │  by store_id       │  export  serving.duckdb      │
   └───────────┘                    └──────────────┬───────────────┘
                                                   │ release asset
                                                   ▼
                                     ┌──────────────────────────────┐
                                     │  Streamlit + DuckDB          │
                                     │  24 MB · no JVM · read-only  │
                                     └──────────────────────────────┘
```

**Compute and serving are deliberately separated.** The app never imports
PySpark; it only reads the artefact that CI publishes. That is what makes it
deployable on a platform with no Java and ~1 GB of RAM
([ADR-0006](docs/ADR/0006-computo-y-serving-separados.md)).

| Layer | Rows | Size |
|---|---:|---:|
| Landing (6 CSV files) | 1,108,987 baskets | 55 MB |
| Silver `transactions_items` | 10,591,792 | 379 MB |
| Gold (15 marts) | — | 9.5 MB |
| **Serving `serving.duckdb`** | — | **24 MB** |

## Results

**Segmentation (K-Means, k = 5, silhouette 0.495 ± 0.008).** Six behavioural
features, z-score scaled. k is chosen by silhouette **averaged over five
subsamples**: with a single sample the winner flipped between 4 and 5, and 0.45 %
of new data was enough to change it. Subsamples are drawn by hashing the
customer id rather than with `DataFrame.sample`, which depends on the physical
row order and is therefore not reproducible when the Gold layer is rebuilt — two
consecutive runs now produce identical figures. PCA explains **80.9 %** of the
variance in two dimensions.

| Segment | % of customers | Profile |
|---|---:|---|
| Recent occasional | 39 % | Buy little, last visit ~1 month ago |
| Active regulars | 24 % | ~14 visits, average basket, recent |
| Inactive | 21 % | Bought early in the period and never came back |
| VIP | 8 % | High frequency and diversity; carry the volume |
| Big basket | 7 % | Infrequent but fill the cart (~24 items) |

**Association rules (FP-Growth).** 270 rules with support ≥ 5 % and
confidence ≥ 30 %; 213 have a single-product antecedent. The highest-lift rules
link root vegetables, fruit vegetables and herbs — a traditional market basket.
The antecedent is kept as a **set**: a rule can read `3 + 16 → 21`, and splitting
it into two single-item rules would misstate the confidence
([ADR-0007](docs/ADR/0007-antecedentes-como-conjunto.md)).

**Recommendation (implicit ALS).** Top-10 products for each of the 131,186
customers. Purchased quantity is treated as confidence in the interest, not as
a rating.

**Business findings.** Saturday and Sunday see ~39 % more transactions than a
Wednesday. Only 449 of the 69,891 catalogue SKUs sold in six months. 46 % of the
products that did sell have no category assigned — a catalogue data-quality
issue, not an analysis one.

## Engineering highlights

Things found and fixed while taking this from a course project to a deployable
one. Each has a regression test.

- **Association rules were silently wrong.** The original code `explode`d the
  multi-item antecedent, turning `{3,16} → {21}` into two false single-item
  rules. It produced 327 rows for only 215 distinct pairs, with one pair repeated
  eleven times with eleven different confidences.
- **One failed write took the whole dashboard down.** Marts were deleted before
  being rewritten, so a crash left an empty directory; the app checked
  `path.exists()`, which is true for an empty directory. Writes are now atomic
  and checks look at content.
- **A seeded sample that wasn't reproducible.** See the segmentation note above.
- **The pipeline hung forever on Windows.** Hadoop's glob raised
  `UnsatisfiedLinkError` inside a ForkJoinPool, where the exception was swallowed.
  The environment is now validated up front (`invoke doctor`) and fails with an
  actionable message.
- **The first deployment failed.** Streamlit Cloud defaults to Python 3.14 and two
  pinned dependencies had no wheels for it. A CI job now installs the serving
  layer on 3.11 and 3.14 with `--only-binary=:all:` so this surfaces in the PR.

## Running it

**Requirements:** Python 3.11, JDK 17 (PySpark 3.5 does not support Java 21+),
and on Windows `winutils.exe` + `hadoop.dll`.

```bash
git clone https://github.com/criskian/retail-transactions-lakehouse
cd retail-transactions-lakehouse
git lfs pull                      # the dataset is versioned with Git LFS

pip install invoke
invoke install                    # creates .venv and installs dependencies
invoke winutils                   # Windows only
invoke doctor                     # checks Java, Hadoop and the Python workers

invoke pipeline                   # bronze -> silver -> gold -> models  (~7 min)
invoke export                     # builds data/serving/serving.duckdb
invoke app                        # http://localhost:8501
```

`make` offers the same targets on Linux and macOS.

**Adding new data:**

```bash
cp new_store.csv data/landing/Transactions/115_Tran.csv
invoke ingest --check             # what changed
invoke ingest                     # reprocess and re-export if anything did
```

A sample file lives in [`docs/demo_assets/`](docs/demo_assets/).

**With Docker** (serving layer only, ~250 MB):

```bash
docker build -t retail-lakehouse .
docker run -p 8501:8501 retail-lakehouse
```

## Quality

```bash
invoke test      # pytest: end-to-end pipeline on synthetic data + AppTest of all 5 pages
invoke lint      # ruff check + format
```

The suite builds a **miniature lakehouse** from synthetic data shaped like the
real thing, so it runs in CI without the dataset or Git LFS (33 tests, ~80 s).

After every run, `src/pipeline/quality.py` checks **26 data contracts** on the
Gold layer — pandera schemas plus cross-table invariants — and fails the build
if any break. For example: every segmented customer exists in the features
table, KPIs reconcile with the daily aggregates, and no association rule has its
consequent inside its own antecedent.

## Layout

```
src/pipeline/
  spark_session.py   Sanitises the environment before starting the JVM
  bronze.py          CSV -> Parquet, no business logic
  silver.py          explode + catalogue join + transaction_id
  gold.py            15 marts: business + serving
  models.py          K-Means + PCA, FP-Growth, ALS
  quality.py         Data contracts on Gold
  export_serving.py  Gold -> serving.duckdb
  ingest.py          Change detection by sha256, lock and run log
  storage.py         Atomic writes
app/
  theme.py           Validated palette and base ECharts options
  data.py            Data-source resolution; read-only
  views/             One page per module
```

## Stack

**Processing** PySpark 3.5 · Parquet · Spark MLlib
**Serving** DuckDB · Streamlit · Apache ECharts
**Quality** pytest · pandera · ruff
**Operations** GitHub Actions · Docker · Git LFS

Every choice is argued, with what was gained and what was given up, in
[`docs/ADR/`](docs/ADR/) (in Spanish).

## Known limitations

- **No train/test split.** Models are validated with internal metrics
  (silhouette, support/confidence/lift) and qualitatively. A temporal hold-out
  with `precision@10` is the natural next step.
- **ALS cold start.** Customers absent from training are dropped
  (`coldStartStrategy='drop'`). The obvious fallback — recommending the top
  products of the customer's K-Means cluster — is designed but not implemented.
- **FP-Growth capped at the top 200 products.** With `minSupport=0.01` over all
  449 products the FP-tree runs out of memory on a single node.
- **Parquet without a table format.** No ACID or time travel; atomic writes and
  deterministic ids compensate. Delta Lake would be the next step
  ([ADR-0004](docs/ADR/0004-reprocesado-completo.md)).

## About the data

An academic grocery-transactions dataset from a Distributed Data Processing
course. Customer ids are anonymised at source; there are no names, addresses or
payment details.

## Authors

Santiago Espinosa · Cristian Molina — 2026. [MIT](LICENSE) licence.
