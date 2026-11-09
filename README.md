# Hacker News Daily Analytics: a micro data pipeline

A miniature Modern Data Stack that costs ₹0 to run: Python extracts the Hacker News
top stories every day, stores them as compressed, partitioned Parquet, models them
with DuckDB, and publishes a dashboard on Vercel.

```
 Hacker News Firebase API
        │  pipeline/extract.py      requests + retries + thread pool
        ▼
 data/raw/YYYY-MM-DD/top_stories.json                      ← bronze (raw, untouched)
        │  pipeline/transform.py    clean, flatten, strict Arrow schema
        ▼
 data/lake/stories/snapshot_date=YYYY-MM-DD/part-0.parquet ← silver (ZSTD, Hive-partitioned)
 data/hacker_news.parquet                                  ← latest snapshot
        │  pipeline/analytics.py    DuckDB views + SQL, no server, no import
        ▼
 web/data/dashboard.json                                   ← gold (analytics marts)
        │
        ├──► web/index.html  →  Vercel (static, free)
        └──► streamlit_app.py → Streamlit Community Cloud (free, queries DuckDB live)

 Orchestration: GitHub Actions cron, once a day, commits the new data → Vercel redeploys.
```

## Why it’s built this way

**Streamlit is not deployed to Vercel.** Streamlit runs as a long-lived server that talks
to the browser over WebSockets; Vercel runs short-lived serverless functions and does not
support that. So the pipeline precomputes the analytics with DuckDB and Vercel serves a
static page, which is faster and genuinely free. The Streamlit app is still included and
deploys to Streamlit Community Cloud, which is built for it.

**GitHub Actions is the scheduler.** A cron workflow is the lightest orchestrator there is,
and committing the data back to the repo means the Parquet history is versioned in git.

## Concepts demonstrated

| Concept | Where |
|---|---|
| Resilient extraction (retries, backoff, timeouts, bounded concurrency) | `extract.py` |
| Bronze / silver / gold layering (medallion architecture) | `data/raw`, `data/lake`, `web/data` |
| Schema enforcement that fails loudly | `transform.py` → `SCHEMA` |
| Columnar storage: ZSTD compression, dictionary encoding, column statistics | `transform.py` → `write_parquet` |
| Hive partitioning and idempotent daily loads | `snapshot_date=…` folders, same-day reruns overwrite |
| Querying files in place (projection pushdown, no RAM load) | `analytics.py` → `read_parquet` views |
| Window functions: `QUALIFY` dedup, 7-day rolling average | `stories_dedup`, `daily_trend` |
| String tokenizing with `regexp_split_to_array` + `UNNEST` | `title_keywords` |
| Data freshness / pipeline observability | metrics strip on both dashboards |

## Run it locally (about 5 minutes)

Requires Python 3.10+.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python -m pipeline.run_pipeline --limit 50
```

You should see a log like `Silver: 50 rows, raw JSON 40.1 KB -> Parquet 18.3 KB`.

Then look at the results either way:

```bash
streamlit run streamlit_app.py      # Streamlit app at http://localhost:8501
python -m http.server -d web 8000   # static site at http://localhost:8000
```

Explore the data yourself with the DuckDB CLI or Python:

```python
import duckdb
duckdb.sql("SELECT domain, count(*) FROM 'data/hacker_news.parquet' GROUP BY 1 ORDER BY 2 DESC").show()
duckdb.sql("DESCRIBE SELECT * FROM 'data/hacker_news.parquet'").show()
```

## Deploy

### 1. Push to GitHub
Create an empty repo, then:
```bash
git init && git add . && git commit -m "Initial pipeline + first snapshot"
git branch -M main
git remote add origin https://github.com/<you>/hn-pipeline.git
git push -u origin main
```
Commit the `data/` and `web/data/` output from your first local run, so the sites have
something to show right away.

### 2. Turn on the daily job
In the repo: **Settings → Actions → General → Workflow permissions → Read and write
permissions → Save.** Then go to **Actions → Daily HN pipeline → Run workflow** once to
check it works. After that it runs every day at 06:15 UTC.

### 3. Vercel (static dashboard)
1. vercel.com → **Add New → Project** → import the repo.
2. **Root Directory: `web`** (click Edit next to it). This is the one important setting:
   Vercel only sees the static site, never the Python code.
3. Framework Preset: **Other**. Leave build and output settings empty. Deploy.

Every commit the daily job makes triggers a new Vercel deployment automatically.

### 4. Streamlit Community Cloud (optional, live DuckDB queries)
share.streamlit.io → **Create app** → pick the repo, branch `main`, main file
`streamlit_app.py` → Deploy. It installs `requirements.txt` and picks up new data on
each push.

## Project layout

```
hn-pipeline/
├── pipeline/
│   ├── extract.py        API → raw JSON
│   ├── transform.py      raw JSON → typed Parquet
│   ├── analytics.py      DuckDB views and SQL queries
│   └── run_pipeline.py   runs everything, exports dashboard.json
├── streamlit_app.py      live dashboard (Streamlit Cloud)
├── web/
│   ├── index.html        static dashboard (Vercel)
│   ├── vercel.json
│   └── data/             dashboard.json written here by the pipeline
├── data/                 raw JSON + Parquet lake (written by the pipeline)
├── .github/workflows/daily-pipeline.yml
├── requirements.txt            pipeline + Streamlit
└── requirements-pipeline.txt   pipeline only (CI)
```

## Troubleshooting

**The workflow fails on `git push` with a 403.** Workflow permissions are still read-only;
see step 2.

**The daily job stopped running.** GitHub pauses scheduled workflows in repos with no
activity for 60 days. Re-enable it from the Actions tab.

**The Vercel page says “No snapshot found”.** `web/data/dashboard.json` isn’t in the repo
yet. Run the pipeline locally or trigger the workflow, then check the file was committed.

**`InvalidInputException: Required module 'pytz'`.** Run `pip install -r requirements.txt`
again; DuckDB needs `pytz` for timezone-aware timestamps.

## Ideas to extend it

- Load DuckDB-WASM in `index.html` and let visitors run SQL against the Parquet in their browser.
- Add data-quality checks before writing (no null titles, scores ≥ 0, row count within range) and fail the run if they break.
- Track each story’s score across snapshots to chart how fast posts rise and fall.
- Swap pandas for Polars in `transform.py` and compare runtimes.
