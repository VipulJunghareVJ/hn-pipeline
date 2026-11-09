"""
Run the whole pipeline:  python -m pipeline.run_pipeline --limit 50

  extract  -> data/raw/YYYY-MM-DD/top_stories.json         (bronze)
  transform-> data/lake/stories/snapshot_date=.../*.parquet (silver)
              data/hacker_news.parquet                      (latest)
  analytics-> web/data/dashboard.json                       (gold / marts)
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline import analytics
from pipeline.extract import extract, save_raw
from pipeline.transform import to_arrow, transform, write_parquet

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
WEB_DATA = ROOT / "web" / "data"

log = logging.getLogger("pipeline")


def export_marts(run_info: dict) -> Path:
    con = analytics.connect(DATA_DIR)
    payload = {
        "run": run_info,
        "metrics": analytics.pipeline_metrics(con, DATA_DIR),
        "top_stories": analytics.records(analytics.top_stories(con, 10)),
        "score_by_hour": analytics.records(analytics.score_by_hour(con)),
        "keywords": analytics.records(analytics.title_keywords(con, 20)),
        "score_distribution": analytics.records(analytics.score_distribution(con)),
        "top_domains": analytics.records(analytics.top_domains(con, 10)),
        "top_authors": analytics.records(analytics.top_authors(con, 10)),
        "daily_trend": analytics.records(analytics.daily_trend(con)),
    }
    WEB_DATA.mkdir(parents=True, exist_ok=True)
    out = WEB_DATA / "dashboard.json"
    out.write_text(json.dumps(payload, indent=1, default=str, allow_nan=False))
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50, help="number of top stories")
    parser.add_argument("--workers", type=int, default=10)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    t0 = time.perf_counter()

    items, snapshot_ts = extract(args.limit, args.workers)
    raw_path = save_raw(items, snapshot_ts, DATA_DIR / "raw")
    log.info("Bronze: %s", raw_path.relative_to(ROOT))

    df = transform(items, snapshot_ts)
    table = to_arrow(df)
    part_path, latest_path = write_parquet(table, snapshot_ts, DATA_DIR / "lake")
    raw_kb = raw_path.stat().st_size / 1024
    pq_kb = latest_path.stat().st_size / 1024
    log.info("Silver: %d rows, raw JSON %.1f KB -> Parquet %.1f KB", len(df), raw_kb, pq_kb)

    run_info = {
        "status": "success",
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.perf_counter() - t0, 2),
        "fetched": len(items),
        "kept": len(df),
        "dropped": len(items) - len(df),
        "raw_json_bytes": raw_path.stat().st_size,
    }
    out = export_marts(run_info)
    log.info("Gold: %s (%.2fs total)", out.relative_to(ROOT), run_info["duration_s"])


if __name__ == "__main__":
    main()
