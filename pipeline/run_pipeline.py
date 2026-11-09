"""
Run the whole pipeline:  python -m pipeline.run_pipeline --limit 50

  extract  -> data/raw/YYYY-MM-DD/top_stories_HHMMSS.json.gz        (bronze)
  quality  -> checks on the cleaned frame; abort before writing if broken
  transform-> data/lake/stories/snapshot_date=.../part-0.parquet      (silver, daily)
              data/lake/observations/snapshot_date=.../part-*.parquet (silver, every run)
              data/hacker_news.parquet                                (latest)
  analytics-> web/data/dashboard.json                                 (gold / marts)

Replay a saved bronze file instead of calling the API (no network needed):
  python -m pipeline.run_pipeline --from-raw data/raw/2026-11-09/top_stories.json

Write somewhere other than the repo (handy for experiments and tests):
  python -m pipeline.run_pipeline --data-dir /tmp/hn/data --web-dir /tmp/hn/web
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline import analytics, quality
from pipeline.extract import extract, load_raw, save_raw
from pipeline.transform import to_arrow, transform, write_observations, write_parquet

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
WEB_DIR = ROOT / "web"

log = logging.getLogger("pipeline")


def build_marts(data_dir: Path) -> dict:
    con = analytics.connect(data_dir)
    rising = analytics.rising_stories(con, 10)
    top_ids = [int(x) for x in rising["story_id"].head(5)] if len(rising) else []
    return {
        "metrics": analytics.pipeline_metrics(con, data_dir),
        "top_stories": analytics.records(analytics.top_stories(con, 10)),
        "rising_stories": analytics.records(rising),
        "score_trajectories": analytics.records(analytics.score_trajectories(con, top_ids)),
        "score_by_hour": analytics.records(analytics.score_by_hour(con)),
        "keywords": analytics.records(analytics.title_keywords(con, 20)),
        "score_distribution": analytics.records(analytics.score_distribution(con)),
        "top_domains": analytics.records(analytics.top_domains(con, 10)),
        "top_authors": analytics.records(analytics.top_authors(con, 10)),
        "daily_trend": analytics.records(analytics.daily_trend(con)),
    }


def export_marts(run_info: dict, data_dir: Path = DATA_DIR, web_dir: Path = WEB_DIR) -> Path:
    payload = {"run": run_info, **build_marts(data_dir)}
    out_dir = web_dir / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "dashboard.json"
    out.write_text(json.dumps(payload, indent=1, default=str, allow_nan=False))
    return out


def run(
    limit: int = 50,
    workers: int = 10,
    *,
    data_dir: Path = DATA_DIR,
    web_dir: Path = WEB_DIR,
    from_raw: Path | None = None,
) -> dict:
    """Run every stage; returns run_info. Raises DataQualityError on bad data."""
    t0 = time.perf_counter()
    data_dir, web_dir = Path(data_dir), Path(web_dir)

    if from_raw:
        raw_path = Path(from_raw)
        items, snapshot_ts = load_raw(raw_path)
        log.info("Bronze (replay): %s, %d items", raw_path, len(items))
    else:
        items, snapshot_ts = extract(limit, workers)
        raw_path = save_raw(items, snapshot_ts, data_dir / "raw")
        log.info("Bronze: %s", raw_path)

    df = transform(items, snapshot_ts)

    # Quality gate: nothing below this line runs if an error-level check fails.
    # For a replay, the original --limit is whatever the highest rank was.
    requested = max((it.get("_rank", 0) for it in items), default=0) if from_raw else limit
    checks = quality.run_checks(df, requested=requested)
    for c in checks:
        (log.info if c.passed else log.warning)("DQ %-26s %s  %s", c.name, "ok  " if c.passed else c.severity.upper(), c.detail)
    quality.enforce(checks)

    table = to_arrow(df)
    _, latest_path = write_parquet(table, snapshot_ts, data_dir / "lake")
    obs_path = write_observations(table, snapshot_ts, data_dir / "lake")
    raw_kb = raw_path.stat().st_size / 1024
    pq_kb = latest_path.stat().st_size / 1024
    log.info("Silver: %d rows, raw %.1f KB -> Parquet %.1f KB; observation %s",
             len(df), raw_kb, pq_kb, obs_path.name)

    run_info = {
        "status": "success",
        "mode": "replay" if from_raw else "live",
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.perf_counter() - t0, 2),
        "fetched": len(items),
        "kept": len(df),
        "dropped": len(items) - len(df),
        "raw_bytes": raw_path.stat().st_size,
        "quality": quality.summary(checks),
    }
    out = export_marts(run_info, data_dir, web_dir)
    log.info("Gold: %s (%.2fs total)", out, run_info["duration_s"])
    return run_info


def main() -> None:
    parser = argparse.ArgumentParser(description="Hacker News micro data pipeline")
    parser.add_argument("--limit", type=int, default=50, help="number of top stories")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--from-raw", type=Path, help="replay a saved bronze .json/.json.gz instead of calling the API")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--web-dir", type=Path, default=WEB_DIR)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run(args.limit, args.workers, data_dir=args.data_dir, web_dir=args.web_dir, from_raw=args.from_raw)
    except quality.DataQualityError as exc:
        log.error("%s", exc)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
