"""
EXTRACT — pull the current top stories from the Hacker News Firebase API
and land them untouched as raw JSON (the "bronze" layer).

API docs: https://github.com/HackerNews/API
  GET /v0/topstories.json      -> list of up to 500 item ids, ranked
  GET /v0/item/<id>.json       -> one item (story, comment, job, poll...)

The API has no auth and no documented rate limit, but we still behave
politely: a shared session, bounded concurrency, timeouts, and retries
with exponential backoff on 429/5xx.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://hacker-news.firebaseio.com/v0"
log = logging.getLogger(__name__)


def make_session(max_workers: int) -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=0.5,  # 0.5s, 1s, 2s, 4s, 8s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=max_workers)
    session = requests.Session()
    session.mount("https://", adapter)
    session.headers["User-Agent"] = "hn-micro-pipeline/1.0 (learning project)"
    return session


def fetch_top_story_ids(session: requests.Session, limit: int) -> list[int]:
    resp = session.get(f"{BASE_URL}/topstories.json", timeout=10)
    resp.raise_for_status()
    return resp.json()[:limit]


def fetch_item(session: requests.Session, item_id: int) -> dict | None:
    try:
        resp = session.get(f"{BASE_URL}/item/{item_id}.json", timeout=10)
        resp.raise_for_status()
        return resp.json()  # can legitimately be null for purged items
    except requests.RequestException as exc:
        log.warning("Item %s failed after retries: %s", item_id, exc)
        return None


def extract(limit: int = 50, max_workers: int = 10) -> tuple[list[dict], datetime]:
    """Return (raw items with their front-page rank attached, snapshot time)."""
    snapshot_ts = datetime.now(timezone.utc)
    session = make_session(max_workers)

    ids = fetch_top_story_ids(session, limit)
    rank_of = {item_id: rank for rank, item_id in enumerate(ids, start=1)}
    log.info("Got %d top story ids", len(ids))

    items: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_item, session, i): i for i in ids}
        for fut in as_completed(futures):
            item = fut.result()
            if item:
                item["_rank"] = rank_of[futures[fut]]
                items.append(item)

    items.sort(key=lambda x: x["_rank"])
    log.info("Fetched %d/%d items", len(items), len(ids))
    return items, snapshot_ts


def save_raw(items: list[dict], snapshot_ts: datetime, raw_dir: Path) -> Path:
    """Bronze layer: keep exactly what the API returned, partitioned by day."""
    day_dir = raw_dir / snapshot_ts.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / "top_stories.json"
    payload = {"snapshot_ts": snapshot_ts.isoformat(), "items": items}
    path.write_text(json.dumps(payload, indent=1))
    return path
