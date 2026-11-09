"""
TRANSFORM — turn raw API JSON into a clean, typed, columnar dataset
(the "silver" layer).

What happens here:
  * drop deleted / dead / non-story items
  * flatten nested fields (kids -> top_level_comments count, url -> domain)
  * unescape + strip HTML from self-post text (Ask HN / Show HN bodies)
  * Unix epoch seconds -> timezone-aware UTC timestamps
  * enforce an explicit Arrow schema, so a surprise type from the API
    fails loudly here instead of silently corrupting downstream queries
  * write ZSTD-compressed Parquet, Hive-partitioned by snapshot_date
  * append a slim "observation" row per story per run, so score and rank
    can be tracked over time (velocity / rising stories)
"""
from __future__ import annotations

import html
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        pa.field("story_id", pa.int64(), nullable=False),
        pa.field("rank", pa.int16(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("url", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("author", pa.string()),
        pa.field("score", pa.int32(), nullable=False),
        pa.field("comment_count", pa.int32(), nullable=False),
        pa.field("top_level_comments", pa.int32(), nullable=False),
        pa.field("story_kind", pa.string(), nullable=False),  # link / ask / show / launch / job
        pa.field("text", pa.string()),
        pa.field("posted_at", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("snapshot_ts", pa.timestamp("s", tz="UTC"), nullable=False),
        pa.field("age_hours", pa.float32(), nullable=False),
    ]
)

# Slim per-run fact table: one row per story per pipeline run.
OBSERVATION_COLUMNS = ["story_id", "rank", "score", "comment_count", "snapshot_ts"]

_TAG_RE = re.compile(r"<[^>]+>")


def _clean_text(raw: str | None) -> str | None:
    if not raw:
        return None
    text = raw.replace("<p>", "\n\n")
    text = _TAG_RE.sub("", text)
    text = html.unescape(text).strip()
    return text or None


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).netloc.lower()
    return host.removeprefix("www.") or None


def _story_kind(item: dict) -> str:
    if item.get("type") == "job":
        return "job"
    title = (item.get("title") or "").lower()
    for prefix, kind in (("ask hn", "ask"), ("show hn", "show"), ("launch hn", "launch")):
        if title.startswith(prefix):
            return kind
    return "link" if item.get("url") else "ask"


def transform(items: list[dict], snapshot_ts: datetime) -> pd.DataFrame:
    rows = []
    for it in items:
        if it.get("deleted") or it.get("dead"):
            continue
        if it.get("type") not in ("story", "job") or not it.get("title"):
            continue
        rows.append(
            {
                "story_id": it["id"],
                "rank": it["_rank"],
                "title": html.unescape(it["title"]).strip(),
                "url": it.get("url"),
                "domain": _domain(it.get("url")),
                "author": it.get("by"),
                "score": it.get("score") or 0,
                "comment_count": it.get("descendants") or 0,  # jobs have no descendants
                "top_level_comments": len(it.get("kids") or []),
                "story_kind": _story_kind(it),
                "text": _clean_text(it.get("text")),
                "posted_at": it.get("time"),
            }
        )

    df = pd.DataFrame(rows, columns=[f.name for f in SCHEMA if f.name not in ("snapshot_ts", "age_hours")])
    df["posted_at"] = pd.to_datetime(df["posted_at"], unit="s", utc=True)
    df["snapshot_ts"] = pd.Timestamp(snapshot_ts).tz_convert("UTC").floor("s")
    df["age_hours"] = (df["snapshot_ts"] - df["posted_at"]).dt.total_seconds() / 3600
    df = df.drop_duplicates("story_id").sort_values("rank").reset_index(drop=True)
    # explicit dtypes so an empty frame still converts cleanly to the schema
    return df.astype({"story_id": "int64", "rank": "int16", "score": "int32",
                      "comment_count": "int32", "top_level_comments": "int32",
                      "age_hours": "float32"})


def to_arrow(df: pd.DataFrame) -> pa.Table:
    # from_pandas + schema = schema enforcement; raises on incompatible types/nulls
    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def write_parquet(table: pa.Table, snapshot_ts: datetime, lake_dir: Path) -> tuple[Path, Path]:
    """
    Writes two copies:
      lake/stories/snapshot_date=YYYY-MM-DD/part-0.parquet  (history, one per day)
      hacker_news.parquet                                    (latest snapshot)
    Re-running on the same day overwrites that day's partition (idempotent).
    """
    part_dir = lake_dir / "stories" / f"snapshot_date={snapshot_ts:%Y-%m-%d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    part_path = part_dir / "part-0.parquet"
    latest_path = lake_dir.parent / "hacker_news.parquet"

    for path in (part_path, latest_path):
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            table,
            path,
            compression="zstd",
            compression_level=6,
            use_dictionary=["domain", "author", "story_kind"],  # low-cardinality columns
            write_statistics=True,  # min/max per row group -> DuckDB can skip data
        )
    return part_path, latest_path


def write_observations(table: pa.Table, snapshot_ts: datetime, lake_dir: Path) -> Path:
    """
    Append-only history of (story_id, rank, score, comments) per run:
      lake/observations/snapshot_date=YYYY-MM-DD/part-HHMMSS.parquet

    Unlike the daily stories partition, every run keeps its own file, which
    is what lets analytics compute how fast a story is gaining points.
    Re-running at the exact same second overwrites (still idempotent).
    """
    ts = pd.Timestamp(snapshot_ts).tz_convert("UTC")
    part_dir = lake_dir / "observations" / f"snapshot_date={ts:%Y-%m-%d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / f"part-{ts:%H%M%S}.parquet"
    pq.write_table(table.select(OBSERVATION_COLUMNS), path, compression="zstd",
                   compression_level=6, write_statistics=True)
    return path
