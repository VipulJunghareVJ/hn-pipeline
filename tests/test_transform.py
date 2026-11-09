import pyarrow as pa
import pytest

from pipeline.extract import load_raw, save_raw
from pipeline.transform import SCHEMA, to_arrow, transform, write_observations, write_parquet


def test_filters_and_flattens(raw_items):
    df = transform(*raw_items)
    # deleted, dead and comment items are dropped
    assert list(df["story_id"]) == [101, 102, 103, 104, 108]
    row = df.set_index("story_id")
    assert row.loc[101, "title"] == 'Rust is "fast"'          # HTML unescaped
    assert row.loc[101, "domain"] == "example.com"             # www. stripped
    assert row.loc[101, "top_level_comments"] == 3
    assert row.loc[102, "text"] == "I tried & failed.\n\nTips?"
    assert row.loc[103, "comment_count"] == 0                  # missing descendants
    kinds = row["story_kind"].to_dict()
    assert kinds == {101: "link", 102: "ask", 103: "show", 104: "job", 108: "launch"}


def test_timestamps_are_utc_and_age_positive(raw_items):
    df = transform(*raw_items)
    assert str(df["posted_at"].dt.tz) == "UTC"
    assert (df["age_hours"] > 0).all()


def test_schema_enforced(raw_items):
    table = to_arrow(transform(*raw_items))
    assert table.schema.equals(SCHEMA, check_metadata=False)


def test_schema_rejects_bad_types(raw_items):
    df = transform(*raw_items)
    df["score"] = df["score"].astype(object)
    df.loc[0, "score"] = "lots"
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        to_arrow(df)


def test_empty_input_still_matches_schema(raw_items):
    _, ts = raw_items
    table = to_arrow(transform([], ts))
    assert table.num_rows == 0


def test_parquet_writes_are_idempotent(tmp_path, raw_items):
    items, ts = raw_items
    table = to_arrow(transform(items, ts))
    lake = tmp_path / "data" / "lake"
    write_parquet(table, ts, lake)
    write_parquet(table, ts, lake)
    parts = list((lake / "stories").rglob("*.parquet"))
    assert [p.parent.name for p in parts] == ["snapshot_date=2026-11-09"]
    obs = write_observations(table, ts, lake)
    assert obs.name == "part-060000.parquet"


def test_raw_roundtrip_gzip(tmp_path, raw_items):
    items, ts = raw_items
    path = save_raw(items, ts, tmp_path / "raw")
    assert path.name == "top_stories_060000.json.gz"
    items2, ts2 = load_raw(path)
    assert items2 == items and ts2 == ts
