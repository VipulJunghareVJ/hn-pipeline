"""End-to-end: replay fixture bronze files through every stage, no network."""
import json

import pytest

from pipeline import analytics, quality
from pipeline.run_pipeline import run

FIXTURE = "tests/fixtures/items.json"


@pytest.fixture
def dirs(tmp_path):
    return tmp_path / "data", tmp_path / "web"


def test_single_run_exports_dashboard(dirs):
    data_dir, web_dir = dirs
    info = run(from_raw=FIXTURE, data_dir=data_dir, web_dir=web_dir)
    assert info["kept"] == 5 and info["dropped"] == 3
    d = json.loads((web_dir / "data" / "dashboard.json").read_text())
    assert d["run"]["quality"]["failed"] == 0
    assert d["metrics"]["records"] == 5
    assert d["metrics"]["runs_observed"] == 1
    assert d["rising_stories"] == []                       # nothing to compare yet
    assert d["metrics"]["snapshot_ts"].endswith("+00:00")  # always UTC
    assert d["top_stories"][0]["title"] == 'Rust is "fast"'


def test_two_runs_give_velocity(dirs, later_raw_file):
    data_dir, web_dir = dirs
    run(from_raw=FIXTURE, data_dir=data_dir, web_dir=web_dir)
    run(from_raw=later_raw_file, data_dir=data_dir, web_dir=web_dir)
    d = json.loads((web_dir / "data" / "dashboard.json").read_text())
    rising = d["rising_stories"]
    assert rising[0]["story_id"] == 102
    assert rising[0]["score_gain"] == 200
    assert rising[0]["points_per_hour"] == 100.0
    assert rising[0]["rank_change"] == 1                   # 2 -> 1
    assert d["metrics"]["runs_observed"] == 2
    assert d["metrics"]["days_of_history"] == 1            # same day, one daily partition
    traj = [t for t in d["score_trajectories"] if t["story_id"] == 102]
    assert [t["score"] for t in traj] == [80, 280]


def test_bad_data_never_reaches_the_lake(dirs, tmp_path, raw_items):
    data_dir, web_dir = dirs
    items, ts = raw_items
    for it in items:
        it["score"] = -5
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"snapshot_ts": ts.isoformat(), "items": items}))
    with pytest.raises(quality.DataQualityError):
        run(from_raw=bad, data_dir=data_dir, web_dir=web_dir)
    assert not (data_dir / "lake").exists()
    assert not (web_dir / "data" / "dashboard.json").exists()


def test_timezone_pinned_to_utc(dirs):
    data_dir, web_dir = dirs
    run(from_raw=FIXTURE, data_dir=data_dir, web_dir=web_dir)
    con = analytics.connect(data_dir)
    assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
