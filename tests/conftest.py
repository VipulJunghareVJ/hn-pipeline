import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def raw_items():
    payload = json.loads((FIXTURES / "items.json").read_text())
    return payload["items"], datetime.fromisoformat(payload["snapshot_ts"])


@pytest.fixture
def later_raw_file(tmp_path, raw_items):
    """Same stories two hours later: scores up, rank order changed."""
    items, ts = raw_items
    bumped = {101: 20, 102: 200, 103: 10, 104: 0, 108: 60}
    later = []
    for it in json.loads(json.dumps(items)):
        it["score"] = (it.get("score") or 0) + bumped.get(it["id"], 0)
        later.append(it)
    ranks = {102: 1, 101: 2, 108: 3, 103: 4, 104: 5, 105: 6, 106: 7, 107: 8}
    for it in later:
        it["_rank"] = ranks[it["id"]]
    path = tmp_path / "later.json"
    path.write_text(json.dumps({"snapshot_ts": (ts + timedelta(hours=2)).isoformat(), "items": later}))
    return path
