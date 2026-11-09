import pandas as pd
import pytest

from pipeline import quality
from pipeline.transform import transform


def _by_name(results):
    return {r.name: r for r in results}


def test_clean_data_passes(raw_items):
    df = transform(*raw_items)
    results = quality.run_checks(df, requested=8)
    assert all(r.passed for r in results), [r for r in results if not r.passed]
    quality.enforce(results)  # does not raise
    s = quality.summary(results)
    assert s["failed"] == 0 and s["passed"] == len(results)


@pytest.mark.parametrize(
    "mutate, check",
    [
        (lambda df: df.assign(title=[""] + list(df["title"][1:])), "title_not_blank"),
        (lambda df: pd.concat([df, df.iloc[[0]]]), "story_id_unique"),
        (lambda df: df.assign(score=[-1] + list(df["score"][1:])), "counts_non_negative"),
        (lambda df: df.assign(rank=[99] + list(df["rank"][1:])), "rank_in_range"),
        (lambda df: df.assign(posted_at=df["snapshot_ts"] + pd.Timedelta(hours=1)), "posted_not_in_future"),
    ],
)
def test_errors_abort_the_run(raw_items, mutate, check):
    df = mutate(transform(*raw_items))
    results = quality.run_checks(df, requested=8)
    assert not _by_name(results)[check].passed
    with pytest.raises(quality.DataQualityError, match=check):
        quality.enforce(results)


def test_too_few_rows_fails(raw_items):
    df = transform(*raw_items)  # 5 rows
    results = quality.run_checks(df, requested=50)
    assert not _by_name(results)["row_count"].passed


def test_empty_frame_fails_only_on_row_count(raw_items):
    _, ts = raw_items
    results = quality.run_checks(transform([], ts), requested=50)
    assert [r.name for r in results] == ["row_count"]
    with pytest.raises(quality.DataQualityError):
        quality.enforce(results)


def test_warnings_do_not_abort(raw_items):
    df = transform(*raw_items)
    df.loc[df["story_kind"] == "link", "domain"] = None
    results = quality.run_checks(df, requested=8)
    assert not _by_name(results)["links_have_domain"].passed
    quality.enforce(results)  # warn severity only
    assert quality.summary(results)["warnings"] == 1
