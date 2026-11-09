"""
DATA QUALITY — checks that run on the silver DataFrame *before* anything is
written to the lake.

The Arrow schema in transform.py already guarantees types and non-null
columns. These checks cover the things a schema can't express: business
rules ("scores are never negative"), uniqueness, plausibility of volumes,
and freshness. Each check returns a result row; any failed check with
severity "error" aborts the run so bad data never reaches the lake or the
dashboards. Warnings are recorded but don't stop the run.

The results are exported into dashboard.json, so the dashboards can show
exactly which checks ran and how they did.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

ERROR, WARN = "error", "warn"


@dataclass
class CheckResult:
    name: str
    severity: str  # "error" | "warn"
    passed: bool
    detail: str

    def to_dict(self) -> dict:
        return asdict(self)


class DataQualityError(RuntimeError):
    def __init__(self, failures: list[CheckResult]):
        self.failures = failures
        lines = "\n".join(f"  - {f.name}: {f.detail}" for f in failures)
        super().__init__(f"{len(failures)} data-quality check(s) failed:\n{lines}")


def _check(name: str, severity: str, bad: int, ok_msg: str, bad_msg: str) -> CheckResult:
    return CheckResult(name, severity, bad == 0, ok_msg if bad == 0 else bad_msg.format(n=bad))


def run_checks(
    df: pd.DataFrame,
    *,
    requested: int,
    min_keep_ratio: float = 0.5,
    future_tolerance_s: int = 300,
    max_plausible_score: int = 10_000,
) -> list[CheckResult]:
    """Run every check and return all results (never raises)."""
    results: list[CheckResult] = []
    n = len(df)

    # Volume: an empty or tiny load usually means the API or extract broke.
    min_rows = max(1, int(requested * min_keep_ratio))
    results.append(
        CheckResult(
            "row_count",
            ERROR,
            min_rows <= n <= requested,
            f"{n} rows (expected {min_rows}–{requested})",
        )
    )
    if n == 0:
        return results  # nothing else is meaningful on an empty frame

    titles = df["title"].fillna("").str.strip()
    results.append(_check("title_not_blank", ERROR, int((titles == "").sum()),
                          "all titles present", "{n} blank title(s)"))

    results.append(_check("story_id_unique", ERROR, int(df["story_id"].duplicated().sum()),
                          "no duplicate story ids", "{n} duplicate story id(s)"))

    results.append(_check("rank_unique", ERROR, int(df["rank"].duplicated().sum()),
                          "ranks are unique", "{n} duplicate rank(s)"))

    out_of_range = int(((df["rank"] < 1) | (df["rank"] > requested)).sum())
    results.append(_check("rank_in_range", ERROR, out_of_range,
                          f"ranks within 1–{requested}", "{n} rank(s) out of range"))

    negative = int(((df["score"] < 0) | (df["comment_count"] < 0) | (df["top_level_comments"] < 0)).sum())
    results.append(_check("counts_non_negative", ERROR, negative,
                          "scores and comment counts ≥ 0", "{n} row(s) with negative counts"))

    # Note: top_level_comments can legitimately exceed comment_count, because
    # the API's `kids` list includes dead/deleted comments that `descendants`
    # leaves out, so that is deliberately *not* a check.
    implausible = int((df["score"] > max_plausible_score).sum())
    results.append(_check("score_plausible", WARN, implausible,
                          f"all scores ≤ {max_plausible_score:,}",
                          "{n} story(ies) with an implausibly high score"))

    future = int((df["posted_at"] > df["snapshot_ts"] + pd.Timedelta(seconds=future_tolerance_s)).sum())
    results.append(_check("posted_not_in_future", ERROR, future,
                          "no stories posted after the snapshot", "{n} story(ies) dated in the future"))

    links = df[df["story_kind"] == "link"]
    no_domain = int(links["domain"].isna().sum())
    results.append(_check("links_have_domain", WARN, no_domain,
                          "every link story has a domain", "{n} link story(ies) without a domain"))

    return results


def enforce(results: list[CheckResult]) -> None:
    """Raise if any error-severity check failed."""
    failures = [r for r in results if not r.passed and r.severity == ERROR]
    if failures:
        raise DataQualityError(failures)


def summary(results: list[CheckResult]) -> dict:
    """Compact form for dashboard.json."""
    return {
        "passed": sum(r.passed for r in results),
        "failed": sum(not r.passed and r.severity == ERROR for r in results),
        "warnings": sum(not r.passed and r.severity == WARN for r in results),
        "checks": [r.to_dict() for r in results],
    }
