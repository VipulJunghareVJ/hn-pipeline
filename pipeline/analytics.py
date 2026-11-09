"""
ANALYTICS — DuckDB as an embedded warehouse over the Parquet "lake".

DuckDB never imports the files: the views below are lazy scans. Each
query reads only the columns it references (columnar projection) and can
skip row groups using the min/max statistics we wrote in transform.py.
Nothing runs as a server; the "warehouse" is a library call.

Used by both run_pipeline.py (to export JSON for the static site) and
streamlit_app.py (to query live).
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

STOPWORDS = sorted(
    """
    a an the and or but if of to in on at by for with from as is are was were be been
    it its this that these those you your we our i my me he she they them his her their
    how what why when where who which not no yes do does did can could will would should
    about into over under than then so too very just more most new using use used via vs
    show ask hn launch tell all any some one two get got make made out up now first
    """.split()
)


def connect(data_dir: Path) -> duckdb.DuckDBPyConnection:
    data_dir = Path(data_dir)
    latest = (data_dir / "hacker_news.parquet").as_posix()
    history = (data_dir / "lake" / "stories" / "*" / "*.parquet").as_posix()
    obs_dir = data_dir / "lake" / "observations"
    observations = (obs_dir / "*" / "*.parquet").as_posix()
    con = duckdb.connect()  # in-memory catalog; data stays in Parquet on disk
    # DuckDB otherwise renders timestamps in the machine's local zone, so a run
    # on a laptop in IST and one on a UTC CI runner would export different
    # strings for the same instant. Pin everything to UTC.
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"CREATE VIEW stories_latest AS SELECT * FROM read_parquet('{latest}')")
    con.execute(
        f"""
        CREATE VIEW stories_history AS
        SELECT * FROM read_parquet('{history}', hive_partitioning = true)
        """
    )
    # One row per story across all days: keep the most recent observation.
    con.execute(
        """
        CREATE VIEW stories_dedup AS
        SELECT * FROM stories_history
        QUALIFY row_number() OVER (PARTITION BY story_id ORDER BY snapshot_ts DESC) = 1
        """
    )
    # Every run's (story, rank, score): the basis for velocity tracking.
    # Daily story partitions are folded in too, so days collected before
    # per-run observations existed still count as one data point each.
    # (A glob with no matches is an error in DuckDB, hence the existence check.)
    obs_cols = "story_id, rank, score, comment_count, snapshot_ts"
    sources = [f"SELECT {obs_cols} FROM stories_history"]
    if any(obs_dir.glob("*/*.parquet")):
        sources.insert(0, f"SELECT {obs_cols} FROM read_parquet('{observations}', hive_partitioning = true)")
    con.execute(
        f"""
        CREATE VIEW observations AS
        SELECT * FROM ({" UNION ALL ".join(sources)})
        QUALIFY row_number() OVER (PARTITION BY story_id, snapshot_ts) = 1
        """
    )
    return con


# --- 1. Top 10 highest-scoring stories in the latest snapshot -------------
def top_stories(con, n: int = 10) -> pd.DataFrame:
    return con.execute(
        """
        SELECT rank, title, url, domain, author, score, comment_count,
               round(age_hours::DOUBLE, 1)                 AS age_hours,
               round(score / greatest(age_hours::DOUBLE, 1), 1) AS points_per_hour
        FROM stories_latest
        ORDER BY score DESC
        LIMIT ?
        """,
        [n],
    ).df()


# --- 2. Average score by submission hour (UTC), across all history --------
def score_by_hour(con) -> pd.DataFrame:
    return con.execute(
        """
        WITH hours AS (SELECT range AS hour FROM range(24))
        SELECT h.hour,
               count(s.story_id)                  AS stories,
               coalesce(round(avg(s.score), 1), 0) AS avg_score,
               coalesce(median(s.score), 0)        AS median_score
        FROM hours h
        LEFT JOIN stories_dedup s ON hour(s.posted_at) = h.hour
        GROUP BY h.hour
        ORDER BY h.hour
        """
    ).df()


# --- 3. Most frequent title keywords (tokenise + UNNEST) ------------------
def title_keywords(con, n: int = 20) -> pd.DataFrame:
    return con.execute(
        """
        WITH tokens AS (
            SELECT story_id, score,
                   trim(unnest(regexp_split_to_array(lower(title), '[^a-z0-9+#.]+')), '.') AS word
            FROM stories_latest
        )
        SELECT word,
               count(DISTINCT story_id)  AS stories,
               sum(score)::BIGINT        AS total_score
        FROM tokens
        WHERE length(word) > 2
          AND NOT list_contains(?, word)
          AND NOT regexp_full_match(word, '[0-9.]+')
        GROUP BY word
        ORDER BY stories DESC, total_score DESC, word
        LIMIT ?
        """,
        [STOPWORDS, n],
    ).df()


# --- Extras ---------------------------------------------------------------
def score_distribution(con) -> pd.DataFrame:
    return con.execute(
        """
        SELECT CASE
                 WHEN score < 50   THEN '0-49'
                 WHEN score < 100  THEN '50-99'
                 WHEN score < 250  THEN '100-249'
                 WHEN score < 500  THEN '250-499'
                 ELSE '500+'
               END AS bucket,
               min(score) AS sort_key,
               count(*)   AS stories
        FROM stories_latest
        GROUP BY bucket
        ORDER BY sort_key
        """
    ).df().drop(columns="sort_key")


def top_domains(con, n: int = 10) -> pd.DataFrame:
    return con.execute(
        """
        SELECT domain, count(*) AS stories, sum(score)::BIGINT AS total_score
        FROM stories_dedup
        WHERE domain IS NOT NULL
        GROUP BY domain
        ORDER BY stories DESC, total_score DESC
        LIMIT ?
        """,
        [n],
    ).df()


def top_authors(con, n: int = 10) -> pd.DataFrame:
    return con.execute(
        """
        SELECT author, count(*) AS stories, sum(score)::BIGINT AS total_score,
               round(avg(score), 0) AS avg_score
        FROM stories_dedup
        WHERE author IS NOT NULL
        GROUP BY author
        ORDER BY stories DESC, total_score DESC
        LIMIT ?
        """,
        [n],
    ).df()


def daily_trend(con) -> pd.DataFrame:
    """7-day rolling average of front-page score — grows more useful each day."""
    return con.execute(
        """
        WITH daily AS (
            SELECT strftime(snapshot_date::DATE, '%Y-%m-%d') AS day,
                   avg(score)          AS avg_score,
                   count(*)            AS stories
            FROM stories_history
            GROUP BY day
        )
        SELECT day, stories,
               round(avg_score, 1) AS avg_score,
               round(avg(avg_score) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1)
                   AS rolling_7d_avg
        FROM daily
        ORDER BY day
        """
    ).df()


# --- Velocity: how fast stories are gaining points between runs -----------
def rising_stories(con, n: int = 10) -> pd.DataFrame:
    """
    Stories in the latest run ranked by points gained per hour since their
    previous observation. Needs at least two runs that saw the same story;
    returns an empty frame until then.
    """
    return con.execute(
        """
        WITH per_obs AS (
            SELECT story_id, snapshot_ts, score, rank, comment_count,
                   lag(snapshot_ts)   OVER w AS prev_ts,
                   lag(score)         OVER w AS prev_score,
                   lag(rank)          OVER w AS prev_rank,
                   lag(comment_count) OVER w AS prev_comments,
                   first_value(score)       OVER w AS first_score,
                   first_value(snapshot_ts) OVER w AS first_seen
            FROM observations
            WINDOW w AS (PARTITION BY story_id ORDER BY snapshot_ts)
        ),
        latest AS (SELECT max(snapshot_ts) AS ts FROM observations)
        SELECT o.story_id, s.title, s.url, s.domain,
               o.rank,
               o.prev_rank - o.rank                                     AS rank_change,
               o.score,
               o.score - o.prev_score                                   AS score_gain,
               o.comment_count - o.prev_comments                        AS comment_gain,
               round(epoch(o.snapshot_ts - o.prev_ts) / 3600, 2)        AS hours_between,
               round((o.score - o.prev_score)
                     / greatest(epoch(o.snapshot_ts - o.prev_ts) / 3600, 0.25), 1)
                                                                        AS points_per_hour,
               o.score - o.first_score                                  AS gain_since_first_seen,
               strftime(o.first_seen, '%Y-%m-%dT%H:%M:%SZ')             AS first_seen
        FROM per_obs o
        JOIN latest l ON o.snapshot_ts = l.ts
        JOIN stories_latest s USING (story_id)
        WHERE o.prev_ts IS NOT NULL
        ORDER BY points_per_hour DESC, score_gain DESC, o.rank
        LIMIT ?
        """,
        [n],
    ).df()


def score_trajectories(con, story_ids: list[int], hours: int = 48) -> pd.DataFrame:
    """Score over time for the given stories (long format, for a line chart)."""
    if not story_ids:
        return pd.DataFrame(columns=["story_id", "ts", "score", "rank"])
    return con.execute(
        """
        SELECT story_id, strftime(snapshot_ts, '%Y-%m-%dT%H:%M:%SZ') AS ts, score, rank
        FROM observations
        WHERE list_contains(?, story_id)
          AND snapshot_ts >= (SELECT max(snapshot_ts) FROM observations) - to_hours(?)
        ORDER BY story_id, snapshot_ts
        """,
        [story_ids, hours],
    ).df()


def records(df: pd.DataFrame) -> list[dict]:
    """DataFrame -> JSON-safe rows (NaN/NaT become null, which browsers can parse)."""
    return df.astype(object).where(df.notna(), None).to_dict("records")


def pipeline_metrics(con, data_dir: Path) -> dict:
    row = con.execute(
        """
        SELECT count(*), max(snapshot_ts), min(posted_at), max(posted_at),
               sum(score), count(DISTINCT author)
        FROM stories_latest
        """
    ).fetchone()
    days = con.execute("SELECT count(DISTINCT snapshot_date) FROM stories_history").fetchone()[0]
    runs = con.execute("SELECT count(DISTINCT snapshot_ts) FROM observations").fetchone()[0]
    total = con.execute("SELECT count(DISTINCT story_id) FROM stories_history").fetchone()[0]
    parquet = data_dir / "hacker_news.parquet"
    return {
        "records": row[0],
        "snapshot_ts": row[1].isoformat() if row[1] else None,
        "oldest_story": row[2].isoformat() if row[2] else None,
        "newest_story": row[3].isoformat() if row[3] else None,
        "total_points": int(row[4] or 0),
        "unique_authors": row[5],
        "days_of_history": days,
        "stories_in_history": total,
        "runs_observed": runs,
        "parquet_bytes": parquet.stat().st_size if parquet.exists() else 0,
    }
