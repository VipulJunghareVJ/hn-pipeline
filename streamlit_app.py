"""
Streamlit dashboard — queries the Parquet lake live through DuckDB.

Run locally:      streamlit run streamlit_app.py
Deploy free on:   Streamlit Community Cloud (share.streamlit.io)
(Streamlit can't run on Vercel — see README. The Vercel site is in web/.)
"""
from datetime import datetime, timezone
from pathlib import Path

import altair as alt
import streamlit as st

from pipeline import analytics

DATA_DIR = Path(__file__).parent / "data"

st.set_page_config(page_title="HN Daily Analytics", page_icon="🟧", layout="wide")


@st.cache_resource
def get_con():
    return analytics.connect(DATA_DIR)


@st.cache_data(ttl=600)
def load():
    con = get_con()
    return {
        "metrics": analytics.pipeline_metrics(con, DATA_DIR),
        "top": analytics.top_stories(con, 10),
        "hours": analytics.score_by_hour(con),
        "keywords": analytics.title_keywords(con, 20),
        "dist": analytics.score_distribution(con),
        "domains": analytics.top_domains(con, 10),
        "authors": analytics.top_authors(con, 10),
        "trend": analytics.daily_trend(con),
    }


if not (DATA_DIR / "hacker_news.parquet").exists():
    st.error("No data yet. Run `python -m pipeline.run_pipeline` first.")
    st.stop()

d = load()
m = d["metrics"]

# ---- Pipeline health -----------------------------------------------------
st.title("Hacker News daily analytics")
snap = datetime.fromisoformat(m["snapshot_ts"])
age_h = (datetime.now(timezone.utc) - snap).total_seconds() / 3600
fresh = "🟢 Fresh" if age_h < 26 else "🔴 Stale"
st.caption(f"Last snapshot {snap:%d %b %Y, %H:%M} UTC ({age_h:.1f} h ago) · {fresh}")

c = st.columns(5)
c[0].metric("Stories in snapshot", m["records"])
c[1].metric("Total points", f'{m["total_points"]:,}')
c[2].metric("Unique authors", m["unique_authors"])
c[3].metric("Days of history", m["days_of_history"])
c[4].metric("Parquet size", f'{m["parquet_bytes"] / 1024:.1f} KB')

# ---- 1. Top stories ------------------------------------------------------
st.subheader("Top 10 by score")
st.dataframe(
    d["top"],
    hide_index=True,
    width="stretch",
    column_config={
        "url": st.column_config.LinkColumn("link", display_text="open"),
        "score": st.column_config.ProgressColumn(
            "score", format="%d", min_value=0, max_value=int(d["top"]["score"].max())
        ),
    },
)

left, right = st.columns(2)

# ---- 2. Score by hour ----------------------------------------------------
with left:
    st.subheader("Average score by hour posted (UTC)")
    st.altair_chart(
        alt.Chart(d["hours"])
        .mark_bar(color="#ff6600")
        .encode(
            x=alt.X("hour:O", title="Hour (UTC)"),
            y=alt.Y("avg_score:Q", title="Avg score"),
            tooltip=["hour", "stories", "avg_score", "median_score"],
        ),
        width="stretch",
    )

# ---- 3. Keywords ---------------------------------------------------------
with right:
    st.subheader("Most frequent title keywords")
    st.altair_chart(
        alt.Chart(d["keywords"])
        .mark_bar(color="#828282")
        .encode(
            y=alt.Y("word:N", sort="-x", title=None),
            x=alt.X("stories:Q", title="Stories"),
            tooltip=["word", "stories", "total_score"],
        ),
        width="stretch",
    )

# ---- Extras --------------------------------------------------------------
a, b, c2 = st.columns(3)
with a:
    st.subheader("Score distribution")
    st.bar_chart(d["dist"], x="bucket", y="stories", color="#ff6600")
with b:
    st.subheader("Top domains")
    st.dataframe(d["domains"], hide_index=True, width="stretch")
with c2:
    st.subheader("Most active authors")
    st.dataframe(d["authors"], hide_index=True, width="stretch")

st.subheader("Daily average score with 7-day rolling average")
st.line_chart(d["trend"], x="day", y=["avg_score", "rolling_7d_avg"])
