"""
dashboard/app.py
─────────────────
Streamlit dashboard for the Wikipedia clickstream pipeline.

Reads batch Parquet output (data/batch_output/) and streaming Parquet
output (data/streaming_output/) and renders interactive charts.

Usage:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config

# ── Paths ──────────────────────────────────────────────────────────────────────
BATCH_DIR = Path(config.BATCH_OUTPUT_DIR)
STREAMING_DIR = Path(config.STREAMING_OUTPUT_DIR)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Wikipedia Pipeline Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("Wikipedia Pipeline Dashboard")
st.caption(
    "Lambda-architecture demo · "
    "Batch layer: monthly clickstream TSV dumps · "
    "Streaming layer: Wikimedia EventStreams recentchange feed"
)

# ── Data loaders ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_batch(name: str) -> pd.DataFrame:
    path = BATCH_DIR / name
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        st.error(f"Could not read {name}: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_streaming(name: str) -> pd.DataFrame:
    path = STREAMING_DIR / name
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        st.error(f"Could not read {name}: {exc}")
        return pd.DataFrame()


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_batch, tab_streaming = st.tabs(["Batch — Clickstream Analysis", "Streaming — Live Edits"])


# ══════════════════════════════════════════════════════════════════════════════
# BATCH TAB
# ══════════════════════════════════════════════════════════════════════════════
with tab_batch:
    indegree_df = load_batch("top_articles_indegree")
    excluded_batch_articles = {"Main_Page", "Hyphen-minus"}

    if indegree_df.empty:
        st.info(
            "No batch output found.  "
            "Download data first (`python ingestion/download_clickstream.py`) "
            "then run the batch job (`python batch/clickstream_batch.py`)."
        )
    else:
        all_months = sorted(indegree_df["month"].dropna().unique().tolist(), reverse=True)
        selected_month = st.selectbox("Month", all_months, key="batch_month")

        # ── Row 1: indegree + outdegree ────────────────────────────────────────
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top 20 Articles by Inbound Clicks")
            df = (
                indegree_df[
                    (indegree_df["month"] == selected_month) &
                    (~indegree_df["curr"].isin(excluded_batch_articles))
                ]
                .nlargest(20, "total_clicks")
            )
            fig = px.bar(
                df,
                x="total_clicks",
                y="curr",
                orientation="h",
                labels={"curr": "Article", "total_clicks": "Inbound Clicks"},
                color="total_clicks",
                color_continuous_scale="Blues",
            )
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False,
                height=520,
                margin=dict(l=0, r=0, t=30, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Top 20 Articles by Outbound Clicks")
            outdegree_df = load_batch("top_articles_outdegree")
            if not outdegree_df.empty:
                df = (
                    outdegree_df[outdegree_df["month"] == selected_month]
                    .nlargest(20, "total_clicks_sent")
                )
                fig = px.bar(
                    df,
                    x="total_clicks_sent",
                    y="prev",
                    orientation="h",
                    labels={"prev": "Article", "total_clicks_sent": "Outbound Clicks"},
                    color="total_clicks_sent",
                    color_continuous_scale="Greens",
                )
                fig.update_layout(
                    yaxis=dict(autorange="reversed"),
                    coloraxis_showscale=False,
                    height=520,
                    margin=dict(l=0, r=0, t=30, b=0),
                )
                st.plotly_chart(fig, use_container_width=True)

        # ── Row 2: top link pairs ──────────────────────────────────────────────
        st.subheader("Top 20 Link Pairs")
        pairs_df = load_batch("top_link_pairs")
        if not pairs_df.empty:
            df = (
                pairs_df[pairs_df["month"] == selected_month]
                .nlargest(20, "total_clicks")
                .copy()
            )
            df["link"] = df["prev"] + "  →  " + df["curr"]
            fig = px.bar(
                df,
                x="total_clicks",
                y="link",
                orientation="h",
                labels={"link": "Link Pair", "total_clicks": "Clicks"},
                color="total_clicks",
                color_continuous_scale="Oranges",
            )
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False,
                height=600,
                margin=dict(l=0, r=0, t=30, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── Row 3: monthly trend ───────────────────────────────────────────────
        st.subheader("Monthly Trend — Top 10 Articles (all months)")
        trend_df = load_batch("monthly_trend")
        if not trend_df.empty:
            top_articles = (
                trend_df[~trend_df["curr"].isin(excluded_batch_articles)]
                .groupby("curr")["monthly_clicks"]
                .sum()
                .nlargest(10)
                .index.tolist()
            )
            filtered = trend_df[trend_df["curr"].isin(top_articles)].copy()
            filtered = filtered.sort_values("month")
            fig = px.line(
                filtered,
                x="month",
                y="monthly_clicks",
                color="curr",
                markers=True,
                labels={
                    "curr": "Article",
                    "monthly_clicks": "Monthly Clicks",
                    "month": "Month",
                },
            )
            fig.update_layout(
                height=420,
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING TAB
# ══════════════════════════════════════════════════════════════════════════════
with tab_streaming:
    refresh = st.button("Refresh streaming data")
    if refresh:
        st.cache_data.clear()

    edits_df = load_streaming("edits_per_wiki_per_minute")
    breakdown_df = load_streaming("edit_type_breakdown")
    editors_df = load_streaming("top_editors_sliding")

    if edits_df.empty and breakdown_df.empty and editors_df.empty:
        st.info(
            "No streaming data yet.  "
            "Start the pipeline:  \n"
            "1. `python ingestion/sse_to_kafka.py`  \n"
            "2. `python streaming/kafka_to_spark.py`"
        )
    else:
        # ── KPI row ───────────────────────────────────────────────────────────
        if not edits_df.empty:
            total_edits = int(edits_df["edit_count"].sum())
            total_bots = int(edits_df["bot_edits"].sum())
            bot_pct = total_bots / total_edits * 100 if total_edits else 0
            latest_ts = edits_df["window_start"].max()

            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            kpi1.metric("Total Edits", f"{total_edits:,}")
            kpi2.metric("Bot Edits", f"{total_bots:,}")
            kpi3.metric("Bot %", f"{bot_pct:.1f}%")
            kpi4.metric("Latest Window", str(latest_ts)[:16] if pd.notna(latest_ts) else "—")

        # ── Edit rate chart ───────────────────────────────────────────────────
        if not edits_df.empty:
            st.subheader("Edit Rate Over Time (enwiki)")
            enwiki = (
                edits_df[edits_df["wiki"] == "enwiki"]
                .sort_values("window_start")
            )
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=enwiki["window_start"], y=enwiki["edit_count"],
                name="Total edits", fill="tozeroy",
                line=dict(color="#636EFA"),
            ))
            fig.add_trace(go.Scatter(
                x=enwiki["window_start"], y=enwiki["bot_edits"],
                name="Bot edits", fill="tozeroy",
                line=dict(color="#EF553B"),
            ))
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Edits / minute",
                height=360,
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── Edit type breakdown ───────────────────────────────────────────────
        if not breakdown_df.empty:
            st.subheader("Edit Type Breakdown")
            col1, col2 = st.columns([1, 2])

            total_by_type = (
                breakdown_df.groupby("type")["count"]
                .sum()
                .reset_index()
            )
            with col1:
                fig = px.pie(
                    total_by_type,
                    names="type",
                    values="count",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hole=0.4,
                )
                fig.update_layout(height=320, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                time_df = breakdown_df.sort_values("window_start")
                fig2 = px.line(
                    time_df,
                    x="window_start",
                    y="count",
                    color="type",
                    labels={"window_start": "Time", "count": "Events"},
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig2.update_layout(
                    height=320,
                    margin=dict(l=0, r=0, t=30, b=0),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig2, use_container_width=True)

        # ── Top editors ───────────────────────────────────────────────────────
        if not editors_df.empty:
            st.subheader("Top Editors — Latest 5-minute Window (human edits only)")
            latest_window = editors_df["window_start"].max()
            latest_editors = (
                editors_df[editors_df["window_start"] == latest_window]
                .nlargest(15, "edit_count")
            )
            fig = px.bar(
                latest_editors,
                x="edit_count",
                y="user",
                orientation="h",
                color="edit_count",
                color_continuous_scale="Purples",
                labels={"user": "Editor", "edit_count": "Edits"},
            )
            fig.update_layout(
                yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False,
                height=460,
                margin=dict(l=0, r=0, t=30, b=0),
            )
            st.plotly_chart(fig, use_container_width=True)
