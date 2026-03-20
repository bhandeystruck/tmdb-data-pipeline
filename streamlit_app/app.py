import pandas as pd
import snowflake.connector
import streamlit as st
import matplotlib.pyplot as plt


st.set_page_config(
    page_title="TMDB Analytics Dashboard",
    layout="wide",
)


IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w342"


def get_connection():
    """
    Create a Snowflake connection for dashboard queries.
    """
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
    )


@st.cache_data(ttl=300)
def run_query(sql: str) -> pd.DataFrame:
    """
    Run a Snowflake query and return a DataFrame.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_summary_data() -> pd.DataFrame:
    sql = """
    select
        dt,
        media_type,
        item_count,
        distinct_item_count,
        avg_popularity,
        avg_vote_average,
        total_vote_count,
        max_popularity
    from TMDB_DB.GOLD.FCT_TMDB_TRENDING_DAILY_SUMMARY
    order by dt desc, media_type
    """
    df = run_query(sql)
    if not df.empty:
        df["DT"] = pd.to_datetime(df["DT"])
    return df


@st.cache_data(ttl=300)
def load_top_items_data() -> pd.DataFrame:
    sql = """
    select
        dt,
        media_type,
        tmdb_id,
        display_title,
        popularity,
        vote_average,
        vote_count,
        poster_path,
        backdrop_path,
        popularity_rank
    from TMDB_DB.GOLD.FCT_TMDB_TRENDING_TOP_ITEMS_DAILY
    order by dt desc, media_type, popularity_rank
    """
    df = run_query(sql)
    if not df.empty:
        df["DT"] = pd.to_datetime(df["DT"])
    return df


@st.cache_data(ttl=300)
def load_latest_data() -> pd.DataFrame:
    sql = """
    select
        dt,
        tmdb_id,
        media_type,
        display_title,
        popularity,
        vote_average,
        vote_count,
        original_language,
        release_date,
        first_air_date
    from TMDB_DB.GOLD.DIM_TMDB_TRENDING_LATEST
    """
    df = run_query(sql)
    if not df.empty:
        df["DT"] = pd.to_datetime(df["DT"])
    return df


def format_metric(value, decimals: int = 0) -> str:
    """
    Format KPI values for display.
    """
    if pd.isna(value):
        return "-"
    if decimals == 0:
        return f"{value:,.0f}"
    return f"{value:,.{decimals}f}"


def get_poster_url(poster_path: str | None) -> str | None:
    """
    Build a full TMDB poster URL from a poster path.
    """
    if not poster_path or pd.isna(poster_path):
        return None
    return f"{IMAGE_BASE_URL}{poster_path}"


summary_df = load_summary_data()
top_items_df = load_top_items_data()
latest_df = load_latest_data()

st.title("TMDB Analytics Dashboard")
st.caption("Gold-layer analytics powered by Snowflake and dbt")

if summary_df.empty or top_items_df.empty or latest_df.empty:
    st.warning("No dashboard data is available yet.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

available_dates = sorted(summary_df["DT"].dt.date.unique(), reverse=True)
selected_date = st.sidebar.selectbox(
    "Select date",
    options=available_dates,
    index=0,
)

available_media_types = sorted(summary_df["MEDIA_TYPE"].dropna().unique().tolist())
selected_media_types = st.sidebar.multiselect(
    "Media type",
    options=available_media_types,
    default=available_media_types,
)

filtered_summary = summary_df[
    (summary_df["DT"].dt.date == selected_date)
    & (summary_df["MEDIA_TYPE"].isin(selected_media_types))
].copy()

filtered_top_items = top_items_df[
    (top_items_df["DT"].dt.date == selected_date)
    & (top_items_df["MEDIA_TYPE"].isin(selected_media_types))
].copy()

filtered_latest = latest_df[
    latest_df["MEDIA_TYPE"].isin(selected_media_types)
].copy()

# KPI cards
total_items = filtered_summary["ITEM_COUNT"].sum()
total_distinct_items = filtered_summary["DISTINCT_ITEM_COUNT"].sum()
avg_popularity = filtered_summary["AVG_POPULARITY"].mean()
total_vote_count = filtered_summary["TOTAL_VOTE_COUNT"].sum()

kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
kpi_col1.metric("Total Items", format_metric(total_items))
kpi_col2.metric("Distinct Items", format_metric(total_distinct_items))
kpi_col3.metric("Average Popularity", format_metric(avg_popularity, 2))
kpi_col4.metric("Total Vote Count", format_metric(total_vote_count))

st.divider()

# Charts
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Average Popularity by Media Type")
    if filtered_summary.empty:
        st.info("No popularity data for the selected filters.")
    else:
        popularity_chart_df = (
            filtered_summary[["MEDIA_TYPE", "AVG_POPULARITY"]]
            .sort_values("AVG_POPULARITY", ascending=False)
        )

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(
            popularity_chart_df["MEDIA_TYPE"],
            popularity_chart_df["AVG_POPULARITY"],
        )
        ax.set_xlabel("Media Type")
        ax.set_ylabel("Average Popularity")
        ax.set_title(f"Average Popularity on {selected_date}")
        st.pyplot(fig)

with chart_col2:
    st.subheader("Total Vote Count by Media Type")
    if filtered_summary.empty:
        st.info("No vote data for the selected filters.")
    else:
        vote_chart_df = (
            filtered_summary[["MEDIA_TYPE", "TOTAL_VOTE_COUNT"]]
            .sort_values("TOTAL_VOTE_COUNT", ascending=False)
        )

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(
            vote_chart_df["MEDIA_TYPE"],
            vote_chart_df["TOTAL_VOTE_COUNT"],
        )
        ax.set_xlabel("Media Type")
        ax.set_ylabel("Total Vote Count")
        ax.set_title(f"Vote Count on {selected_date}")
        st.pyplot(fig)

st.divider()

# Leaderboard cards
st.subheader("Top 10 Trending Leaderboard")

if filtered_top_items.empty:
    st.info("No top-item data for the selected filters.")
else:
    for media_type in selected_media_types:
        media_df = filtered_top_items[
            filtered_top_items["MEDIA_TYPE"] == media_type
        ].sort_values("POPULARITY_RANK")

        if media_df.empty:
            continue

        st.markdown(f"### {media_type.title()}")

        for _, row in media_df.iterrows():
            poster_url = get_poster_url(row["POSTER_PATH"])

            poster_col, content_col = st.columns([1, 5])

            with poster_col:
                if poster_url:
                    st.image(poster_url, width=110)
                else:
                    st.markdown("No poster")

            with content_col:
                st.markdown(
                    f"""
                    <div style="
                        border: 1px solid #333;
                        border-radius: 14px;
                        padding: 14px 18px;
                        margin-bottom: 12px;
                        background-color: rgba(255,255,255,0.03);
                    ">
                        <div style="font-size: 18px; font-weight: 700;">
                            #{int(row["POPULARITY_RANK"])} • {row["DISPLAY_TITLE"]}
                        </div>
                        <div style="margin-top: 8px; font-size: 14px;">
                            <strong>TMDB ID:</strong> {row["TMDB_ID"]} &nbsp;&nbsp; 
                            <strong>Popularity:</strong> {row["POPULARITY"]:.2f} &nbsp;&nbsp;
                            <strong>Vote Avg:</strong> {row["VOTE_AVERAGE"]:.2f} &nbsp;&nbsp;
                            <strong>Votes:</strong> {int(row["VOTE_COUNT"])}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

st.divider()

# Latest snapshot
st.subheader("Latest Trending Snapshot")

if filtered_latest.empty:
    st.info("No latest snapshot data for the selected filters.")
else:
    snapshot_df = filtered_latest[
        [
            "DT",
            "MEDIA_TYPE",
            "DISPLAY_TITLE",
            "POPULARITY",
            "VOTE_AVERAGE",
            "VOTE_COUNT",
            "ORIGINAL_LANGUAGE",
            "RELEASE_DATE",
            "FIRST_AIR_DATE",
            "TMDB_ID",
        ]
    ].sort_values(["DT", "POPULARITY"], ascending=[False, False])

    st.dataframe(snapshot_df.head(100), width="stretch")