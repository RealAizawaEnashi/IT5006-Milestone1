import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Chicago Crime Dashboard", layout="wide")


@st.cache_data
def load_agg():
    """
    Loads lightweight, pre-aggregated data produced by prepare_agg.py:
      - monthly_total.parquet: month, count
      - monthly_type.parquet: month, primary_type, count
      - sample_points.parquet: date, primary_type, latitude, longitude, year
    """
    monthly_total = pd.read_parquet("data/agg/monthly_total.parquet")
    monthly_type = pd.read_parquet("data/agg/monthly_type.parquet")
    sample_points = pd.read_parquet("data/agg/sample_points.parquet")

    monthly_total["month"] = pd.to_datetime(monthly_total["month"])
    monthly_type["month"] = pd.to_datetime(monthly_type["month"])
    sample_points["date"] = pd.to_datetime(sample_points["date"], errors="coerce")

    # basic sanity
    sample_points = sample_points.dropna(subset=["date", "latitude", "longitude", "primary_type"])

    return monthly_total, monthly_type, sample_points


monthly_total, monthly_type, sample_points = load_agg()

# ---- Sidebar filters ----
st.sidebar.header("Filters")

min_date = sample_points["date"].min().date()
max_date = sample_points["date"].max().date()

date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

types = sorted(monthly_type["primary_type"].dropna().unique().tolist())
selected_types = st.sidebar.multiselect(
    "Crime type (affects map + trend)",
    options=types,
    default=types[:3] if len(types) >= 3 else types
)

# Apply filters
if isinstance(date_range, tuple) and len(date_range) == 2:
    start, end = date_range
else:
    start, end = min_date, max_date

# --- Map data: sampled points (fast) ---
mask_map = (sample_points["date"].dt.date >= start) & (sample_points["date"].dt.date <= end)
if selected_types:
    mask_map &= sample_points["primary_type"].isin(selected_types)
map_df = sample_points.loc[mask_map].copy()

# --- Trend data: aggregated by month (fast) ---
# if types selected, build monthly series from monthly_type; otherwise use monthly_total
month_start = pd.to_datetime(pd.Timestamp(start).to_period("M").start_time)
month_end = pd.to_datetime(pd.Timestamp(end).to_period("M").start_time)

if selected_types:
    mtt = monthly_type[(monthly_type["month"] >= month_start) & (monthly_type["month"] <= month_end)]
    mtt = mtt[mtt["primary_type"].isin(selected_types)]
    trend_df = mtt.groupby("month", as_index=False)["count"].sum()
else:
    mt = monthly_total[(monthly_total["month"] >= month_start) & (monthly_total["month"] <= month_end)]
    trend_df = mt.copy()

# --- Top types (date-filtered, NOT constrained by selected_types by default) ---
mtt_all = monthly_type[(monthly_type["month"] >= month_start) & (monthly_type["month"] <= month_end)]
top_types = (mtt_all.groupby("primary_type", as_index=False)["count"].sum()
             .sort_values("count", ascending=False)
             .head(10))

# --- Summary counts ---
total_in_range = int(monthly_total[(monthly_total["month"] >= month_start) & (monthly_total["month"] <= month_end)]["count"].sum())
total_selected_types = int(mtt_all[mtt_all["primary_type"].isin(selected_types)].groupby("primary_type")["count"].sum().sum()) if selected_types else total_in_range

# ---- Page ----
st.title("Chicago Crimes (2015–2024) — Interactive EDA Dashboard (Pre-aggregated)")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total crimes (date range)", f"{total_in_range:,}")
c2.metric("Crimes in selected types", f"{total_selected_types:,}")
c3.metric("Sampled points for map", f"{len(map_df):,}")
c4.metric("Date range", f"{start} → {end}")

# ---- Layout: Map + Charts ----
left, right = st.columns([1.2, 1])

with left:
    st.subheader("Spatial distribution (map)")
    if map_df.empty:
        st.warning("No data for current filters. Try expanding the date range or selecting different crime types.")
    else:
        # Keep map responsive even if sample_points is large
        plot_df = map_df
        if len(plot_df) > 200000:
            plot_df = plot_df.sample(200000, random_state=42)

        fig_map = px.density_mapbox(
            plot_df,
            lat="latitude",
            lon="longitude",
            radius=6,
            zoom=9,
            mapbox_style="carto-positron",
            height=520,
            title="Crime density (sampled points; filters applied)"
        )
        st.plotly_chart(fig_map, use_container_width=True)

with right:
    st.subheader("Temporal trend (monthly)")
    if trend_df.empty:
        st.warning("No trend data for current filters.")
    else:
        fig_line = px.line(trend_df, x="month", y="count", title="Monthly crime counts (fast, aggregated)")
        st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("Top crime types (date-filtered)")
    if top_types.empty:
        st.warning("No type data for current date range.")
    else:
        fig_bar = px.bar(top_types, x="primary_type", y="count", title="Top 10 crime types (date-filtered)")
        st.plotly_chart(fig_bar, use_container_width=True)

st.caption("Team21 LIN,YIHAN / XU,QIAOYANG / QI,RUIXUAN")
