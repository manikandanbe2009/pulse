import streamlit as st
import pandas as pd
import plotly.express as px
import dbconnection as dbconnection

# --- DB Connection (adjust credentials) ---
@st.cache_data
def load_data():
    conn = dbconnection.create_connection()
    query = """
        SELECT state, year, quarter, brand, registered_users, app_opens
        FROM aggregated_user;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()
#  Formating Numbers
def format_inr(value):
    if value >= 1e12:
        return f"{value/1e12:.2f} Trillion"
    elif value >= 1e7:
        return f"{value/1e7:.2f} Crores"
    elif value >= 1e5:
        return f"{value/1e5:.2f} Lakhs"
    else:
        return f"{value:,.0f}"

def userDeviceAnalysis() :
    st.title("Device Dominance and User Engagement Analysis")
    df["year_quarter"] = df["year"].astype(str) + "Q" + df["quarter"].astype(str)
    df["engagement_ratio"] = df["app_opens"] / df["registered_users"].replace(0, None)

    # # --- Create filters ---
    # col1, col2, col3 = st.columns(3)
    # with col1:
    #     select_year = st.selectbox("Select Year", sorted(df['year'].dropna().unique()))
    # with col2:
    #     select_quarter = st.selectbox("Quarter", sorted(df["quarter"].dropna().unique()))
    # with col3:
    #     select_state = st.selectbox("States", sorted(df['state'].dropna().unique()))

    # # --- Apply filters ---
    # df_filtered = df.copy()

    # if select_year:
    #     df_filtered = df_filtered[df_filtered["year"] == select_year]

    # if select_quarter:
    #     df_filtered = df_filtered[df_filtered["quarter"] == select_quarter]

    # if select_state:
    #     df_filtered = df_filtered[df_filtered["state"] == select_state]

    # st.write(df_filtered)

    # multi Select
    col1, col2, col3 = st.columns(3)
    with col1:
        years = st.multiselect("Select Year(s)", sorted(df["year"].dropna().unique()), default=sorted(df["year"].dropna().unique()))
    with col2:
        quarters = st.multiselect("Select Quarter(s)", sorted(df["quarter"].dropna().unique()), default=sorted(df["quarter"].dropna().unique()))
    with col3:
        states = st.multiselect("Select State(s)", sorted(df["state"].dropna().unique()))
    df_filtered = df[
        df["year"].isin(years) &
        df["quarter"].isin(quarters) 
    ]
    if states:  # apply only if selected
        df_filtered = df_filtered[df_filtered["state"].isin(states)]

    # -----------------------------
    # 1. Stacked Bar → Users & Opens by Brand
    # -----------------------------
    fig_bar = px.bar(
        df_filtered.groupby("brand")[["registered_users","app_opens"]].sum().reset_index().melt(
            id_vars="brand", value_vars=["registered_users","app_opens"],
            var_name="Metric", value_name="Count"
        ),
        x="brand", y="Count", color="Metric",
        title="Registered Users vs App Opens by Device Brand",
        barmode="stack"
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Insight block
    st.subheader("Insights: Device Dominance")
    top_brands = df_filtered.groupby("brand")["registered_users"].sum().nlargest(3).index.tolist()
    st.write(f"- **Top 3 brands by registered users**: {', '.join(top_brands)}")
    top_open = df_filtered.groupby("brand")["app_opens"].sum().idxmax()
    st.write(f"- **Highest app opens** observed on: **{top_open}** devices")

    # -----------------------------
    # 2. Heatmap → Engagement Ratio by Brand & state
    # -----------------------------
    heat_data = df_filtered.groupby(["brand","state"])["engagement_ratio"].mean().reset_index()

    fig_heat = px.imshow(
        heat_data.pivot(index="brand", columns="state", values="engagement_ratio"),
        text_auto=True,
        title="Engagement Ratio (App Opens ÷ Registered Users)"
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # Insight block
    st.subheader("Insights: Engagement Gaps")
    high_eng = df_filtered.groupby("brand")["engagement_ratio"].mean().idxmax()
    low_eng = df_filtered.groupby("brand")["engagement_ratio"].mean().idxmin()
    st.write(f"- **Highest engagement ratio**: {high_eng}")
    st.write(f"- **Lowest engagement ratio**: {low_eng} → indicates underutilization")

    # -----------------------------
    # 3. Time-Series Line → Device-wise App Opens Growth
    # -----------------------------
    time_data = df_filtered.groupby(["year_quarter","brand"])["app_opens"].sum().reset_index()

    fig_line = px.line(
        time_data, x="year_quarter", y="app_opens",
        color="brand", markers=True,
        title="Device-wise Growth of App Opens Across Quarters"
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # Insight block
    st.subheader("Insights: Growth Trends")
    latest_q = time_data["year_quarter"].max()
    latest_data = time_data[time_data["year_quarter"] == latest_q]
    fastest_growth = latest_data.sort_values("app_opens", ascending=False).head(1)["brand"].values[0]
    st.write(f"- In the latest quarter (**{latest_q}**), **{fastest_growth}** recorded the most app opens.")

    # -----------------------------
    # 4. Treemap → Market Share by Registered Users
    # -----------------------------
    fig_treemap = px.treemap(
        df_filtered.groupby("brand")["registered_users"].sum().reset_index(),
        path=["brand"], values="registered_users",
        title="Market Share of Devices by Registered Users"
    )
    st.plotly_chart(fig_treemap, use_container_width=True)

    # Insight block
    st.subheader("Insights: Market Share")
    market_share = df_filtered.groupby("brand")["registered_users"].sum().reset_index()
    market_share["percentage"] = market_share["registered_users"] / market_share["registered_users"].sum() * 100
    top_share = market_share.sort_values("percentage", ascending=False).head(1)
    st.write(f"- **{top_share.iloc[0]['brand']}** holds the largest market share with **{top_share.iloc[0]['percentage']:.2f}%** of registered users.")
