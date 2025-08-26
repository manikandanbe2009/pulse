# 9. Insurance Transactions Analysis

import streamlit as st
import pandas as pd
import plotly.express as px
import dbconnection as dbconnection
import json
import requests

# --- DB Connection ---
@st.cache_data
def load_insurance_data():
    conn = dbconnection.create_connection()
    query = """
        SELECT 
            state, 
            year,
            quater,
            insurance_type AS user_type,
            SUM(insurance_count) AS insurance_transaction_count,
            SUM(insurance_amount) AS insurance_transaction_amount
        FROM top_insurance
        GROUP BY state, year, quater, insurance_type
        ORDER BY state ASC, year ASC, quater ASC, insurance_type ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df_transaction = load_insurance_data()
def insuranceAnalysis1():
    st.title("Insurance Transactions Analysis")
    df_transaction["insurance_transaction_count"] = df_transaction["insurance_transaction_count"].fillna(0).astype(int)
    df_transaction["insurance_transaction_amount"] = df_transaction["insurance_transaction_amount"].fillna(0).astype(float)
    df_transaction["year_quarter"] = df_transaction["year"].astype(str) + " Q" + df_transaction["quater"].astype(str)
    df_transaction["is_pincode"] = df_transaction["user_type"].str.isdigit()
    df_transaction["district"] = df_transaction.apply(lambda row: None if row["is_pincode"] else row["user_type"], axis=1)
    df_transaction["pincode"] = df_transaction.apply(lambda row: row["user_type"] if row["is_pincode"] else None, axis=1)

    # --- Filters ---
    col1, col2 = st.columns(2)
    with col1:
        select_year = st.selectbox("Select Year", sorted(df_transaction['year'].unique()))
    with col2:
        select_quarter = st.selectbox("Select Quarter", sorted(df_transaction['quater'].unique()))

    filtered_df = df_transaction[(df_transaction['year'] == select_year) & (df_transaction['quater'] == select_quarter)]

    if filtered_df.empty:
        st.warning("No insurance transaction data found for this selection.")
    else:
        # --- Top States ---
        st.subheader("Top States by Insurance Transactions")
        state_group = filtered_df.groupby("state").agg({
            "insurance_transaction_count": "sum",
            "insurance_transaction_amount": "sum"
        }).reset_index().sort_values("insurance_transaction_amount", ascending=False)
        
        # # Choropleth Map (India States)
        # st.subheader("State-wise Insurance Transactions (Map)")
        # url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        # india_states = requests.get(url).json()   

        # fig_map = px.choropleth(
        #     state_group,
        #     geojson=india_states,
        #     featureidkey="properties.ST_NM",  # adjust key if needed
        #     locations="state",
        #     color="insurance_transaction_amount",
        #     color_continuous_scale="Blues",
        #     title="Insurance Transaction Amount by State",
        #     labels={"insurance_transaction_amount": "Transaction Amount (₹)"}
        # )
        # fig_map.update_geos(fitbounds="locations", visible=False)
        # st.plotly_chart(fig_map, use_container_width=True)

        # --- Bar chart for Top 10 States ---
        fig_state = px.bar(
            state_group.head(10),
            x="state",
            y="insurance_transaction_amount",
            color="insurance_transaction_count",
            title="Top 10 States by Insurance Transaction Amount",
            labels={"insurance_transaction_amount": "Transaction Amount (₹)", "state": "State"}
        )
        st.plotly_chart(fig_state, use_container_width=True)

        # --- State Selection for Districts ---
        select_state = st.selectbox("Select a State for District Analysis", state_group['state'])

        # --- Top Districts ---
        st.subheader(f"Top Districts in {select_state}")
        district_group = filtered_df[filtered_df['state'] == select_state].groupby("district").agg({
            "insurance_transaction_count": "sum",
            "insurance_transaction_amount": "sum"
        }).reset_index().sort_values("insurance_transaction_amount", ascending=False)

        fig_district = px.bar(
            district_group.head(10),
            x="district",
            y="insurance_transaction_amount",
            color="insurance_transaction_count",
            title=f"Top 10 Districts in {select_state} by Insurance Transaction Amount",
            labels={"insurance_transaction_amount": "Transaction Amount (₹)", "district": "District"}
        )
        st.plotly_chart(fig_district, use_container_width=True)

        # --- State Selection for Pincode ---
        select_state = st.selectbox("Select a State for Pin Code Analysis", state_group['state'] )

        # --- Top Pin Codes ---
        st.subheader(f"Top Pin Codes in {select_state}")
        pincode_group = filtered_df[(filtered_df['state'] == select_state)].groupby("pincode").agg({
            "insurance_transaction_count": "sum",
            "insurance_transaction_amount": "sum"
        }).reset_index().sort_values("insurance_transaction_amount", ascending=False)

        fig_pincode = px.bar(
            pincode_group.head(10),
            x="pincode",
            y="insurance_transaction_amount",
            color="insurance_transaction_count",
            title=f"Top 10 Pin Codes in {select_state} by Insurance Transaction Amount",
            labels={"insurance_transaction_amount": "Transaction Amount (₹)", "pincode": "Pin Code"}
        )
        st.plotly_chart(fig_pincode, use_container_width=True)

        # --- Trend over Time  Transaction Amount---
        st.subheader("Quarterly Trend of Insurance Transactions")
        trend_df = df_transaction.groupby(["year", "quater"]).agg({
            "insurance_transaction_count": "sum",
            "insurance_transaction_amount": "sum"
        }).reset_index()

        fig_trend = px.line(
            trend_df,
            x=trend_df["year"].astype(str) + " Q" + trend_df["quater"].astype(str),
            y="insurance_transaction_amount",
            markers=True,
            title="Quarterly Trend of Insurance Transaction Amount",
            labels={"x": "Year-Quarter", "insurance_transaction_amount": "Transaction Amount (₹)"}
        )
        st.plotly_chart(fig_trend, use_container_width=True)

        # --- Trend over Time Transaction Count---
        st.subheader("Quarterly Trend of Insurance Transactions")
        trend_df = df_transaction.groupby(["year", "quater"]).agg({
            "insurance_transaction_count": "sum",
            "insurance_transaction_amount": "sum"
        }).reset_index()

        fig_trend_count = px.line(
            trend_df,
            x=trend_df["year"].astype(str) + " Q" + trend_df["quater"].astype(str),
            y="insurance_transaction_count",
            markers=True,
            title="Quarterly Trend of Insurance Transaction Count",
            labels={"x": "Year-Quarter", "insurance_transaction_count": "Transaction Count (₹)"}
        )
        st.plotly_chart(fig_trend_count, use_container_width=True)