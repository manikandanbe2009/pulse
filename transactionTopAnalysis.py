import streamlit as st
from streamlit_option_menu import option_menu
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import json 
import requests
from streamlit_plotly_events import plotly_events
import plotly.graph_objects as go
import plotly.express as px
import math
# import aggregatedData as aggregatedData
import dbconnection as dbconnection
import map as map
def transaction_analysis():
    def format_inr(value):
        if value >= 1e12:
            return f"{value/1e12:.2f} Trillion"
        elif value >= 1e7:
            return f"{value/1e7:.2f} Crores"
        elif value >= 1e5:
            return f"{value/1e5:.2f} Lakhs"
        else:
            return f"{value:,.0f}"

    st.title("Transactions Analysis")
    st.write("This section provides an analysis of Transactions across different states and quarters.")
    # Add your analysis code here
    def load_user_data():
        try:
            conn = dbconnection.create_connection()
            query = """
                SELECT 
                    state, 
                    year,
                    quater,
                    transaction_type AS user_type,
                    SUM(transaction_count) AS total_count,
                    SUM(transaction_amount) AS total_amount
                FROM top_transaction
                GROUP BY state, year, quater, transaction_type
                ORDER BY state ASC, year ASC, quater ASC, transaction_type ASC
            """
            df = pd.read_sql(query, conn)
            df.columns = [col.lower() for col in df.columns]
            return df
        except Exception as e:
            st.error(f"An error occurred: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()
    df_transaction = load_user_data()
    if not df_transaction.empty:  
        df_transaction["total_count"] = df_transaction["total_count"].fillna(0).astype(int)
        df_transaction["total_amount"] = df_transaction["total_amount"].fillna(0).astype(float)
        df_transaction["year_quarter"] = df_transaction["year"].astype(str) + " Q" + df_transaction["quater"].astype(str)
        df_transaction["is_pincode"] = df_transaction["user_type"].str.isdigit()
        df_transaction["district"] = df_transaction.apply(lambda row: None if row["is_pincode"] else row["user_type"], axis=1)
        df_transaction["pincode"] = df_transaction.apply(lambda row: row["user_type"] if row["is_pincode"] else None, axis=1)
        # District-level records
        district_df = df_transaction[df_transaction["district"].notna()]
        # Pincode-level records
        pincode_df = df_transaction[df_transaction["pincode"].notna()]
        district_df = district_df[["state", "year", "quater","year_quarter", "district",  "total_count","total_amount"]]
        state_df = pincode_df[["state", "year", "quater","year_quarter", "pincode",  "total_count","total_amount"]]
        top_districts = (
                district_df.groupby("district").agg(total_count = ("total_count","sum"), total_amount = ("total_amount","sum"))
                .reset_index() 
                .sort_values(["total_amount"], ascending=False)
                .head(20)
         )
        top_pincode = (
                pincode_df.groupby("pincode").agg(total_count = ("total_count","sum"), total_amount = ("total_amount","sum"))
                .sort_values(["total_count"], ascending=False)
                .reset_index() 
                .head(20)
         )   
        top_states = (
            state_df.groupby("state", as_index=False)  # keep 'state' as column
            .agg(total_count=("total_count","sum"), total_amount=("total_amount","sum"))
            .sort_values(["total_count"], ascending=False)
            .head(20)
        )
        top_districts["total_count_lakh"] = top_districts["total_count"] / 100000   
        top_pincode["total_count_lakh"] = top_pincode["total_count"] / 100000 
        top_states["total_count"] = top_states["total_count"] / 100000
        top_districts['total_amount_foramtted'] = top_districts['total_amount'].fillna(0).astype(float).apply(map.format_inr_short)
        top_pincode['total_amount_foramtted'] = top_pincode['total_amount'].fillna(0).astype(float).apply(map.format_inr_short)
        top_states['total_amount_foramtted'] = top_states['total_amount'].fillna(0).astype(float).apply(map.format_inr_short)

        state_summary = (
            df_transaction.groupby("state", as_index=False)
            .agg({"total_count": "sum", "total_amount": "sum"})
            .sort_values("total_amount", ascending=False)
        )
        district_summary = (
            df_transaction.groupby(["state", "district"], as_index=False)
            .agg({"total_count": "sum", "total_amount": "sum"})
            .sort_values("total_amount", ascending=False)
        )
        pincode_summary = (
            df_transaction.groupby(["state", "pincode"], as_index=False)
            .agg({"total_count": "sum", "total_amount": "sum"})
            .sort_values("total_amount", ascending=False)
        )

        # Add formatted values directly to THIS DataFrame
        top10_states = (
            state_summary.sort_values('total_amount', ascending=False)
                        .head(10)
                        .copy()
        )
        top10_states['Total_Count_F'] = top10_states['total_count'].apply(format_inr)
        top10_states['Total_Amount_F'] = top10_states['total_amount'].apply(format_inr)
        top10_districts = (
            district_summary.sort_values('total_amount', ascending=False)
                        .head(10)
                        .copy()
        )
        top10_districts['Total_Count_F'] = top10_districts['total_count'].apply(format_inr)
        top10_districts['Total_Amount_F'] = top10_districts['total_amount'].apply(format_inr)
        top10_pincodes = (
            pincode_summary.sort_values('total_amount', ascending=False)
                        .head(10)
                        .copy()
        )
        top10_pincodes['Total_Count_F'] = top10_pincodes['total_count'].apply(format_inr)
        top10_pincodes['Total_Amount_F'] = top10_pincodes['total_amount'].apply(format_inr)
        st.title("PhonePe  Transaction Insights")        

        st.markdown("### Top 10 States by Transaction Amount")
       
        # Use the same slice for plotting
        fig_state = px.bar(
            top10_states,
            x="state",
            y="total_amount",
            color="state",
            title="Top 10 States by Transaction Amount",
            labels={"total_amount": "Total Amount (₹)", "state": "State"},
            custom_data=["Total_Count_F", "Total_Amount_F"]
        )

        fig_state.update_traces(width=0.4)
        fig_state.update_layout(
            xaxis_title="State",
            yaxis_title="Transaction Amount (₹)",
            xaxis=dict(type="category", categoryorder="total descending")
        )

        # ✅ Now columns exist in top10_states
        fig_state.update_traces(
            hovertemplate=(
                "<b>State:</b> %{x}<br>"
                "<b>Transaction Count:</b> %{customdata[0]}<br>"
                "<b>Amount:</b> ₹%{customdata[1]}<extra></extra>"
            )
        )

        st.plotly_chart(fig_state, use_container_width=True)

        st.markdown("### Top 10 Districts by Transaction Amount")
        fig_district = px.bar(
            top10_districts,
            x="district",
            y="total_amount",
            color="district",
            title="Top 10 Districts by Transaction Amount",
            labels={"total_amount": "Total Amount (₹)", "district": "District"},
            custom_data=["Total_Count_F", "Total_Amount_F"]
        )
        fig_district.update_traces(width=0.4)
        fig_district.update_layout(
            xaxis_title="Districts",
            yaxis_title="Transaction Count",
            xaxis=dict(type="category", categoryorder="total descending")
        )
        fig_district.update_traces(
            hovertemplate="<b>Transaction Count:</b> %{customdata[0]}<br><b>Amount:</b> ₹%{customdata[1]}<br><b>District:</b> %{x}<extra></extra>",
          
        )
        st.plotly_chart(fig_district, use_container_width=True)

        st.markdown("### Top 10 Pincodes by Transaction Amount")
        fig_pincode = px.bar(
            top10_pincodes,
            x="pincode",
            y="total_amount",
            color="pincode",
            barmode='group',
            title="Top 10 Pncodes by Transaction Amount",
            labels={"total_amount": "Total Amount (₹)", "pincode": "Pncodes"},
            custom_data=["Total_Count_F", "Total_Amount_F"]
        )
        fig_pincode.update_traces(width=0.4)
        fig_pincode.update_layout(
            xaxis_title="Pncodes",
            yaxis_title="Transaction Count",
            xaxis=dict(type="category", categoryorder="total descending")
        )
        fig_pincode.update_traces(
            hovertemplate="<b>Transaction Count:</b> %{customdata[0]}<br><b>Amount:</b> ₹%{customdata[01]}<br><b>Pincode:</b> %{x}<extra></extra>",
          
        )
        st.plotly_chart(fig_pincode, use_container_width=True)


        # ================== STREAMLIT TABS ==================
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Top States", "Top Districts", "Top Pincodes", "Top Year - Quarter Combination ", "Top Combination Filer "])
        with tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total  Transactions Count (Lakhs)", f"{top_states['total_count'].sum():,.2f}")
                fig_states = px.bar(
                    top_states,
                    x='state',
                    y='total_count',
                    color='total_count',
                    barmode='group',
                    title="Top 20 State  Transactions Count",
                    labels={"total_count": "Total", "state": "State"},
                    height=600
                )
                fig_states.update_traces(width=0.4)
                fig_states.update_layout(
                    xaxis_title="State",
                    yaxis_title="Transaction Count",
                    xaxis=dict(type="category")
                )
                fig_states.update_traces(               
                    hovertemplate="<b>State:</b> %{x}<br>"
                                "<b>Count:</b> %{y:.2f} Lakhs<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_states, use_container_width=True)
            with col2:
                st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_states['total_amount'].sum())}")
                fig_states2 = px.bar(
                    top_states,
                    x='total_amount_foramtted',
                    y='state',
                    color='total_amount_foramtted',
                    barmode='group',
                    title="Top 20 State  Transactions Count",
                    labels={"total_amount_foramtted": "Total", "state": "State"},
                    height=600
                )
                fig_states2.update_traces(width=0.4)
                fig_states2.update_layout(
                    xaxis_title="State",
                    yaxis_title="Transaction Amouny",
                    xaxis=dict(type="category")
                )
                fig_states2.update_traces(               
                    hovertemplate="<b>Amount:</b> %{x} <br>"
                                "<b>Sate:</b> %{y}<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_states2, use_container_width=True)
            col3, col4 = st.columns(2)
            with col3:
                st.metric("Total  Transactions Count (Lakhs)", f"{top_states['total_count'].sum():,.2f}")
                fig_scatter1 = px.scatter(
                    top_states,
                    x='state',
                    y='total_count',
                    color='total_count',
                    title="Top 20 State  Transactions Count",
                    labels={"total_count": "Total", "state": "State"},
                    height=600
                )

                # ✅ adjust scatter marker properties instead of width
                fig_scatter1.update_traces(
                    marker=dict(size=12, line=dict(width=1, color="DarkSlateGrey")),
                    hovertemplate="<b>State:</b> %{x}<br>"
                                "<b>Count:</b> %{y:.2f} Lakhs<br>"
                )

                fig_scatter1.update_layout(
                    xaxis_title="State",
                    yaxis_title="Transaction Count",
                    xaxis=dict(type="category")
                )

                st.plotly_chart(fig_scatter1, use_container_width=True)
            with col4:
                st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_states['total_amount'].sum())}")
                fig_scatter2 = px.scatter(
                    top_states,
                    x='total_amount_foramtted',
                    y='state',
                    color='total_amount_foramtted',
                    title="Top 20 State  Transactions Count",
                    labels={"total_amount_foramtted": "Total", "state": "State"},
                    height=600
                )
                fig_scatter2.update_layout(
                    xaxis_title="State",
                    yaxis_title="Transaction Amouny",
                    xaxis=dict(type="category")
                )
                fig_scatter2.update_traces(     
                    marker=dict(size=12, line=dict(width=1, color="DarkSlateGrey")),          
                    hovertemplate="<b>Amount:</b> %{x} <br>"
                                "<b>Sate:</b> %{y}<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_scatter2, use_container_width=True)           

        with tab2:
            st.write("Top 20 Districts based on Transactions")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Transactions Count (Lakhs)", f"{top_districts['total_count'].sum():,.2f}")
                fig_states5 = px.bar(
                    top_districts,
                    x='district',
                    y='total_count',
                    color='district',
                    barmode='group',
                    title="Top 20 District Transactions Count",
                    labels={"total_count": "Total", "district": "District"},
                    height=600
                )
                fig_states5.update_traces(width=0.4)
                fig_states5.update_layout(
                    xaxis_title="District",
                    yaxis_title="Transaction Count",
                    xaxis=dict(type="category")
                )
                fig_states5.update_traces(               
                    hovertemplate="<b>District:</b> %{x}<br>"
                                "<b>Count:</b> %{y:.2f} Lakhs<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_states5, use_container_width=True)
            with col2:
                st.metric("Total Transactions Amount", f"₹ {map.format_inr_short(top_districts['total_amount'].sum())}")
                fig_states2 = px.bar(
                    top_districts,
                    x='total_amount_foramtted',
                    y='district',
                    color='total_amount_foramtted',
                    barmode='group',
                    title="Top 20 District  Transactions Count",
                    labels={"total_amount_foramtted": "Total", "district": "District"},
                    height=600
                )
                fig_states2.update_traces(width=0.4)
                fig_states2.update_layout(
                    xaxis_title="District",
                    yaxis_title="Transaction Amouny",
                    xaxis=dict(type="category")
                )
                fig_states2.update_traces(               
                    hovertemplate="<b>Amount:</b> %{x} <br>"
                                "<b>District:</b> %{y}<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_states2, use_container_width=True)
            col3, col4 = st.columns(2)
            with col3:
                st.metric("Total District Transactions Count (Lakhs)", f"{top_districts['total_count'].sum():,.2f}")
                fig_scatter1 = px.scatter(
                    top_districts,
                    x='district',
                    y='total_count',
                    color='total_count',
                    title="Top 20 State District Transactions Count",
                    labels={"total_count": "Total", "district": "District"},
                    height=600
                )

                # ✅ adjust scatter marker properties instead of width
                fig_scatter1.update_traces(
                    marker=dict(size=12, line=dict(width=1, color="DarkSlateGrey")),
                    hovertemplate="<b>District:</b> %{x}<br>"
                                "<b>Count:</b> %{y:.2f} Lakhs<br>"
                )

                fig_scatter1.update_layout(
                    xaxis_title="District",
                    yaxis_title="Transaction Count",
                    xaxis=dict(type="category")
                )

                st.plotly_chart(fig_scatter1, use_container_width=True)
            with col4:
                st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_districts['total_amount'].sum())}")
                fig_scatter2 = px.scatter(
                    top_districts,
                    x='total_amount_foramtted',
                    y='district',
                    color='total_amount_foramtted',
                    title="Top 20 District  Transactions Count",
                    labels={"total_amount_foramtted": "Total", "district": "District"},
                    height=600
                )
                fig_scatter2.update_layout(
                    xaxis_title="District",
                    yaxis_title="Transaction Amouny",
                    xaxis=dict(type="category")
                )
                fig_scatter2.update_traces(     
                    marker=dict(size=12, line=dict(width=1, color="DarkSlateGrey")),          
                    hovertemplate="<b>Amount:</b> %{x} <br>"
                                "<b>State:</b> %{y}<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_scatter2, use_container_width=True)    

        with tab3:
            st.write("Top 20 Pincode based on  Transactions")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total  Transactions Count (Lakhs)", f"{top_pincode['total_count'].sum():,.2f}")
                fig_states5 = px.bar(
                    top_pincode,
                    x='pincode',
                    y='total_count',
                    color='pincode',
                    barmode='group',
                    title="Top 20 Pincode  Transactions Count",
                    labels={"total_count": "Total", "pincode": "Pincode"},
                    height=600
                )
                fig_states5.update_traces(width=0.4)
                fig_states5.update_layout(
                    xaxis_title="Pincode",
                    yaxis_title="Transaction Count",
                    xaxis=dict(type="category")
                )
                fig_states5.update_traces(               
                    hovertemplate="<b>Pincode:</b> %{x}<br>"
                                "<b>Count:</b> %{y:.2f} Lakhs<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_states5, use_container_width=True)
            with col2:
                st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_pincode['total_amount'].sum())}")
                fig_states2 = px.bar(
                    top_pincode,
                    x='total_amount_foramtted',
                    y='pincode',
                    color='total_amount_foramtted',
                    barmode='group',
                    title="Top 20 Pincode  Transactions Count",
                    labels={"total_amount_foramtted": "Total", "pincode": "Pincode"},
                    height=600
                )
                fig_states2.update_traces(width=0.4)
                fig_states2.update_layout(
                    xaxis_title="Pincode",
                    yaxis_title="Transaction Amouny",
                    xaxis=dict(type="category")
                )
                fig_states2.update_traces(               
                    hovertemplate="<b>Amount:</b> %{x} <br>"
                                "<b>Pincode:</b> %{y}<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_states2, use_container_width=True)
            col3, col4 = st.columns(2)
            with col3:
                st.metric("Total Pincode Transactions Count (Lakhs)", f"{top_pincode['total_count'].sum():,.2f}")
                fig_scatter1 = px.scatter(
                    top_pincode,
                    x='pincode',
                    y='total_count',
                    color='total_count',
                    title="Top 20 Pincode Transactions Count",
                    labels={"total_count": "Total", "pincode": "Pincode"},
                    height=600
                )

                # ✅ adjust scatter marker properties instead of width
                fig_scatter1.update_traces(
                    marker=dict(size=12, line=dict(width=1, color="DarkSlateGrey")),
                    hovertemplate="<b>Pincode:</b> %{x}<br>"
                                "<b>Count:</b> %{y:.2f} Lakhs<br>"
                )

                fig_scatter1.update_layout(
                    xaxis_title="Pincode",
                    yaxis_title="Transaction Count",
                    xaxis=dict(type="category")
                )

                st.plotly_chart(fig_scatter1, use_container_width=True)
            with col4:
                st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_pincode['total_amount'].sum())}")
                fig_scatter2 = px.scatter(
                    top_pincode,
                    x='total_amount_foramtted',
                    y='pincode',
                    color='total_amount_foramtted',
                    title="Top 20 Pincode  Transactions Count",
                    labels={"total_amount_foramtted": "Total", "pincode": "Pincode"},
                    height=600
                )
                fig_scatter2.update_layout(
                    xaxis_title="Pincode",
                    yaxis_title="Transaction Amouny",
                    xaxis=dict(type="category")
                )
                fig_scatter2.update_traces(     
                    marker=dict(size=12, line=dict(width=1, color="DarkSlateGrey")),          
                    hovertemplate="<b>Amount:</b> %{x} <br>"
                                "<b>Pincode:</b> %{y}<br>"
                )

                # Streamlit display
                st.plotly_chart(fig_scatter2, use_container_width=True)
        with tab4:
            top_year_quarter = (
                district_df.groupby("year_quarter", as_index=False)[["total_count", "total_amount"]]
                .sum()
                .sort_values("total_amount", ascending=False)
                .head(20)
            )
            top_year_quarter["total_amount_formatted"] = top_year_quarter["total_amount"].apply(format_inr)
            top_year_quarter["total_count_formatted"] = top_year_quarter["total_count"].apply(format_inr)  
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total  Transactions Count (Lakhs)", f"{top_year_quarter['total_count'].sum():,.2f}")
                fig_tab4_count = px.bar(
                    top_year_quarter,
                    x="year_quarter",
                    y="total_count",
                    color="year_quarter",
                    title="Top 20 Year Quarter by  Transaction Count",
                    labels={"total_count": "Total Count", "year_quarter": "Year Quarter"},
                    height=600
                )

                fig_tab4_count.update_traces(
                    customdata=top_year_quarter[["total_count_formatted"]],
                    hovertemplate="<b>Year Quarter:</b> %{x}<br>Total Count: %{customdata[0]}<extra></extra>"
                )

                st.plotly_chart(fig_tab4_count, use_container_width=True)
            with col2:
                st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(district_df['total_amount'].sum())}")
                fig_tab4_2 = px.bar(
                    top_year_quarter,
                    x="year_quarter",
                    y="total_amount",
                    color="year_quarter",
                    title="Top 20 Year Quarter by   Transactions Amount",
                    labels={"total_amount": "Total Amount", "year_quarter": "Year Quarter"},
                    height=600
                )

                # Step 4: Update layout
                fig_tab4_2.update_layout(
                    xaxis_title="Year Quarter",
                    yaxis_title="Total Amount (₹)",
                    xaxis=dict(type="category")
                )

                # Step 5: Add hover with formatted values
                fig_tab4_2.update_traces(
                    customdata=top_year_quarter[["total_amount_formatted"]],
                    hovertemplate="<b>Year Quarter:</b> %{x}<br>Total Amount: ₹ %{customdata[0]}<extra></extra>"
                )

                # Streamlit display
                st.plotly_chart(fig_tab4_2, use_container_width=True)

        with tab5:
            # ================== DROPDOWNS ==================
            year_filter = st.selectbox("Select Year", sorted(df_transaction["year"].unique()))
            quarter_filter = st.selectbox("Select Quarter", sorted(df_transaction["quater"].unique()))
            # ✅ Filter data
            filtered = df_transaction[
                (df_transaction["year"] == year_filter) & 
                (df_transaction["quater"] == quarter_filter)
            ]

            # ================== GROUP DATA ==================
            top_states = (
                filtered.groupby("state", as_index=False)[["total_count", "total_amount"]]
                .sum()
                .sort_values(by="total_count", ascending=False)
                .head(10)
            )

            top_districts = (
                filtered.groupby("district", as_index=False)[["total_count", "total_amount"]]
                .sum()
                .sort_values(by="total_count", ascending=False)
                .head(10)
            )

            top_pincodes = (
                filtered.groupby("pincode", as_index=False)[["total_count", "total_amount"]]
                .sum()
                .sort_values(by="total_count", ascending=False)
                .head(10)
            )

            # ✅ Create formatted columns for display
            top_states["total_count_lakh"] = top_states["total_count"] / 1e5   # if values are in lakhs
            top_states["total_amount_formatted"] = top_states["total_amount"].apply(format_inr)

            top_districts["total_count_lakh"] = top_districts["total_count"] / 1e5
            top_districts["total_amount_formatted"] = top_districts["total_amount"].apply(format_inr)

            top_pincodes["total_count_lakh"] = top_pincodes["total_count"] / 1e5
            top_pincodes["total_amount_formatted"] = top_pincodes["total_amount"].apply(format_inr)

            # ================== STREAMLIT TABS ==================
            tab1, tab2, tab3 = st.tabs(["🏙️ Top States", "🌆 Top Districts", "📮 Top Pincodes"])

            # -------------------- STATES --------------------
            with tab1:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total  Transactions Count (Lakhs)", f"{top_states['total_count_lakh'].sum():,.2f}")
                    fig_states = px.bar(
                        top_states,
                        x="state",
                        y="total_count_lakh",
                        color="state",
                        title=f"Top 10 States ({year_filter} Q{quarter_filter})",
                        labels={"total_count_lakh": "Transactions (Lakhs)", "state": "State"},
                        height=500
                    )
                    fig_states.update_traces(hovertemplate="State: %{x}<br>Count: %{y:.2f} Lakhs")
                    st.plotly_chart(fig_states, use_container_width=True)

                with col2:
                    st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_states['total_amount'].sum())}")
                    fig_states2 = px.bar(
                        top_states,
                        x="state",
                        y="total_amount_formatted",
                        color="state",
                        title=f"Top 10 States ({year_filter} Q{quarter_filter})",
                        labels={"total_amount_formatted": "Amount (₹)", "state": "State"},
                        height=500
                    )
                    fig_states2.update_traces(
                        customdata=top_states[["total_amount_formatted"]],
                        hovertemplate="<b>State:</b> %{x}<br>Total Amount: ₹ %{customdata[0]}<extra></extra>"
                    )
                    st.plotly_chart(fig_states2, use_container_width=True)

            # -------------------- DISTRICTS --------------------
            with tab2:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total  Transactions Count (Lakhs)", f"{top_districts['total_count_lakh'].sum():,.2f}")
                    fig_districts = px.bar(
                        top_districts,
                        x="district",
                        y="total_count_lakh",
                        color="district",
                        title=f"Top 10 Districts ({year_filter} Q{quarter_filter})",
                        labels={"total_count_lakh": "Transactions (Lakhs)", "district": "District"},
                        height=600
                    )
                    fig_districts.update_traces(hovertemplate="District: %{x}<br>Count: %{y:.2f} Lakhs")

                    st.plotly_chart(fig_districts, use_container_width=True)

                with col2:
                    st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_districts['total_amount'].sum())}")
                    fig_districts2 = px.bar(
                        top_districts,
                        x="district",
                        y="total_amount_formatted",
                        color="district",
                        title=f"Top 10 Districts ({year_filter} Q{quarter_filter})",
                        labels={"total_amount_formatted": "Amount (₹)", "district": "District"},
                        height=600
                    )
                    # fig_districts2.update_traces(hovertemplate="District: %{x}<br>Amount: ₹ %{y:,.0f}")
                    fig_districts2.update_traces(
                        customdata=top_districts[["total_amount_formatted"]],
                        hovertemplate="<b>State:</b> %{x}<br>Total Amount: ₹ %{customdata[0]}<extra></extra>"
                    )
                    st.plotly_chart(fig_districts2, use_container_width=True)

            # -------------------- PINCODES --------------------
            with tab3:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total  Transactions Count (Lakhs)", f"{top_pincodes['total_count_lakh'].sum():,.2f}")
                    fig_pincodes = px.bar(
                        top_pincodes,
                        x="pincode",
                        y="total_count_lakh",
                        color="pincode",
                        title=f"Top 10 Pincodes ({year_filter} Q{quarter_filter})",
                        labels={"total_count_lakh": "Transactions (Lakhs)", "pincode": "Pincode"},
                        height=600
                    )
                    fig_pincodes.update_traces(hovertemplate="Pincode: %{x}<br>Count: %{y:.2f} Lakhs")
                    st.plotly_chart(fig_pincodes, use_container_width=True)

                with col2:
                    st.metric("Total  Transactions Amount", f"₹ {map.format_inr_short(top_pincodes['total_amount'].sum())}")
                    fig_pincodes2 = px.bar(
                        top_pincodes,
                        x="pincode",
                        y="total_amount_formatted",
                        color="pincode",
                        title=f"Top 10 Pincodes ({year_filter} Q{quarter_filter})",
                        labels={"total_amount_formatted": "Amount (₹)", "pincode": "Pincode"},
                        height=600
                    )
                    # fig_pincodes2.update_traces(hovertemplate="Pincode: %{x}<br>Amount: ₹ %{y:,.0f}")
                    fig_districts2.update_traces(
                        customdata=top_pincodes[["total_amount_formatted"]],
                        hovertemplate="<b>State:</b> %{x}<br>Total Amount: ₹ %{customdata[0]}<extra></extra>"
                    )
                    
                    st.plotly_chart(fig_pincodes2, use_container_width=True)
    else:
        st.info("No user registration data available for analysis.")