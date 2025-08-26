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

def user_registration_analysis():
    st.title("User Registration Analysis")
    st.write("This section provides an analysis of user registrations across different states and quarters.")
    # Add your analysis code here
    def load_user_data():
        try:
            conn = dbconnection.create_connection()
            query = """
                SELECT 
                    state, 
                    year,
                    quater,
                    user_type,
                    SUM(registeredusers_count) AS total_users
                FROM top_user
                GROUP BY state, year, quater, user_type
                ORDER BY state ASC, year ASC, quater ASC, user_type ASC
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
    df_user = load_user_data()
    if not df_user.empty:  
        df_user["total_users"] = df_user["total_users"].fillna(0).astype(int)
        df_user["year_quarter"] = df_user["year"].astype(str) + " Q" + df_user["quater"].astype(str)
        df_user["is_pincode"] = df_user["user_type"].str.isdigit()
        df_user["district"] = df_user.apply(lambda row: None if row["is_pincode"] else row["user_type"], axis=1)
        df_user["pincode"] = df_user.apply(lambda row: row["user_type"] if row["is_pincode"] else None, axis=1)
        # District-level records
        district_df = df_user[df_user["district"].notna()]
        # Pincode-level records
        pincode_df = df_user[df_user["pincode"].notna()]
        district_df = district_df[["state", "year", "quater","year_quarter", "district",  "total_users"]]
        pincode_df = pincode_df[["state", "year", "quater","year_quarter", "pincode",  "total_users"]]

        top_districts = (
                district_df.groupby("district")["total_users"]
                .sum()
                .reset_index()
                .sort_values(["total_users"], ascending=False)
                .head(20)
         )
        top_pincode = (
                pincode_df.groupby("pincode")["total_users"]
                .sum()
                .reset_index()
                .sort_values(["total_users"], ascending=False)
                .head(20)
         )   
        top_states = (
                district_df.groupby("state")["total_users"]
                .sum()
                .reset_index()
                .sort_values(["total_users"], ascending=False)
                .head(20)
            )
        top_districts["total_users_lakh"] = top_districts["total_users"] / 100000   
        top_pincode["total_users_lakh"] = top_pincode["total_users"] / 100000 
        top_states["total_users"] = top_states["total_users"] / 100000
        
       
        # ================== STREAMLIT TABS ==================
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["🏙️ Top States", "🌆 Top Districts", "📮 Top Pincodes", " Top Year - Quarter Combination ", "Top Combination Filer "])

        with tab1:
            fig_states = px.bar(
                top_states, 
                x="state", 
                y="total_users",
                color="state",
                title="Total Registered Users by State",
                labels={"total_users": "Total Registered Users (Lakhs)", "state": "State"},
                height=600
            )  
            # Format axis labels
            fig_states.update_layout(   
                xaxis_title="State",
                yaxis_title="Total Registered Users (Lakhs)",
                xaxis=dict(type="category")   # 🔹 ensures states appear as labels, not as numbers
            )
            # Hover tooltip in lakhs
            fig_states.update_traces(
                hovertemplate="<b>State:</b> %{x}<br>Total Users: %{y:.2f} Lakhs<extra></extra>"
            )
            st.plotly_chart(fig_states, use_container_width=True)

            # scater plot
            fig_scatter = px.scatter(
                top_states, 
                x="state", 
                y="total_users",
                color="state",
                title="Total Registered Users by State",
                labels={"total_users": "Total Registered Users (Lakhs)", "state": "State"},
                height=600
            )  
            # Format axis labels
            fig_scatter.update_layout(   
                xaxis_title="State",
                yaxis_title="Total Registered Users (Lakhs)",
                xaxis=dict(type="category")   # 🔹 ensures states appear as labels, not as numbers
            )
            # Hover tooltip in lakhs
            fig_scatter.update_traces(
                hovertemplate="<b>State:</b> %{x}<br>Total Users: %{y:.2f} Lakhs<extra></extra>"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

            #line plot
            fig_line = px.line( 
                top_states, 
                x="state",  
                y="total_users",
                color="state",
                title="Total Registered Users by State",
                labels={"total_users": "Total Registered Users (Lakhs)", "state": "State"},
                height=600
            )  
            # Format axis labels    
            fig_line.update_layout(   
                xaxis_title="State",
                yaxis_title="Total Registered Users (Lakhs)",
                xaxis=dict(type="category")   # 🔹 ensures states appear as labels, not as number
            )
            # Hover tooltip in lakhs    
            fig_line.update_traces(
                hovertemplate="<b>State:</b> %{x}<br>Total Users: %{y:.2f} Lakhs<extra></extra>"
            )
            st.plotly_chart(fig_line, use_container_width=True)

            #seaborn bar plot
            # Set style
            sns.set_theme(style="whitegrid")

            # Create bar plot
            plt.figure(figsize=(14, 7))          

            ax = sns.barplot(
                data=top_states,
                x="state",
                y="total_users",
                palette="viridis"
            )
            # Rotate x labels for readability
            plt.xticks(rotation=90)
            # Labels and title
            plt.xlabel("State")
            plt.ylabel("Total Registered Users (Lakhs)")
            plt.title("Total Registered Users by State")
            # Show chart in Streamlit
            st.pyplot(plt)
                
            

        with tab2:
            fig_tab2 = px.bar(
                top_districts,
                x="district",           # 🔹 X-axis = Pincode
                y="total_users_lakh",   # 🔹 Y-axis = Users in Lakhs
                color="district",
                title="Top 20 Pincodes for Registered Users",
                labels={"total_users_lakh": "Total Registered Users (Lakhs)", "district": "District"},
                height=600
            )
            # Format axis labels
            fig_tab2.update_layout(
                xaxis_title="District",
                yaxis_title="Total Registered Users (Lakhs)",
                xaxis=dict(type="category")   # 🔹 ensures pincodes appear as labels, not as numbers
            )
            # Hover tooltip in lakhs
            fig_tab2.update_traces(
                hovertemplate="<b>District:</b> %{x}<br>Total Users: %{y:.2f} Lakhs<extra></extra>"
            )
            st.plotly_chart(fig_tab2, use_container_width=True)

        with tab3:
            fig_tab3 = px.bar(
                top_pincode,
                x="pincode",           # 🔹 X-axis = Pincode
                y="total_users_lakh",   # 🔹 Y-axis = Users in Lakhs
                color="pincode",
                title="Top 20 Pincodes for Registered Users",
                labels={"total_users_lakh": "Total Registered Users (Lakhs)", "pincode": "Pincode"},
                height=600
            )
            # Format axis labels
            fig_tab3.update_layout(
                xaxis_title="Pincode",
                yaxis_title="Total Registered Users (Lakhs)",
                xaxis=dict(type="category")   # 🔹 ensures pincodes appear as labels, not as numbers
            )
            # Hover tooltip in lakhs
            fig_tab3.update_traces(
                hovertemplate="<b>Pincode:</b> %{x}<br>Total Users: %{y:.2f} Lakhs<extra></extra>"
            )
            st.plotly_chart(fig_tab3, use_container_width=True)
        with tab4:
            top_year_quarter = (
                district_df.groupby("year_quarter")["total_users"]
                .sum()
                .reset_index()
                .sort_values(["total_users"], ascending=False)
                .head(20)
            )
            top_year_quarter["total_users_lakh"] = top_year_quarter["total_users"] / 100000   
            fig_tab4 = px.bar(
                top_year_quarter,
                x="year_quarter",           # 🔹 X-axis = Pincode
                y="total_users_lakh",   # 🔹 Y-axis = Users in Lakhs
                color="year_quarter",
                title="Top 20 Year Quarter Combination for Registered Users",
                labels={"total_users_lakh": "Total Registered Users (Lakhs)", "year_quarter": "Year Quarter"},
                height=600
            )
            # Format axis labels
            fig_tab4.update_layout(
                xaxis_title="Year Quarter",
                yaxis_title="Total Registered Users (Lakhs)",
                xaxis=dict(type="category")   # 🔹 ensures pincodes appear as labels, not as numbers
            )
            # Hover tooltip in lakhs
            fig_tab4.update_traces(
                hovertemplate="<b>Year Quarter:</b> %{x}<br>Total Users: %{y:.2f} Lakhs<extra></extra>"
            )
            st.plotly_chart(fig_tab4, use_container_width=True)
        with tab5:
            # ================== DROPDOWNS ==================
            year_filter = st.selectbox("Select Year", sorted(df_user["year"].unique()))
            quarter_filter = st.selectbox("Select Quarter", sorted(df_user["quater"].unique()))

            # ✅ Filter data
            filtered = df_user[(df_user["year"] == year_filter) & (df_user["quater"] == quarter_filter)]

            # ================== GROUP DATA ==================
            top_states = (
                filtered.groupby("state", as_index=False)["total_users"]
                .sum()
                .sort_values(by="total_users", ascending=False)
                .head(10)
            )
            top_states["total_users_lakh"] = top_states["total_users"] / 100000

            top_districts = (
                filtered.groupby("district", as_index=False)["total_users"]
                .sum()
                .sort_values(by="total_users", ascending=False)
                .head(10)
            )
            top_districts["total_users_lakh"] = top_districts["total_users"] / 100000

            top_pincodes = (
                filtered.groupby("pincode", as_index=False)["total_users"]
                .sum()
                .sort_values(by="total_users", ascending=False)
                .head(10)
            )
            top_pincodes["total_users_lakh"] = top_pincodes["total_users"] / 100000

            # ================== STREAMLIT TABS ==================
            tab1, tab2, tab3 = st.tabs(["🏙️ Top States", "🌆 Top Districts", "📮 Top Pincodes"])

            with tab1:
                fig_states = px.bar(
                    top_states,
                    x="state",
                    y="total_users",
                    color="state",
                    title=f"Top 10 States by Registered Users ({year_filter} Q{quarter_filter})",
                    labels={"total_users": "Total Registered Users", "state": "State"},
                    height=500
                )
                fig_states.update_traces(hovertemplate="State: %{y}<br>Users: %{x:,}")
                st.plotly_chart(fig_states, use_container_width=True)

            with tab2:
                fig_districts = px.bar(
                    top_districts,
                    x="district",
                    y="total_users",
                    color="district",
                    title=f"Top 10 Districts by Registered Users ({year_filter} Q{quarter_filter})",
                    labels={"total_users": "Total Registered Users", "district": "District"},
                    height=500
                )
                fig_districts.update_traces(hovertemplate="District: %{y}<br>Users: %{x:,}")
                st.plotly_chart(fig_districts, use_container_width=True)

            with tab3:
                fig_pincodes = px.bar(
                    top_pincode,
                    x="pincode",           # 🔹 X-axis = Pincode
                    y="total_users_lakh",   # 🔹 Y-axis = Users in Lakhs
                    color="pincode",
                    title=f"Top 10 Pincodes by Registered Users ({year_filter} Q{quarter_filter})",
                    labels={"total_users_lakh": "Total Registered Users (Lakhs)", "pincode": "Pincode"},
                    height=600
                )
                # Format axis labels
                fig_pincodes.update_layout(
                    xaxis_title="Pincode",
                    yaxis_title="Total Registered Users (Lakhs)",
                    xaxis=dict(type="category")   # 🔹 ensures pincodes appear as labels, not as numbers
                )
                # Hover tooltip in lakhs
                fig_pincodes.update_traces(hovertemplate="Pincode: %{y}<br>Users: %{x:,}")
                st.plotly_chart(fig_pincodes, use_container_width=True)

                        
                
                


            
    else:
        st.info("No user registration data available for analysis.")