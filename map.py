
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
def format_in_indian_commas(n: float, decimals: int = 2) -> str:
    sign = "-" if n < 0 else ""
    n = abs(float(n))
    whole, frac = divmod(n, 1)
    s = f"{int(whole)}"
    # First group (last 3 digits)
    last3 = s[-3:]
    rest = s[:-3]
    if rest:
        # Put commas every 2 digits in the rest
        rest = ",".join([rest[max(i-2,0):i] for i in range(len(rest), 0, -2)][::-1])
        s = rest + "," + last3
    else:
        s = last3
    if decimals > 0:
        return f"{sign}{s}.{str(round(frac, decimals))[2:].ljust(decimals,'0')}"
    return f"{sign}{s}"

def format_inr_short(n: float) -> str:    
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "₹0.00"
    n = float(n)
    absn = abs(n)
    if absn >= 1e7:   # 1 crore
        return f"₹{absn/1e7:,.2f} Cr" if n >= 0 else f"-₹{absn/1e7:,.2f} Cr"
    if absn >= 1e5:   # 1 lakh
        return f"₹{absn/1e5:,.2f} Lakh" if n >= 0 else f"-₹{absn/1e5:,.2f} Lakh"
    return f"₹{format_in_indian_commas(n, 2)}"

def homepage():
    # ✅ All lists same length (4 elements each)
    dataList = ["Trasaction", "Insurance", "User"]
    quarters = [1, 2, 3, 4]
    col1, col2, col3 = st.columns(3)

    with col1:
        selected_data = st.selectbox("Select Data", dataList)
    with col2:
        if dataList == "Insurance":        
            years = [2020,2021, 2022, 2023, 2024]
            selected_year = st.selectbox("Select Year", years)
        else:
            years = [2018, 2019, 2020,2021, 2022, 2023,2024]
            selected_year = st.selectbox("Select Year", years)
        
    with col3:
        selected_quarter = st.selectbox("Select Quarter", quarters)

    # Function to fetch data from the database based on user selection
    def get_data():
        try:
            conn = dbconnection.create_connection()
            cursor = conn.cursor()
            # Query to fetch data based on user selection
            if selected_data == "User":
                query = f"""
                    SELECT 
                        state, 
                        year,
                        Quarter,
                        SUM(App_Opens) AS Total_App_Opens,
                        SUM(Brand_Count) AS Total_Count,
                        SUM(Registered_Users) AS Total_Users,
                        ROW_NUMBER() OVER (PARTITION BY state,year ORDER BY Quarter ASC) AS row_num
                    FROM Aggregated_user
                    WHERE CAST(year AS INT) = %s AND CAST(Quarter AS INT) = %s
                    GROUP BY state, year, Quarter
                    ORDER BY state ASC, Quarter ASC
                """
            elif selected_data == "Insurance":
                query = f"""
                    SELECT 
                        state, 
                        year,
                        quater,
                        SUM(insurance_count) AS Total_Count,
                        SUM(insurance_amount) AS Total_amount,
                        ROW_NUMBER() OVER (PARTITION BY state, year ORDER BY quater ASC) AS row_num
                    FROM Aggregated_insurance
                    WHERE CAST(year AS INT) = %s AND CAST(quater AS INT) = %s
                    GROUP BY state, year, quater
                    ORDER BY state ASC, quater ASC
                """
            elif selected_data == "Trasaction":
                query = f"""
                    SELECT 
                        state, 
                        year,
                        quater,
                        SUM(transaction_count) AS Total_Count,
                        SUM(transaction_amount) AS Total_amount,
                        ROW_NUMBER() OVER (PARTITION BY state,year ORDER BY quater ASC) AS row_num
                    FROM Aggregated_transaction
                    WHERE CAST(year AS INT) = %s AND CAST(quater AS INT) = %s 
                    GROUP BY state, year, quater
                    ORDER BY state ASC, quater ASC
                """
            cursor.execute(query, (selected_year, selected_quarter))
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=columns)
            df.columns = [col.lower() for col in df.columns]
            if df.empty:
                st.warning("No data found for the selected criteria.")
                return pd.DataFrame()
            return df
        except Exception as e:
            st.error(f"An error occurred: {e}")
        finally:
            if conn:
                cursor.close()
                conn.close()

    df1 = get_data()

    # st.write(f"### {selected_data} Data for Year {selected_year} and Quarter {selected_quarter}")

    text = None
    zdata = None

    if not df1.empty:
        # Standardize count column
        if "total_count" in df1.columns:
            df1["total_count"] = df1["total_count"].fillna(0).astype(int)

        if "total_amount" in df1.columns:
            df1["pretty_amount"] = df1["total_amount"].fillna(0).astype(float).apply(format_inr_short)

        if "total_app_opens" in df1.columns:
            df1["total_app_opens"] = df1["total_app_opens"].fillna(0).astype(int)
        
        if selected_data == "User":
            text = df1.apply(
                lambda row: f"India : {row['state']}<br>"
                            f"Total App Opens: {row['total_app_opens']:,}<br>"
                            f"Registered Users: {row['total_users']:,}",
                axis=1
            )
            zdata = df1["total_app_opens"].astype(int)

        elif selected_data == "Insurance":
            text = df1.apply(
                lambda row: f"India : {row['state']}<br>"
                            f"All Insurance: {row['total_count']:,}<br>"
                            f"Total Insurance Value: {row['pretty_amount']}",
                axis=1
            )
            zdata = df1["total_count"].astype(int)

        elif selected_data == "Trasaction":
            text = df1.apply(
                lambda row: f"India : {row['state']}<br>"
                            f"All Transaction: {row['total_count']:,}<br>"
                            f"Total Payment Value: {row['pretty_amount']}",
                axis=1
            )
            zdata = df1["total_count"].astype(int)
    # Maps and Visualizations
    # st.dataframe(df1)
    # st.write(df1['state'])
    # st.write(zdata)
    # st.write(text)
    # st.write(zdata)

    if not df1.empty:    
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        india_states = requests.get(url).json()    
        # --- Choropleth with custom hover info ---
        fig = go.Figure(data=go.Choropleth(
            geojson=india_states,
            featureidkey="properties.ST_NM",
            locations=df1["state"],
            z=zdata,  # Ensure z is numeric

        colorscale="Reds",
            autocolorscale=False,
            marker_line_color="peachpuff",

            hovertemplate=text,  # only show custom text, no trace name

            colorbar=dict(
                title="Transaction Count",
                thickness=15,
                len=0.35,
                bgcolor="rgba(255,255,255,0.6)",
                tick0=0,
                dtick=5000,
                xanchor="left",
                x=0.01,
                yanchor="bottom",
                y=0.05
            )
        ))

        # --- Layout ---
        fig.update_geos(
            fitbounds="locations",
            visible=False
        )

        fig.update_layout(
            title="PhonePe Transaction Analysis Across States",
            geo=dict(scope="asia"),
            margin={"r":0,"t":30,"l":0,"b":0}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available to display the map.")