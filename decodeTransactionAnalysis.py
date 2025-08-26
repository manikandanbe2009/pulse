import streamlit as st
import pandas as pd
import plotly.express as px
import dbconnection as dbconnection

# --- DB Connection (adjust credentials) ---
@st.cache_data
def load_data():
    conn = dbconnection.create_connection()
    query = """
        SELECT 
            state,
            transaction_type,
            year,
            quater,
            transaction_count,
            transaction_amount
        FROM aggregated_transaction;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()

def format_inr(value):
    if value >= 1e12:
        return f"{value/1e12:.2f} Trillion"
    elif value >= 1e7:
        return f"{value/1e7:.2f} Crores"
    elif value >= 1e5:
        return f"{value/1e5:.2f} Lakhs"
    else:
        return f"{value:,.0f}"
        
def decodeTransactionAnalysis():
    # --- Data Preparation ---
    df["quarter"] = df["year"].astype(str) + "Q" + df["quater"].astype(str)

    # Aggregate
    agg = df.groupby(["state", "transaction_type", "year", "quater", "quarter"], as_index=False).agg({
        "transaction_count": "sum",
        "transaction_amount": "sum"
    })

    # Growth calcs
    agg = agg.sort_values(["state", "transaction_type", "year", "quater"])
    agg['transaction_count_Ft'] = agg['transaction_count'].apply(format_inr)
    agg['transaction_amount_Ft'] = agg['transaction_amount'].apply(format_inr)

    agg["qoq_growth"] = agg.groupby(["state","transaction_type"])["transaction_amount"].pct_change()
    agg["yoy_growth"] = agg.groupby(["state","transaction_type"])["transaction_amount"].pct_change(4)
    agg["qoq_growth"] = agg["qoq_growth"].fillna(0)
    agg["yoy_growth"] = agg["yoy_growth"].fillna(0)
    # --- Streamlit Layout ---
    st.set_page_config(page_title="PhonePe Transaction Dynamics", layout="wide")

    st.title("Decoding Transaction Dynamics on PhonePe")
    column1, column2, column3, column4 = st.columns(4)
    # Filters
    with column1:
        sel_qtr = st.selectbox("Select Quarter", sorted(agg["quarter"].unique()))
    with column2:
        sel_state = st.selectbox("Select State", sorted(agg["state"].unique()))
    with column3:
        sel_type = st.multiselect("Select Transaction Types", agg["transaction_type"].unique(),
        default=agg["transaction_type"].unique())
    
    movers = agg[agg["quarter"]==sel_qtr].sort_values("qoq_growth", ascending=False)
    # KPI header
    kpi = agg[(agg["quarter"]==sel_qtr) & (agg["transaction_type"].isin(sel_type))]
    col1,col2,col3,col4 = st.columns(4)
    col1.metric("Total GMV", f"₹{format_inr(kpi['transaction_amount'].sum())}")
    col2.metric("Total Transactions", f"{format_inr(kpi['transaction_count'].sum())}")
    col3.metric("Avg QoQ Growth", f"{kpi['qoq_growth'].mean():+.1%}")
    col4.metric("Avg YoY Growth", f"{kpi['yoy_growth'].mean():+.1%}")

    # Heatmap: QoQ growth by State × Type
    pivot = agg[agg["quarter"]==sel_qtr].pivot_table(index="state", columns="transaction_type", values="qoq_growth")
    fig_hm = px.imshow(pivot, aspect="auto", origin="lower", color_continuous_scale="RdYlGn",
                    labels=dict(color="QoQ Growth"), title=f"QoQ Growth Heatmap — {sel_qtr}")
    st.plotly_chart(fig_hm, use_container_width=True)
    top_state = movers.iloc[0]["state"]
    top_type = movers.iloc[0]["transaction_type"]
    top_growth = movers.iloc[0]["qoq_growth"]
    st.markdown(
        f"💡 **Insight:** In {sel_qtr}, *{top_state}* saw the highest growth in "
        f"**{top_type}** transactions with a QoQ growth of {top_growth:.1%}."
    )

    # Trend by State & Type
    trend = agg[(agg["state"]==sel_state) & (agg["transaction_type"].isin(sel_type))]
    fig_trend = px.line(trend, x="quarter", y="transaction_amount", color="transaction_type",
                        markers=True, title=f"Transaction Amount Trend — {sel_state}")
    st.plotly_chart(fig_trend, use_container_width=True)
    # Insight
    latest = trend.groupby("transaction_type")["transaction_amount"].last()
    fastest = latest.idxmax()
    st.markdown(
        f"📈 **Insight:** In {sel_state}, **{fastest} transactions** currently drive the largest share of GMV. "
        f"The trend shows {'consistent growth' if trend['transaction_amount'].iloc[-1] > trend['transaction_amount'].mean() else 'fluctuations'} over recent quarters."
    )

    # Mix Shift: Category share within State
    mix = trend.groupby(["quarter","transaction_type"])["transaction_amount"].sum().reset_index()
    fig_mix = px.area(mix, x="quarter", y="transaction_amount", color="transaction_type",
                    groupnorm="fraction", title=f"Category Mix Shift — {sel_state}")
    st.plotly_chart(fig_mix, use_container_width=True)

    # Insight
    mix_latest = mix[mix["quarter"]==mix["quarter"].max()]
    dominant = mix_latest.loc[mix_latest["transaction_amount"].idxmax(), "transaction_type"]
    st.markdown(
        f"🌀 **Insight:** By {mix_latest['quarter'].iloc[0]}, **{dominant}** dominates the transaction mix in {sel_state}, "
        f"indicating a strong shift in user preference."
    )

    # Top Movers Table
    movers = agg[agg["quarter"]==sel_qtr].sort_values("qoq_growth", ascending=False)
    st.subheader(f"📈 Top States/Types by QoQ Growth — {sel_qtr}")
    st.dataframe(movers[["state","transaction_type","transaction_amount","qoq_growth"]].head(15))
    top_state = movers.iloc[0]["state"]
    top_type = movers.iloc[0]["transaction_type"]
    top_growth = movers.iloc[0]["qoq_growth"]
    st.markdown(
        f"💡 **Insight:** In {sel_qtr}, *{top_state}* saw the highest growth in "
        f"**{top_type}** transactions with a QoQ growth of {top_growth:.1%}."
    )
    
    worst = movers.iloc[-1]
    st.markdown(
        f"📉 **Insight:** While top performers surged, **{worst['state']} – {worst['transaction_type']}** "
        f"faced the steepest decline with a QoQ drop of {worst['qoq_growth']:.1%}."
    )
