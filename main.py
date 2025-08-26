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
# dbconnection.create_connection()
import  map as map
import userTopAnalysis as userTopAnalysis
import insuranceTopAnalysis as insuranceTopAnalysis
import transactionTopAnalysis as transactionTopAnalysis
import decodeTransactionAnalysis as decodeTransactionAnalysis
import userDeviceAnalysis as userDeviceAnalysis
import insuranceAnalysis1 as insuranceAnalysis1
st.set_page_config(
    page_title="PhonePe Data Analysis",
    layout="wide")

with st.sidebar : 
    selected = option_menu(
        menu_title="PhonePe Data Analysis", 
        options=["Home","Decoding Transaction Dynamics on PhonePe","Device Dominance and User Engagement Analysis","Insurance Transactions Analysis", "User Registration Analysis", "Transaction Analysis Across States and Districts"],
        menu_icon="film",
        default_index=0,
    )
if selected == "Home":
        map.homepage()
        
elif selected == "Insurance Transactions Analysis":
    insuranceTopAnalysis.insurance_analysis()
elif selected == "User Registration Analysis":
    userTopAnalysis.user_registration_analysis()
elif selected == "Transaction Analysis Across States and Districts":
    transactionTopAnalysis.transaction_analysis()
   
elif selected == "Decoding Transaction Dynamics on PhonePe":
     decodeTransactionAnalysis.decodeTransactionAnalysis()
elif selected == "Device Dominance and User Engagement Analysis":
     userDeviceAnalysis.userDeviceAnalysis()
else:
    # insuranceAnalysis1.insuranceAnalysis1()
    st.info("No insurance data available for analysis.")

    
        