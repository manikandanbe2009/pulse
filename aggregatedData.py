import pandas as pd
import json
import os
import psycopg2
import dbconnection as dbconnection

# Path to the aggregated data directory
path_user =".venv/data/aggregated/user/country/india/state"
path_insurance =".venv/data/aggregated/insurance/country/india/state"
path_transaction =".venv/data/aggregated/transaction/country/india/state"

#Table creation SQL statements
create_table_User_sql = """
CREATE TABLE IF NOT EXISTS Aggregated_user (
    user_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quarter INT,
    Brand VARCHAR(50),
    Brand_Count BIGINT,
    Brand_Percentage FLOAT,
    Registered_Users BIGINT,
    App_Opens BIGINT
);
"""
create_table_insurance_sql = """
CREATE TABLE IF NOT EXISTS aggregated_insurance (
    Insurance_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater BIGINT,
    Insurance_type VARCHAR(50),
    Insurance_count BIGINT,
    Insurance_amount FLOAT
);
"""
create_table_transaction_sql = """
CREATE TABLE IF NOT EXISTS aggregated_transaction (
    trans_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater INT,
    Transaction_type VARCHAR(50),
    Transaction_count BIGINT,
    Transaction_amount FLOAT
);
"""
# Insert SQL statements for each table
insert_User_sql = """
INSERT INTO Aggregated_user (State, Year, Quarter, Brand, Brand_Count, Brand_Percentage, Registered_Users, App_Opens)      
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

insert_Insurance_sql = """
INSERT INTO aggregated_insurance (State, Year, Quater, Insurance_type, Insurance_count, Insurance_amount)      
VALUES (%s, %s, %s, %s, %s, %s)
"""

insert_transaction_sql = """
INSERT INTO aggregated_transaction (State, Year, Quater, Transaction_type, Transaction_count, Transaction_amount)      
VALUES (%s, %s, %s, %s, %s, %s)
"""
# Get the list of aggregated state cleaned data files
state_mapping = {
    'andaman-&-nicobar-islands':'Andaman & Nicobar',
'andhra-pradesh':'Andhra Pradesh',
'arunachal-pradesh':'Arunachal Pradesh',
'assam':'Assam',
'bihar':'Bihar',
'chandigarh':'Chandigarh',
'chhattisgarh':'Chhattisgarh',
'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu',
'delhi':'Delhi',
'goa':'Goa',
'gujarat':'Gujarat',
'haryana':'Haryana',
'himachal-pradesh':'Himachal Pradesh',
'jammu-&-kashmir':'Jammu & Kashmir',
'jharkhand':'Jharkhand',
'karnataka':'Karnataka',
'kerala':'Kerala',
'ladakh':'Ladakh',
'madhya-pradesh':'Madhya Pradesh',
'maharashtra':'Maharashtra',
'manipur':'Manipur',
'meghalaya':'Meghalaya',
'mizoram':'Mizoram',
'nagaland':'Nagaland',
'odisha':'Odisha',
'puducherry':'Puducherry',
'punjab':'Punjab',
'rajasthan':'Rajasthan',
'sikkim':'Sikkim',
'tamil-nadu':'Tamil Nadu',
'telangana':'Telangana',
'tripura':'Tripura',
'uttar-pradesh':'Uttarakhand',
'uttarakhand':'Uttar Pradesh',
'west-bengal':'West Bengal',
'lakshadweep':'Lakshadweep'
}

#Added the path list directly to the code
Agg_state_user_list=os.listdir(path_user)
Agg_state_insurance_list=os.listdir(path_insurance)
Agg_state_transaction_list=os.listdir(path_transaction)

# Initialize data structures for each table
user_data ={
    "State": [], "Year": [], "Quarter": [],
    "Registered_Users": [], "App_Opens": [],
    "Brand": [], "Brand_Count": [], "Brand_Percentage": []
}
insuranceData ={'State':[], 'Year':[],'Quater':[],'Insurance_type':[], 'Insurance_count':[], 'Insurance_amount':[]}
transactionData ={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

# Process user data
try:
    # 1. Connect to DB
    conn = dbconnection.create_connection()
    cursor = conn.cursor()
    # # 2. Create Table
    cursor.execute(create_table_User_sql)
    conn.commit()
    for state in os.listdir(path_user):
        state_path = os.path.join(path_user, state)
        for year in os.listdir(state_path):
            if not year.isdigit():  # skip non-year folders
                continue
            year_val = int(year)

            year_path = os.path.join(state_path, year)
            for qfile in os.listdir(year_path):
                if not qfile.endswith(".json"):
                    continue

                qname = qfile.replace(".json", "")
                if not qname.isdigit():  # skip bad file names
                    continue
                quarter_val = int(qname)

                qpath = os.path.join(year_path, qfile)
                with open(qpath, "r") as f:
                    data = json.load(f)

                agg = data.get("data", {}).get("aggregated", {})
                registered = agg.get("registeredUsers", 0)
                app_opens = agg.get("appOpens", 0)

                users_by_device = data.get("data", {}).get("usersByDevice")

                if users_by_device and isinstance(users_by_device, list):
                    for dev in users_by_device:
                        user_data["State"].append(state)
                        user_data["Year"].append(year_val)
                        user_data["Quarter"].append(quarter_val)
                        user_data["Registered_Users"].append(registered)
                        user_data["App_Opens"].append(app_opens)
                        user_data["Brand"].append(dev.get("brand"))
                        user_data["Brand_Count"].append(dev.get("count"))
                        user_data["Brand_Percentage"].append(dev.get("percentage"))
                else:
                    user_data["State"].append(state)
                    user_data["Year"].append(year_val)
                    user_data["Quarter"].append(quarter_val)
                    user_data["Registered_Users"].append(registered)
                    user_data["App_Opens"].append(app_opens)
                    user_data["Brand"].append(None)
                    user_data["Brand_Count"].append(None)
                    user_data["Brand_Percentage"].append(None)
    dfuserData = pd.DataFrame(user_data)
    dfuserData['State'] = dfuserData['State'].replace(state_mapping)       
    records = []
    for _, row in dfuserData.iterrows():
        records.append((
            row['State'],
            int(row['Year']),
            int(row['Quarter']),
            row['Brand'],
            int(row['Brand_Count']) if pd.notna(row['Brand_Count']) else None,
            float(row['Brand_Percentage']) if pd.notna(row['Brand_Percentage']) else None,
            int(row['Registered_Users']) if pd.notna(row['Registered_Users']) else None,
            int(row['App_Opens']) if pd.notna(row['App_Opens']) else None
        ))

    cursor.executemany(insert_User_sql, records)
    conn.commit()
#Succesfully created a dataframe
except Exception as e:
    print(f"An error occurred: {e}")

# Process insurance data
try:
    # 1. Connect to DB
    conn = dbconnection.create_connection()
    cursor = conn.cursor()
    # # 2. Create Table
    cursor.execute(create_table_insurance_sql)
    conn.commit()
    for i in Agg_state_insurance_list:
        p_i=path_insurance+"/"+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['transactionData']:
                    Name=z['name']
                    count=z['paymentInstruments'][0]['count']
                    amount=z['paymentInstruments'][0]['amount']
                    insuranceData['Insurance_type'].append(Name)
                    insuranceData['Insurance_count'].append(count)
                    insuranceData['Insurance_amount'].append(amount)
                    insuranceData['State'].append(i)
                    insuranceData['Year'].append(j)
                    insuranceData['Quater'].append(int(k.strip('.json')))
    dfinsuranceData = pd.DataFrame(insuranceData)
    dfinsuranceData['State'] = dfinsuranceData['State'].replace(state_mapping)   # print(df)
    for _, row in dfinsuranceData.iterrows():
        # Insert data into the table
    
        cursor.execute(insert_Insurance_sql, (
            row['State'][0] if isinstance(row['State'], list) else row['State'],
            row['Year'][0] if isinstance(row['Year'], list) else row['Year'],
            int(row['Quater'][0] if isinstance(row['Quater'], list) else row['Quater']),
            row['Insurance_type'][0] if isinstance(row['Insurance_type'], list) else row['Insurance_type'],
            int(row['Insurance_count'][0] if isinstance(row['Insurance_count'], list) else row['Insurance_count']),
            float(row['Insurance_amount'][0] if isinstance(row['Insurance_amount'], list) else row['Insurance_amount'])
        ))
        conn.commit()
#Succesfully created a dataframe
except Exception as e:
    print(f"An error occurred: {e}")

# Process transaction data
try:
    # 1. Connect to DB
    conn = dbconnection.create_connection()
    cursor = conn.cursor()

    # # 2. Create Table
    cursor.execute(create_table_transaction_sql)
    conn.commit()
    for i in Agg_state_transaction_list:
        p_i=path_transaction+"/"+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                for z in D['data']['transactionData']:
                    Name=z['name']
                    count=z['paymentInstruments'][0]['count']
                    amount=z['paymentInstruments'][0]['amount']
                    transactionData['Transacion_type'].append(Name)
                    transactionData['Transacion_count'].append(count)
                    transactionData['Transacion_amount'].append(amount)
                    transactionData['State'].append(i)
                    transactionData['Year'].append(j)
                    transactionData['Quater'].append(int(k.strip('.json')))
    dftransactionData = pd.DataFrame(transactionData)
    dftransactionData['State'] = dftransactionData['State'].replace(state_mapping)   # print(df)
    for _, row in dftransactionData.iterrows():
        # Insert data into the table
    
        cursor.execute(insert_transaction_sql, (
            row['State'][0] if isinstance(row['State'], list) else row['State'],
            row['Year'][0] if isinstance(row['Year'], list) else row['Year'],
            int(row['Quater'][0] if isinstance(row['Quater'], list) else row['Quater']),
            row['Transacion_type'][0] if isinstance(row['Transacion_type'], list) else row['Transacion_type'],
            int(row['Transacion_count'][0] if isinstance(row['Transacion_count'], list) else row['Transacion_count']),
            float(row['Transacion_amount'][0] if isinstance(row['Transacion_amount'], list) else row['Transacion_amount'])
        ))
        conn.commit()
#Succesfully created a dataframe
except Exception as e:
    print(f"An error occurred: {e}")



