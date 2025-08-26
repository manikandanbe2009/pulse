import pandas as pd
import json
import os
import psycopg2
import dbconnection as dbconnection

# Path to the map data directory
path_user =".venv/data/map/user/hover/country/india/state"
path_insurance =".venv/data/map/insurance/hover/country/india/state"
path_transaction =".venv/data/map/transaction/hover/country/india/state"

#Table creation SQL statements
create_table_User_sql = """
CREATE TABLE IF NOT EXISTS map_user (
    user_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater INT,
    User_district VARCHAR(50),
    registeredUsers_count INT
);
"""
create_table_insurance_sql = """
CREATE TABLE IF NOT EXISTS map_insurance (
    Insurance_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater INT,
    Trans_district VARCHAR(50),
    districtTrans_count INT,
    districtTrans_amount FLOAT
);
"""
create_table_transaction_sql = """
CREATE TABLE IF NOT EXISTS map_transaction (
    trans_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater INT,
    Insurance_district VARCHAR(50),
    districtInsurance_count INT,
    districtInsurancet_amount FLOAT
);
"""
# Insert SQL statements for each table
insert_User_sql = """
INSERT INTO map_user (State, Year, Quater, User_district, registeredUsers_count)      
VALUES (%s, %s, %s, %s, %s)
"""

insert_Insurance_sql = """
INSERT INTO map_insurance (State, Year, Quater, Trans_district, districtTrans_count, districtTrans_amount)      
VALUES (%s, %s, %s, %s, %s, %s)
"""

insert_transaction_sql = """
INSERT INTO map_transaction (State, Year, Quater, Insurance_district, districtInsurance_count, districtInsurancet_amount)      
VALUES (%s, %s, %s, %s, %s, %s)
"""
# Get the list of map state cleaned data files
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
Map_state_user_list=os.listdir(path_user)
Map_state_insurance_list=os.listdir(path_insurance)
Map_state_transaction_list=os.listdir(path_transaction)

# Initialize data structures for each table
userData = {'State':[], 'Year':[],'Quater':[],'User_district':[], 'registeredUsers_count':[]}
insuranceData ={'State':[], 'Year':[],'Quater':[],'Trans_district':[], 'districtTrans_count':[], 'districtTrans_amount':[]}
transactionData ={'State':[], 'Year':[],'Quater':[],'Insurance_district':[], 'districtInsurance_count':[], 'districtInsurancet_amount':[]}

# Process user data
try:
    # 1. Connect to DB
    conn = dbconnection.create_connection()
    cursor = conn.cursor()
    # # 2. Create Table
    cursor.execute(create_table_User_sql)
    conn.commit()
    for i in Map_state_user_list:
        p_i=path_user+"/"+i+"/"
        Map_yr=os.listdir(p_i)
        for j in Map_yr:
            p_j=p_i+j+"/"
            Map_yr_list=os.listdir(p_j)
            for k in Map_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D1=json.load(Data)
                hover_data = D1['data'].get('hoverData', {})
                for place, info in hover_data.items():
                    name=place
                    count=info['registeredUsers']
                    userData['User_district'].append(name)
                    userData['registeredUsers_count'].append(count)
                    userData['State'].append(i)
                    userData['Year'].append(j)
                    userData['Quater'].append(int(k.strip('.json')))
    dfuserData = pd.DataFrame(userData)
    dfuserData['State'] = dfuserData['State'].replace(state_mapping)   # print(df)
    for _, row in dfuserData.iterrows():       
        # Insert data into the table    
        cursor.execute(insert_User_sql, (
            row['State'][0] if isinstance(row['State'], list) else row['State'],
            row['Year'][0] if isinstance(row['Year'], list) else row['Year'],
            int(row['Quater'][0] if isinstance(row['Quater'], list) else row['Quater']),
            row['User_district'][0] if isinstance(row['User_district'], list) else row['User_district'],
            int(row['registeredUsers_count'][0] if isinstance(row['registeredUsers_count'], list) else row['registeredUsers_count'])
        ))
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
    for i in Map_state_insurance_list:
        p_i=path_insurance+"/"+i+"/"
        Map_yr=os.listdir(p_i)
        for j in Map_yr:
            p_j=p_i+j+"/"
            Map_yr_list=os.listdir(p_j)
            for k in Map_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D1=json.load(Data)
                for z in D1['data']['hoverDataList']:
                    name=z['name']
                    count=z['metric'][0]['count']
                    amount=z['metric'][0]['amount']
                    insuranceData['Trans_district'].append(name)
                    insuranceData['districtTrans_count'].append(count)
                    insuranceData['districtTrans_amount'].append(amount)
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
            row['Trans_district'][0] if isinstance(row['Trans_district'], list) else row['Trans_district'],
            int(row['districtTrans_count'][0] if isinstance(row['districtTrans_count'], list) else row['districtTrans_count']),
            float(row['districtTrans_amount'][0] if isinstance(row['districtTrans_amount'], list) else row['districtTrans_amount'])
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
    for i in Map_state_transaction_list:
        p_i=path_transaction+"/"+i+"/"
        Map_yr=os.listdir(p_i)
        for j in Map_yr:
            p_j=p_i+j+"/"
            Map_yr_list=os.listdir(p_j)
            for k in Map_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D1=json.load(Data)
                for z in D1['data']['hoverDataList']:
                    name=z['name']
                    count=z['metric'][0]['count']
                    amount=z['metric'][0]['amount']
                    transactionData['Insurance_district'].append(name)
                    transactionData['districtInsurance_count'].append(count)
                    transactionData['districtInsurancet_amount'].append(amount)
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
            row['Insurance_district'][0] if isinstance(row['Insurance_district'], list) else row['Insurance_district'],
            int(row['districtInsurance_count'][0] if isinstance(row['districtInsurance_count'], list) else row['districtInsurance_count']),
            float(row['districtInsurancet_amount'][0] if isinstance(row['districtInsurancet_amount'], list) else row['districtInsurancet_amount'])
        ))
        conn.commit()
#Succesfully created a dataframe
except Exception as e:
    print(f"An error occurred: {e}")



