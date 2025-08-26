import pandas as pd
import json
import os
import psycopg2
import dbconnection as dbconnection

# Path to the top data directory
path_user =".venv/data/top/user/country/india/state"
path_insurance =".venv/data/top/insurance/country/india/state"
path_transaction =".venv/data/top/transaction/country/india/state"

#Table creation SQL statements
create_table_User_sql = """
CREATE TABLE IF NOT EXISTS top_user (
    user_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater INT,
    user_type VARCHAR(50),
    registeredUsers_count INT
);
"""
create_table_insurance_sql = """
CREATE TABLE IF NOT EXISTS top_insurance (
    Insurance_id SERIAL PRIMARY KEY,  -- auto increment
    State VARCHAR(50) NOT NULL,
    Year VARCHAR(15),
    Quater INT,
    Insurance_type VARCHAR(50),
    Insurance_count BIGINT,
    Insurance_amount FLOAT
);
"""
create_table_transaction_sql = """
CREATE TABLE IF NOT EXISTS top_transaction (
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
INSERT INTO top_user (State, Year, Quater, user_type, registeredUsers_count)      
VALUES (%s, %s, %s, %s, %s)
"""

insert_Insurance_sql = """
INSERT INTO top_insurance (State, Year, Quater, Insurance_type, Insurance_count, Insurance_amount)      
VALUES (%s, %s, %s, %s, %s, %s)
"""

insert_transaction_sql = """
INSERT INTO top_transaction (State, Year, Quater, Transaction_type, Transaction_count, Transaction_amount)      
VALUES (%s, %s, %s, %s, %s, %s)
"""
# Get the list of top state cleaned data files
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
top_state_user_list=os.listdir(path_user)
top_state_insurance_list=os.listdir(path_insurance)
top_state_transaction_list=os.listdir(path_transaction)

# Initialize data structures for each table
userData ={'State':[], 'Year':[],'Quater':[],'user_type':[], 'registeredUsers_count':[]}
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
    for i in top_state_user_list:
        p_i=path_user+"/"+i+"/"
        top_yr=os.listdir(p_i)
        for j in top_yr:
            p_j=p_i+j+"/"
            top_yr_list=os.listdir(p_j)
            for k in top_yr_list:
                p_k=p_j+k
                with open(p_k, 'r') as Data:
                    D1 = json.load(Data)

                # districts
                if D1['data']['districts'] is not None:
                    for z in D1['data']['districts']:
                        userData['State'].append(i)
                        userData['Year'].append(j)
                        userData['Quater'].append(int(k.strip('.json')))
                        userData['user_type'].append(z['name'])
                        userData['registeredUsers_count'].append(z['registeredUsers'])

                # pincodes
                if D1['data']['pincodes'] is not None:
                    for z1 in D1['data']['pincodes']:
                        userData['State'].append(i)
                        userData['Year'].append(j)
                        userData['Quater'].append(int(k.strip('.json')))
                        userData['user_type'].append(z1['name'])
                        userData['registeredUsers_count'].append(z1['registeredUsers'])
    dfuserData = pd.DataFrame(userData)
    dfuserData['State'] = dfuserData['State'].replace(state_mapping)   # print(df)
    for _, row in dfuserData.iterrows():       
        # Insert data into the table    
        cursor.execute(insert_User_sql, (
            row['State'][0] if isinstance(row['State'], list) else row['State'],
            row['Year'][0] if isinstance(row['Year'], list) else row['Year'],
            int(row['Quater'][0] if isinstance(row['Quater'], list) else row['Quater']),
            row['user_type'][0] if isinstance(row['user_type'], list) else row['user_type'],
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
    for i in top_state_insurance_list:
        p_i=path_insurance+"/"+i+"/"
        top_yr=os.listdir(p_i)
        for j in top_yr:
            p_j=p_i+j+"/"
            top_yr_list=os.listdir(p_j)
            for k in top_yr_list:
                p_k=p_j+k
                with open(p_k, 'r') as Data:
                    D1 = json.load(Data)
                # districts
                if D1['data']['districts'] is not None:
                    for z in D1['data']['districts']:
                        insuranceData['State'].append(i)
                        insuranceData['Year'].append(j)
                        insuranceData['Quater'].append(int(k.strip('.json')))
                        insuranceData['Insurance_type'].append(z['entityName'])
                        insuranceData['Insurance_count'].append(z['metric']['count'])
                        insuranceData['Insurance_amount'].append(z['metric']['amount'])

                # pincodes
                if D1['data']['pincodes'] is not None:
                    for z1 in D1['data']['pincodes']:
                        insuranceData['State'].append(i)
                        insuranceData['Year'].append(j)
                        insuranceData['Quater'].append(int(k.strip('.json')))
                        insuranceData['Insurance_type'].append(z1['entityName'])
                        insuranceData['Insurance_count'].append(z1['metric']['count'])
                        insuranceData['Insurance_amount'].append(z1['metric']['amount'])
    dfinsuranceData = pd.DataFrame(insuranceData)
    dfinsuranceData['State'] = dfinsuranceData['State'].replace(state_mapping)   # print(df)
    for _, row in dfinsuranceData.iterrows():
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
    for i in top_state_transaction_list:
        p_i = path_transaction + "/" + i + "/"
        top_yr = os.listdir(p_i)

        for j in top_yr:  # Year folders
            p_j = p_i + j + "/"
            top_yr_list = os.listdir(p_j)

            for k in top_yr_list:  # Quarter JSON files
                p_k = p_j + k
                with open(p_k, 'r') as Data:
                    D1 = json.load(Data)

                # districts
                if D1['data']['districts'] is not None:
                    for z in D1['data']['districts']:
                        transactionData['State'].append(i)
                        transactionData['Year'].append(j)
                        transactionData['Quater'].append(int(k.strip('.json')))
                        transactionData['Transacion_type'].append(z['entityName'])
                        transactionData['Transacion_count'].append(z['metric']['count'])
                        transactionData['Transacion_amount'].append(z['metric']['amount'])

                # pincodes
                if D1['data']['pincodes'] is not None:
                    for z1 in D1['data']['pincodes']:
                        transactionData['State'].append(i)
                        transactionData['Year'].append(j)
                        transactionData['Quater'].append(int(k.strip('.json')))
                        transactionData['Transacion_type'].append(z1['entityName'])
                        transactionData['Transacion_count'].append(z1['metric']['count'])
                        transactionData['Transacion_amount'].append(z1['metric']['amount'])
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



