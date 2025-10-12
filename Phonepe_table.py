#!/usr/bin/env python
# coding: utf-8

# In[2]:


import json
import pandas as pd
import os


# In[3]:


import seaborn as sns


# In[4]:


import warnings
warnings.filterwarnings('ignore')


# In[5]:


import matplotlib.pyplot as plt


# In[4]:


# Change to your folder path
os.chdir("C:\\Users\\schan\\Shyam\\data\\data")  # use double backslashes on Windows

# Confirm the current directory
os.getcwd()


# In[5]:


import mysql.connector 

# Connect to the MySQL database 
conn = mysql.connector.connect( 
    host="127.0.0.1", # Hostname 
    user="root", # Username 
    password="Admin@123", # Replace with your MySQL root password 
    database="phonepe", # Database name 
    port=3306 # Port number 
) 
cursor = conn.cursor()

# Test the connection
cursor.execute("SELECT DATABASE();")
db = cursor.fetchone()
print(f"Connected to database: {db[0]}")


# In[28]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\aggregated\\transaction\\country\\india\\state")
loc1=os.getcwd()
path=loc1
Agg_tran_state_list=os.listdir(path)
Agg_tran_state_list


# In[ ]:


# aggregated_transaction_state


# In[7]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\aggregated\\transaction\\country\\india\\state")
loc1=os.getcwd()
path=loc1
Agg_tran_state_list=os.listdir(loc1)

clm_agg_tran={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Agg_tran_state_list:
    p_i=path+"\\"+i+"\\"
    Agg_tran_yr=os.listdir(p_i)

    for j in Agg_tran_yr:
        p_j=p_i+"\\"+j+"\\"
        Agg_tran_list=os.listdir(p_j)

        for k in Agg_tran_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)

            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm_agg_tran['Transaction_type'].append(Name)
              clm_agg_tran['Transaction_count'].append(count)
              clm_agg_tran['Transaction_amount'].append(amount)
              clm_agg_tran['State'].append(i)
              clm_agg_tran['Year'].append(j)
              clm_agg_tran['Quarter'].append(int(k.strip('.json')))

agg_trans=pd.DataFrame(clm_agg_tran)
agg_trans


# In[ ]:


# table creation for aggregated_transaction_state


# In[58]:


agg_trans_table = '''CREATE TABLE aggregated_transaction (State varchar(50), Year int, Quarter int, Transaction_type varchar(50), Transaction_count bigint, 
Transaction_amount bigint)'''

cursor.execute(agg_trans_table)
conn.commit()

for index,row in agg_trans.iterrows():
    insert_values = '''INSERT INTO aggregated_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["Transaction_type"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:


# aggregated_insurance_state


# In[46]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\aggregated\\insurance\\country\\india\\state")
loc2=os.getcwd()
path2=loc2
agg_ins_state_list = os.listdir(path2)

clm_agg_ins = {"State":[], "Year":[], "Quarter":[], "Name":[],"Insurance_count":[], "Insurance_amount":[]}

for i in agg_ins_state_list:
    p_i=path2+"\\"+i+"\\"
    agg_ins_yr = os.listdir(p_i)

    for j in agg_ins_yr:
        p_j= p_i+"\\"+j+"\\"
        agg_ins_list = os.listdir(p_j)

        for k in agg_ins_list:
            p_k =p_j+k
            data=open(p_k,'r')
            B=json.load(data)

            for z in B['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm_agg_ins['Name'].append(Name)
              clm_agg_ins['Insurance_count'].append(count)
              clm_agg_ins['Insurance_amount'].append(amount)
              clm_agg_ins['State'].append(i)
              clm_agg_ins['Year'].append(j)
              clm_agg_ins['Quarter'].append(int(k.strip('.json')))

agg_ins=pd.DataFrame(clm_agg_ins)
agg_ins


# In[ ]:


# table creation for aggregated_insurance_state


# In[57]:


agg_ins_table = '''CREATE TABLE aggregated_insurance (State varchar(50), Year int, Quarter int, Name varchar(50), Insurance_count bigint, 
Insurance_amount bigint)'''

cursor.execute(agg_ins_table)
conn.commit()

for index,row in agg_ins.iterrows():
    insert_values = '''INSERT INTO aggregated_insurance (State, Year, Quarter, Name, Insurance_count, Insurance_amount)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["Name"],
              row["Insurance_count"],
              row["Insurance_amount"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:


# aggregated_users_state


# In[3]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\aggregated\\user\\country\\india\\state")
loc1=os.getcwd()
path3=loc1
agg_usr_state_list = os.listdir(path3)

clm_agg_usr = {"State":[], "Year":[], "Quarter":[], "Brands":[],"UserByDevice_count":[], "Percentage":[]}

for i in agg_usr_state_list:
    p_i=path3+"\\"+i+"\\"
    agg_usr_yr = os.listdir(p_i)

    for j in agg_usr_yr:
        p_j= p_i+"\\"+j+"\\"
        agg_usr_list = os.listdir(p_j)

        for k in agg_usr_list:
            p_k =p_j+k
            data=open(p_k,'r')
            A=json.load(data)

            try:

                for z in A["data"]["usersByDevice"]:
                    brand = z["brand"]
                    count = z["count"]
                    percentage = z["percentage"]
                    clm_agg_usr["Brands"].append(brand)
                    clm_agg_usr["UserByDevice_count"].append(count)
                    clm_agg_usr["Percentage"].append(percentage)
                    clm_agg_usr["State"].append(i)
                    clm_agg_usr["Year"].append(j)
                    clm_agg_usr["Quarter"].append(int(k.strip(".json")))

            except:
                pass

agg_user = pd.DataFrame(clm_agg_usr)
agg_user


# In[ ]:


# table creation for aggregated_user_state


# In[6]:


agg_usr_table = '''CREATE TABLE aggregated_user (State varchar(50), Year int, Quarter int, Brands varchar(50), UserByDevice_count bigint, 
Percentage float)'''

cursor.execute(agg_usr_table)
conn.commit()

for index,row in agg_user.iterrows():
    insert_values = '''INSERT INTO aggregated_user (State, Year, Quarter, Brands, UserByDevice_count, Percentage)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["Brands"],
              row["UserByDevice_count"],
              row["Percentage"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[1]:


# map_insurance_state


# In[3]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\map\\insurance\\hover\\country\\india\\state")
loc1=os.getcwd()
path4=loc1
map_ins_state_list = os.listdir(path4)

clm_map_ins = {"State":[], "Year":[], "Quarter":[],"District_name":[], "Insurance_count":[],"Insurance_amount":[]}

for i in map_ins_state_list:
    p_i = path4+"\\"+i+"\\"
    map_ins_yr = os.listdir(p_i)
    
    for j in map_ins_yr:
        p_j = p_i+"\\"+j+"\\"
        map_ins_list = os.listdir(p_j)
        
        for k in map_ins_list:
            p_k = p_j+k
            data = open(p_k,"r")
            C = json.load(data)

            for z in C['data']["hoverDataList"]:
                name = z["name"]
                count = z["metric"][0]["count"]
                amount = z["metric"][0]["amount"]
                clm_map_ins["District_name"].append(name)
                clm_map_ins["Insurance_count"].append(count)
                clm_map_ins["Insurance_amount"].append(amount)
                clm_map_ins["State"].append(i)
                clm_map_ins["Year"].append(j)
                clm_map_ins["Quarter"].append(int(k.strip(".json")))

map_ins = pd.DataFrame(clm_map_ins)
map_ins


# In[ ]:


# table creation for map_insurance_state


# In[8]:


map_ins_table = '''CREATE TABLE map_insurance (State varchar(50), Year int, Quarter int, District_name varchar(50), Insurance_count bigint, 
Insurance_amount bigint)'''

cursor.execute(map_ins_table)
conn.commit()

for index,row in map_ins.iterrows():
    insert_values = '''INSERT INTO map_insurance (State, Year, Quarter, District_name, Insurance_count, Insurance_amount)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["District_name"],
              row["Insurance_count"],
              row["Insurance_amount"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:


#map_transactions_state


# In[5]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\map\\transaction\\hover\\country\\india\\state")
loc1=os.getcwd()
path=loc1
map_tran_state_list = os.listdir(path)

clm_map_tran = {"State":[], "Year":[], "Quarter":[],"District_name":[], "Transaction_count":[],"Transaction_amount":[]}

for i in map_tran_state_list:
    p_i = path+"\\"+i+"\\"
    map_tran_yr = os.listdir(p_i)
    
    for j in map_tran_yr:
        p_j = p_i+"\\"+j+"\\"
        map_tran_list = os.listdir(p_j)
        
        for k in map_tran_list:
            p_k = p_j+k
            data = open(p_k,"r")
            A = json.load(data)

            for z in A['data']["hoverDataList"]:
                name = z["name"]
                count = z["metric"][0]["count"]
                amount = z["metric"][0]["amount"]
                clm_map_tran["District_name"].append(name)
                clm_map_tran["Transaction_count"].append(count)
                clm_map_tran["Transaction_amount"].append(amount)
                clm_map_tran["State"].append(i)
                clm_map_tran["Year"].append(j)
                clm_map_tran["Quarter"].append(int(k.strip(".json")))

map_tran = pd.DataFrame(clm_map_tran)
map_tran


# In[ ]:


# table creation for map_transaction_state


# In[6]:


map_tran_table = '''CREATE TABLE map_transaction (State varchar(50), Year int, Quarter int, District_name varchar(50), Transaction_count bigint, 
Transaction_amount bigint)'''

cursor.execute(map_tran_table)
conn.commit()

for index,row in map_tran.iterrows():
    insert_values = '''INSERT INTO map_transaction (State, Year, Quarter, District_name, Transaction_count, Transaction_amount)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["District_name"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:


#map_user_state


# In[3]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\map\\user\\hover\\country\\india\\state")
loc1=os.getcwd()
path=loc1
map_usr_state_list = os.listdir(path)

clm_map_usr = {"State":[], "Year":[], "Quarter":[],"District_name":[], "Registered_user":[],"AppOpens":[]}

for i in map_usr_state_list:
    p_i = path+"\\"+i+"\\"
    map_usr_yr = os.listdir(p_i)
    
    for j in map_usr_yr:
        p_j = p_i+"\\"+j+"\\"
        map_usr_list = os.listdir(p_j)
        
        for k in map_usr_list:
            p_k = p_j+k
            data = open(p_k,"r")
            B = json.load(data)

            # print(B['data']["hoverData"].items())

            for z in B['data']["hoverData"].items():
                name = z[0]
                registereduser = z[1]["registeredUsers"]
                appopens = z[1]["appOpens"]
                clm_map_usr["District_name"].append(name)
                clm_map_usr["Registered_user"].append(registereduser)
                clm_map_usr["AppOpens"].append(appopens)
                clm_map_usr["State"].append(i)
                clm_map_usr["Year"].append(j)
                clm_map_usr["Quarter"].append(int(k.strip(".json")))

map_usr = pd.DataFrame(clm_map_usr)
map_usr


# In[ ]:


# table creation for map_user_state


# In[6]:


map_usr_table = '''CREATE TABLE map_user (State varchar(50), Year int, Quarter int, District_name varchar(50), Registered_user bigint, 
AppOpens bigint)'''

cursor.execute(map_usr_table)
conn.commit()

for index,row in map_usr.iterrows():
    insert_values = '''INSERT INTO map_user (State, Year, Quarter, District_name, Registered_user, AppOpens)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["District_name"],
              row["Registered_user"],
              row["AppOpens"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:


# top_transaction_state


# In[29]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\top\\transaction\\country\\india\\state")
loc1=os.getcwd()
path=loc1
top_tran_state_list = os.listdir(path)

clm_top_tran = {"State":[], "Year":[], "Quarter":[], "Pincode":[], "Transaction_count":[], "Transaction_amount":[]}

for i in top_tran_state_list:
    p_i = path+"\\"+i+"\\"
    top_tran_yr = os.listdir(p_i)
    
    for j in top_tran_yr:
        p_j = p_i+"\\"+j+"\\"
        top_tran_list = os.listdir(p_j)
        
        for k in top_tran_list:
            p_k = p_j+k
            data = open(p_k,"r")
            A = json.load(data)
            # print(A["data"]["pincodes"])

            for z in A["data"]["pincodes"]:
                entityname = z["entityName"]
                count = z["metric"]["count"]
                amount = z["metric"]["amount"]
                clm_top_tran["Pincode"].append(entityname)
                clm_top_tran["Transaction_count"].append(count)
                clm_top_tran["Transaction_amount"].append(amount)
                clm_top_tran["State"].append(i)
                clm_top_tran["Year"].append(j)
                clm_top_tran["Quarter"].append(int(k.strip(".json")))

top_tran = pd.DataFrame(clm_top_tran)
top_tran


# In[ ]:


# table creation for top_transaction_state


# In[32]:


top_trans_table = '''CREATE TABLE top_transaction (State varchar(50), Year int, Quarter int, Pincode int, Transaction_count bigint, 
Transaction_amount bigint)'''

cursor.execute(top_trans_table)
conn.commit()

for index,row in top_tran.iterrows():
    insert_values = '''INSERT INTO top_transaction (State, Year, Quarter, Pincode, Transaction_count, Transaction_amount)
                       values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["Pincode"],
              row["Transaction_count"],
              row["Transaction_amount"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:


# top_user_state


# In[39]:


os.chdir("C:\\Users\\schan\\Shyam\\data\\data\\top\\user\\country\\india\\state")
loc1=os.getcwd()
path=loc1
top_user_state_list = os.listdir(path)

clm_top_user = {"State":[], "Year":[], "Quarter":[], "Pincode":[], "Registered_user":[]}
for i in top_user_state_list:
    p_i = path+"\\"+i+"\\"
    top_usr_yr = os.listdir(p_i)

    for j in top_usr_yr:
        p_j = p_i+"\\"+j+"\\"
        top_usr_list = os.listdir(p_j)

        for k in top_usr_list:
            p_k = p_j+k
            data = open(p_k,"r")
            B = json.load(data)
            # print (B["data"]["pincodes"])

            for z in B["data"]["pincodes"]:
                name = z["name"]
                registeredusers = z["registeredUsers"]
                clm_top_user["Pincode"].append(name)
                clm_top_user["Registered_user"].append(registeredusers)
                clm_top_user["State"].append(i)
                clm_top_user["Year"].append(j)
                clm_top_user["Quarter"].append(int(k.strip(".json")))

top_usr = pd.DataFrame(clm_top_user)
top_usr


# In[35]:


# table creation for top_user_state


# In[40]:


top_usr_table = '''CREATE TABLE top_user (State varchar(50), Year int, Quarter int, Pincode int, Registered_user bigint)'''

cursor.execute(top_usr_table)
conn.commit()

for index,row in top_usr.iterrows():
    insert_values = '''INSERT INTO top_user (State, Year, Quarter, Pincode, Registered_user)
                       values(%s,%s,%s,%s,%s)'''
    values = (row["State"],
              row["Year"],
              row["Quarter"],
              row["Pincode"],
              row["Registered_user"]
              )
    cursor.execute(insert_values,values)
    conn.commit()


# In[ ]:




