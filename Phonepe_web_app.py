#!/usr/bin/env python
# coding: utf-8
# %%

# %%


import json
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import requests
import mysql.connector

try:
    mydb = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="Admin@123",
    database="phonepe"
    )
except Exception as e:
    print(f"Unable to connect to database: {e}")

cursor = mydb.cursor()

cursor.execute("select * from aggregated_transaction")
aggr_trans_table = cursor.fetchall()
aggre_trans = pd.DataFrame(aggr_trans_table, columns = ("State", "Year", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))

cursor.execute("select * from aggregated_insurance")
aggr_ins_table = cursor.fetchall()
aggre_ins = pd.DataFrame(aggr_ins_table, columns = ("State", "Year", "Quarter", "Name", "Insurance_count", "Insurance_amount"))

cursor.execute("select * from aggregated_user")
aggr_user_table = cursor.fetchall()
aggre_user = pd.DataFrame(aggr_user_table, columns = ("State", "Year", "Quarter", "Brands", "UserByDevice_count", "Percentage"))

cursor.execute("select * from map_transaction")
map_trans_table = cursor.fetchall()
map_trans = pd.DataFrame(map_trans_table, columns = ("State", "Year", "Quarter", "District_name", "Transaction_count", "Transaction_amount"))

cursor.execute("select * from map_insurance")
map_ins_table = cursor.fetchall()
map_ins = pd.DataFrame(map_trans_table, columns = ("State", "Year", "Quarter", "District_name", "Insurance_count", "Insurance_amount"))

cursor.execute("select * from map_user")
map_user_table = cursor.fetchall()
map_user = pd.DataFrame(map_user_table,columns = ("State", "Year", "Quarter", "District_name", "Registered_user", "AppOpens"))

cursor.execute("select * from top_transaction")
top_trans_table = cursor.fetchall()
top_trans = pd.DataFrame(top_trans_table,columns = ("State", "Year", "Quarter", "Pincode", "Transaction_count", "Transaction_amount"))

cursor.execute("select * from top_user")
top_user_table = cursor.fetchall()
top_user = pd.DataFrame(top_user_table, columns = ("State", "Year", "Quarter", "Pincode", "Registered_user"))

def scenario1():
    brand= aggre_user[["Brands","UserByDevice_count"]]
    brand1= brand.groupby("Brands")["UserByDevice_count"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.pie(brand2, values= "UserByDevice_count", names= "Brands", color_discrete_sequence=px.colors.sequential.dense_r,
                       title= "Top Mobile Brands of Transaction count")
    return st.plotly_chart(fig_brands)

def scenario2():
    ins=aggre_ins[["State","Insurance_amount"]]
    ins1=ins.groupby("State")["Insurance_amount"].sum().sort_values(ascending=True)
    ins2= pd.DataFrame(ins1).reset_index().head()

    fig_ins= px.bar(ins2, x= "State", y= "Insurance_amount",title= "LOWEST INSURANCE AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_ins)

def scenario3():
    lt= aggre_trans[["State", "Transaction_amount"]]
    lt1= lt.groupby("State")["Transaction_amount"].sum().sort_values(ascending= True)
    lt2= pd.DataFrame(lt1).reset_index().head(10)

    fig_lts= px.bar(lt2, x= "State", y= "Transaction_amount",title= "LOWEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def scenario4():
    htd= map_trans[["District_name", "Transaction_amount"]]
    htd1= htd.groupby("District_name")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "District_name", title="TOP 10 DISTRICTS OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def scenario5():
    sa= map_user[["State", "AppOpens"]]
    sa1= sa.groupby("State")["AppOpens"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "State", y= "AppOpens", title="Top 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.deep_r)
    return st.plotly_chart(fig_sa)

def scenario6():
    stc= aggre_trans[["State", "Transaction_count"]]
    stc1= stc.groupby("State")["Transaction_count"].sum().sort_values(ascending=False)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "State", y= "Transaction_count", title= "STATES WITH HIGHEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

def scenario7():
    sa= map_ins[["State", "Insurance_count"]]
    sa1= sa.groupby("State")["Insurance_count"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head()

    fig_sa= px.bar(sa2, x= "State", y= "Insurance_count", title="Top 5 States With Insurance count",
                color_discrete_sequence= px.colors.sequential.deep_r)
    return st.plotly_chart(fig_sa)


st.set_page_config(page_title="PhonePe Transaction Insights", page_icon=":dragon:", layout="wide", initial_sidebar_state="auto")
st.markdown("<h1 style='text-align: center; color: violet;'>PHONEPE TRANSACTION INSIGHTS</h1>", unsafe_allow_html=True)
tab1, tab2 = st.tabs(["***HOME***","***DATA***"])

with tab1:
    st.header("PHONEPE")
    st.subheader("Digital Wallet & Online Payment App")
    st.markdown("PhonePe is an Indian digital payments and financial services app that allows users to make payments, recharge phones, pay bills, and invest in mutual funds and insurance")

with tab2:
    scenario= st.selectbox("select the question",('Top Brands Of Mobiles Used','States with Lowest Insurance Amount','States With Lowest Trasaction Amount',
                                  'Districts With Highest Transaction Amount','Top 10 States With AppOpens','States With Highest Trasaction Count','Top 5 States With Insurance count'))
    if scenario=="Top Brands Of Mobiles Used":
        scenario1()

    elif scenario=="States with Lowest Insurance Amount":
        scenario2()

    elif scenario=="States With Lowest Trasaction Amount":
        scenario3()

    elif scenario=="Districts With Highest Transaction Amount":
        scenario4()

    elif scenario=="Top 10 States With AppOpens":
        scenario5()

    elif scenario=="States With Highest Trasaction Count":
        scenario6()

    elif scenario=="Top 5 States With Insurance count":
        scenario7()

  

                                              

                                              

