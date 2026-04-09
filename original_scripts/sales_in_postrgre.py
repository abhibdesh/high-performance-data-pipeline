import datetime

import pandas as pd
import psycopg2 as psycopg2
from psycopg2 import extras

read=pd.read_csv("sales_comp2.csv")
pd.set_option('display.max_columns', None)
read["sales_line"]="SALES_LINE_VALUE"
read["country"]="COUNTRY_CODE"
read=read.drop(columns=["store_name"],axis=1)

read["date_folder"]=datetime.datetime.now()
read["sales_channel_1"]=None
read["sales_channel_2"]=None
read["sales_channel_3"]=None
read["sales_channel_4"]=None
read["sales_channel_5"]=None
read["check_updated"]=True
read["file_name"]="POC"
read["file_flag"]=None
read=read.rename(columns={"item_id_x":"legacy_item_id","store_number":"store","Quantity sold":"units"
                          ,"item_id":"item"})
print("added cols")
print(len(read))
for i in range(len(read)):
    if read.loc[i,"transaction_date"] == "11/2021":
        read.loc[i,"transaction_date"]=datetime.date(2021,11,30)
        read.loc[i,"week_ending_date"]=datetime.date(2021,12,5)
    if read.loc[i,"transaction_date"] == "12/2021":
        read.loc[i,"transaction_date"]=datetime.date(2021,12,31)
        read.loc[i,"week_ending_date"]=datetime.date(2022,1,2)
    if read.loc[i,"transaction_date"] == "01/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,1,28)
        read.loc[i,"week_ending_date"]=datetime.date(2022,2,6)
    if read.loc[i,"transaction_date"] == "02/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,2,25)
        read.loc[i,"week_ending_date"]=datetime.date(2022,3,6)
    if read.loc[i,"transaction_date"] == "03/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,3,31)
        read.loc[i,"week_ending_date"]=datetime.date(2022,4,3)
    if read.loc[i,"transaction_date"] == "04/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,4,29)
        read.loc[i,"week_ending_date"]=datetime.date(2022,5,1)
    if read.loc[i,"transaction_date"] == "05/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,5,31)
        read.loc[i,"week_ending_date"]=datetime.date(2022,6,5)
    if read.loc[i,"transaction_date"] == "06/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,6,30)
        read.loc[i,"week_ending_date"]=datetime.date(2022,7,3)
    if read.loc[i,"transaction_date"] == "07/2022":
        read.loc[i,"transaction_date"]=datetime.date(2022,7,31)
        read.loc[i,"week_ending_date"]=datetime.date(2022,8,4)

read=read.loc[:,["country","store","sales_line","item","transaction_date","sales","units","margin",
                "week_ending_date","legacy_item_id","date_folder","check_updated","sales_channel_1","sales_channel_2",
                 "sales_channel_3","sales_channel_4","sales_channel_5","file_name","file_flag"]]

print(read.columns)
print(read.dtypes)
print(read)
read=read.drop_duplicates(subset=["store","item","transaction_date",'sales','units','week_ending_date','legacy_item_id'],keep="first")
print(len(read))
read.to_csv("final_sale_post.csv",index=False)

conn = psycopg2.connect(database="DATABASE_NAME", user='DATABASE_USER', password='USER_PASSWORD', host='DATABASE_IP', port= 'DATABASE_PORT_NO')
cursor = conn.cursor()
print("for loop done")
def insertValues(conn,df,table):
    tuples=[tuple(x) for x in df.to_numpy()]
    cols=",".join(list(df.columns))
    query= "INSERT INTO %s(%s) VALUES %%s"%(table,cols)
    try:
        print("inside try")
        # UNCOMMENT THIS AND RUN IF THE TABLE IS NOT ALREADY CREATED
        # cursor.execute('''CREATE TABLE DATABASE_SCHEMA.TABLE_NAME(country VARCHAR(10), store VARCHAR(30),
        # sales_line VARCHAR(20),item VARCHAR(30), transaction_date DATE, sales DECIMAL, units DECIMAL,
        # margin DECIMAL, week_ending_date DATE,legacy_item_id VARCHAR(30), date_folder VARCHAR(30),
        # check_updated boolean, sales_channel_1 VARCHAR(20), sales_channel_2 VARCHAR(20), sales_channel_3 VARCHAR(20),
        # sales_channel_4 VARCHAR(20), sales_channel_5 VARCHAR(20), file_name VARCHAR(30) ,
        # file_flag VARCHAR(30)) PARTITION BY RANGE ( country, week_ending_date) TABLESPACE TABLE_SPACE_NAME;''')
        extras.execute_values(cursor, query, tuples, page_size=20000)
        conn.commit()
        print("DONE")
    except Exception as e:
        print(e)

insertValues(conn,read,"DATABASE_SCHEMA.TABLE_NAME")
cursor.close()
conn.close()
