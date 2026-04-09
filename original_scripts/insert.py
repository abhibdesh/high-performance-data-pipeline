import os
import pandas as pd
import psycopg2
import datetime
from dask_gateway import Gateway
from distributed.utils import LoopRunner, format_bytes
from dask import utils
# from utils import format_byte
import psycopg2.extras
# from pymongo import MongoClient, InsertOne
# # from testdask import get_dask
from flask_pymongo import MongoClient
from pymongo import InsertOne
from sqlalchemy.dialects.postgresql import psycopg2


def get_date_info(Date, parms):
    if parms == 1:
        dt = Date - datetime.timedelta(days=Date.weekday())
        return dt
    elif parms == 2:
        dt = Date - datetime.timedelta(days=Date.weekday()) + datetime.timedelta(6)
        return dt
    elif parms == 3:
        return int(Date.strftime("%V"))
    else:
        return Date.year
def queryy(x, partition_info=None):
    print("weekly - inside query")
    print(partition_info.get("number"))
    print(x)
    if partition_info.get('number', False) >= 0 and not x.empty:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(database='DATABASE_NAME',
                                user='DATABASE_USER',
                                password='DATABASE_PASSWORD',
                                host='DATABASE_HOST_IP',
                                port='DATABASE_PORT_NO')

        cur = conn.cursor()
        print("weekly ka query")
        print("inserting")
        df_columns = list(x.columns)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format('schema.table_name', columns,
                                                      values)
        psycopg2.extras.execute_batch(cur, insert_stmt, x.values,page_size=2000)
        conn.commit()
        print("committed a batch for weekly table")
    return None


item_master_dask['PRICE'] = item_master_dask['PRICE'].map_partitions(clean_currency).astype(float)
if __name__ == '__main__':
    def run_postgres(n):
        print("inside weekly run")
        print(type(n))
        print(n)

        cur = conn.cursor()
        a = n.map_partitions(queryy).compute()
        conn.commit()

    import dask.dataframe as dd
    from dask.distributed import Client, LocalCluster

    df = pd.read_csv('fru_new', sep='\t', low_memory=False, header=None, skiprows=1,
                     names=["Date", "Store Number", "Store Name", "Product Number", "quantity",
                            "Sales Value"]
                     )
    print("read df succesfully")
    print(df.head())
    print("len of the dataframe")
    print(len(df))


    df['Date'] = df['Date'].astype('M8[us]')
    df['week_ending_date'] = df['Date'].apply(lambda x: get_date_info(x, 2))
    df['week_of_year'] = df['Date'].apply(lambda x: get_date_info(x, 3)).astype(int)
    df['year'] = df['Date'].apply(lambda x: get_date_info(x, 4)).astype(int)

    df = df.rename(columns={
        "Date": "transaction_date",
        "Store Number": "outlet_id",
        "Store Sign": "store",
        "Store Name": "store",
        "Product Number": "item",
        "quantity": "units",
        "Sales Value": "sales"
    })
    #cleaning sales ka data
    df['sales'] = df['sales'].astype(str)
    df['sales'] = df['sales'].apply(lambda x: x.replace(".", ""))
    df['sales'] = df['sales'].apply(lambda x: x.replace(",", "."))
    #type casting items, sales, units
    df['item'] = df['item'].astype(str)
    df['sales'] = df['sales'].astype(float)
    df['units'] = df['units'].astype(float)
    #hardcoding values
    df['country'] = 'IT'
    df['sales_line'] = 'SALES_LINE'
    df['margin'] = 0.0
    df['process'] = False
    df['date_folder'] = "31-12-2021"
    df['date_folder'] = df['date_folder'].astype('M8[us]')
    df['check_updated'] = True

    df_w = df[['country','store','sales_line', 'item', 'transaction_date', 'sales',  'units', 'margin',
               'week_ending_date', 'date_folder', 'check_updated']]
    print(df_w.to_string())
    print(df_w.dtypes)
    # exit()
    gateway_url = "local gateway url"
    gateway = Gateway(gateway_url)
    cluster = gateway.new_cluster()
    cluster.scale(1)
    cluster.adapt(minimum=2, maximum=4)

    client = Client(cluster)
    print(client.dashboard_link)
    print(client.dashboard_link)
    print(client.dashboard_link)
    #
    ddf_weekly = dd.from_pandas(df_w, npartitions=100)
    print(ddf_weekly)
    print(ddf_weekly.head())

    weekly_df = ddf_weekly.persist()

    print(type(weekly_df))
    try:
        print(client)
        future = client.submit(run_postgres, weekly_df)
        future.result()
        print("done loading the data, check count in postgress :)")
        # print(a)
    except Exception as e:
        print(e)