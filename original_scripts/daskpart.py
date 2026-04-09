import dask.dataframe as dd
import psycopg2
from psycopg2 import extras
from distributed import LocalCluster, Client

def insert(dataframe):
    print("in insert function")
    conn = psycopg2.connect(database="DATABASE_NAME", 
        user='DATABASE_USER', 
        password='DATABASE_PASSWORD',host='DATABASE_HOST_IP', port='DATABASE_PORT_NO')
    cursor = conn.cursor()
    ddf_columns=list(dataframe.columns)
    columns = ",".join(ddf_columns)
    print(columns)
    values = "VALUES({})".format(",".join(["%s" for _ in ddf_columns]))
    print(values)
    insert_stmt = "INSERT INTO {} ({}) {}".format('sales_data_larger_file', columns,values)
    psycopg2.extras.execute_batch(cursor, insert_stmt, dataframe.values.compute(), page_size=2000)
    conn.commit()

if __name__=="__main__":
    def run(n):
        n.map_partitions(insert).compute()

    df = dd.read_csv("sales_transaction5000000.txt", sep="\t")
    df = df.partitions[3]
    df = df.repartition(npartitions=100)
    print(type(df))
    cols = {"Store name": "store_name", "Store Number": "store_number", "Quantity": "quantity", "Date": "date",
            "Product Number": "product_number", "Sales Value": "sales_value"}
    ddf = df.rename(columns=cols)
    ddf['sales_channel']="offline"
    print(len(ddf))
    print(ddf)
    cluster = LocalCluster(n_workers=4)
    client = Client(cluster)
    print(client.dashboard_link)
    print(client.dashboard_link)
    print(client.dashboard_link)
    print(client)
    future = client.submit(insert,ddf)
    future.result()
    print("Inserting...")