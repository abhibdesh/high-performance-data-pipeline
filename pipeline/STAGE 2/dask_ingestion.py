import dask.dataframe as dd
import psycopg2
from psycopg2 import extras
from distributed import LocalCluster, Client
import os

def insert(dataframe):
    conn = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cursor = conn.cursor()

    cols = ",".join(list(dataframe.columns))
    values = "VALUES({})".format(",".join(["%s"] * len(dataframe.columns)))
    insert_stmt = f"INSERT INTO table_name ({cols}) {values}"

    extras.execute_batch(cursor, insert_stmt, dataframe.values.compute(), page_size=2000)
    conn.commit()

if __name__ == "__main__":
    df = dd.read_csv("data/sample_sales.csv")

    df["sales_channel"] = "offline"

    cluster = LocalCluster(n_workers=4)
    client = Client(cluster)

    future = client.submit(insert, df)
    future.result()