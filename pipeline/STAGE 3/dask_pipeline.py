import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import psycopg2
from psycopg2.extras import execute_batch
import os
from .etl import etl

def insert_partition(df):
    conn = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

    cursor = conn.cursor()

    cols = ",".join(df.columns)
    query = f"INSERT INTO table_name ({cols}) VALUES %s"

    data = [tuple(x) for x in df.to_numpy()]

    execute_batch(cursor, query, data, page_size=2000)

    conn.commit()
    cursor.close()
    conn.close()


def run():
    cluster = LocalCluster(n_workers=4)
    client = Client(cluster)

    df = dd.read_csv("data/sample_sales.csv")

    df = etl(df)

    df.map_partitions(insert_partition).compute()