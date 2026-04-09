from psycopg2 import connect
from psycopg2.extras import execute_values
from db.py import get_connection
import os

def insert_to_db(df):
    conn = get_connection()
    
    cursor = conn.cursor()
    
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))
    query = f"INSERT INTO table_name ({cols}) VALUES %s"
    
    execute_values(cursor, query, tuples, page_size=20000)
    conn.commit()
    
    cursor.close()
    conn.close()
    
    print("Inserted successfully")