import psycopg2
import pandas as pd
import OptionStrategies.dbconnection as creds
from sqlalchemy import create_engine

# Set up a connection to the postgres server.
def DBConnect():
    conn_string = "host="+ creds.PGHOST +" port="+ creds.PGPORT +" dbname="+ creds.PGDATABASE +" user=" + creds.PGUSER \
    +" password="+ creds.PGPASSWORD
    conn = psycopg2.connect(conn_string)
    #print("Connected!")
    return conn

def getsqldata(sqlquery_):
    conn = DBConnect()
    data = pd.read_sql_query(sqlquery_, conn)
    conn.close()
    return data