import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from sqlalchemy.dialects.postgresql import insert
import psycopg2
def load_data(table_file, table_name,key, s3):
    # DB connection string specified in docker-compose
    db_url = os.environ['DB_URL']
    conn = create_engine(db_url)

    # def insert_on_conflict_nothing(table, conn, keys, data_iter):
    #     data = [dict(zip(keys, row)) for row in data_iter]
    #
    #     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
    #
    #     result = conn.execute(stmt)
    #
    #     return result.rowcount

    # load all the data in a loop
    obj = s3.Bucket('animalshelter').Object(table_file).get()
    #pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="append", index=False, method=insert_on_conflict_nothing)
    pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="replace", index=False, )
    print(table_name+" loaded")

def load_fact_data(table_file, table_name, s3):
    # DB connection string specified in docker-compose
    db_url = os.environ['DB_URL']
    conn = create_engine(db_url)

    obj = s3.Bucket('animalshelter').Object(table_file).get()
    pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="replace", index=False,)
    print(table_name + " loaded")
