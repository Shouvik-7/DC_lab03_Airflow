import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from sqlalchemy.dialects.postgresql import insert
import psycopg2

def load_data(table_file,table_name,s3):
    db_url = os.environ['DB_URL']
    conn = create_engine(db_url)
    obj = s3.Bucket('animalshelter').Object(table_file).get()
    #new_data = pd.read_csv(obj['Body'], index_col=0)
    pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="replace", index=False, )
    print(table_name + " loaded")

    # animal_dim.to_sql("animal_dim", conn, if_exists="append", index=False)
    # date_dim.to_sql("date_dim", conn, if_exists="append", index=False)
    # outcome_dim.to_sql("outcome_dim", conn, if_exists="append", index=False)
    # date_df = pd.read_sql_table('date_dim',conn)
    # outcome_df = pd.read_sql_table('outcome_dim', conn)
    # return date_df, outcome_df

#
# def load_data(table_file, table_name,key, s3):
#     # DB connection string specified in docker-compose
#     db_url = os.environ['DB_URL']
#     conn = create_engine(db_url)
#
#     # def insert_on_conflict_nothing(table, conn, keys, data_iter):
#     #     data = [dict(zip(keys, row)) for row in data_iter]
#     #
#     #     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
#     #
#     #     result = conn.execute(stmt)
#     #
#     #     return result.rowcount
#
#     # load all the data in a loop
#     obj = s3.Bucket('animalshelter').Object(table_file).get()
#     #pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="append", index=False, method=insert_on_conflict_nothing)
#     pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="replace", index=False, )
#     print(table_name+" loaded")

# def load_fact_data(table_file, table_name, s3):
#     # DB connection string specified in docker-compose
#     db_url = os.environ['DB_URL']
#     conn = create_engine(db_url)
#
#     obj = s3.Bucket('animalshelter').Object(table_file).get()
#     pd.read_csv(obj['Body'], index_col=0).to_sql(table_name, conn, if_exists="replace", index=False,)
#     print(table_name + " loaded")



# def create_outcome_fct(data, date_df, outcome_df):
#     data['date_id'] = date_df['date_id']
#     outcome_df = outcome_df.set_index('outcome_type')
#     outcome_map = outcome_df.to_dict()['outcome_type_id']
#     data['outcome_type_id'] = data['outcome_type'].replace(outcome_map)
#     return data

# def load_outcome_fct(data):
#     # DB connection string specified in docker-compose
#     db_url = os.environ['DB_URL']
#     conn = create_engine(db_url)
#     date_df = pd.read_sql_table('date_dim',conn)
#     outcome_df = pd.read_sql_table('outcome_dim', conn)
#
#     data['date_id'] = date_df['date_id']
#     outcome_df = outcome_df.set_index('outcome_type')
#     outcome_map = outcome_df.to_dict()['outcome_type_id']
#     data['outcome_type_id'] = data['outcome_type'].replace(outcome_map)
#
#     data2 = data[['animal_id','outcome_type_id','date_id','outcome_subtype','gender_upon_outcome']]
#     #data2['outcome_id'] = [i for i in range(len(data2))]
#
#     data2.to_sql('outcome_fct',conn, if_exists="replace", index=False)