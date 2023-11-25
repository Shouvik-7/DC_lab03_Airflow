from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.transform import  transform_data
from etl_scripts.load import load_data
from etl_scripts.extract import  extract_data
import boto3


SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv?date=20231118&accessType=DOWNLOAD'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR+'/outcomes_{{ ds }}.csv'
PQ_TARGET_FILE = AIRFLOW_HOME + '/data/{{ ds }}/processed'

SOURCE_CSV = 'https://data.austintexas.gov/resource/9t4d-g238.json'
FILE_NAME = 'shelter.csv'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']


s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)



with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023,11,24),
    schedule_interval='@daily'
) as dag:


    extract = PythonOperator(

        task_id="extract",
        python_callable=extract_data,
        op_kwargs={
            'source_csv': SOURCE_CSV,
            's3':s3
        }
    )

    transform = PythonOperator(

        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'filename': FILE_NAME,
            's3':s3
        }
    )

    load_animals_dim = PythonOperator(

        task_id="load_animals_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': 'animal_dim.csv',
            'table_name': 'animal_dim',
            's3' : s3
        }
    )

    load_dates_dim = PythonOperator(

        task_id="load_dates_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': 'date_dim.csv',
            'table_name': 'date_dim',
            's3' : s3
        }
    )

    load_outcomes_types_dim = PythonOperator(

        task_id="load_outcome_types_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': 'outcome_dim.csv',
            'table_name': 'outcome_dim',
            's3' : s3
        }
    )

    load_outcomes_fct = PythonOperator(

        task_id="load_outcomes_fct",
        python_callable=load_data,
        op_kwargs={
            'table_file': 'outcome_fct.csv',
            'table_name': 'outcome_fct',
            's3' : s3
        }
    )

    extract >> transform >> [load_animals_dim, load_dates_dim, load_outcomes_types_dim] >> load_outcomes_fct