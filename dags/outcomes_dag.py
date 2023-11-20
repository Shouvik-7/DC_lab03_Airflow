from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.transform import  transform_data
from etl_scripts.load import load_data, load_fact_data
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
    start_date = datetime(2023,11,20),
    schedule_interval='@daily'
) as dag:

    extract = BashOperator(

        task_id="extract",
        bash_command= f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",

    )

    extract2 = PythonOperator(

        task_id="extract2",
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
            'table_file': 'dim_animals.csv',
            'table_name': 'dim_animals',
            'key':'animal_id',
            's3' : s3
        }
    )

    load_dates_dim = PythonOperator(

        task_id="load_dates_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': 'dim_dates.csv',
            'table_name': 'dim_dates',
            'key': 'date_id',
            's3' : s3
        }
    )

    load_outcomes_types_dim = PythonOperator(

        task_id="load_outcome_types_dim",
        python_callable=load_data,
        op_kwargs={
            'table_file': 'dim_outcome_types.csv',
            'table_name': 'dim_outcome_types',
            'key': 'outcome_type_id',
            's3' : s3
        }
    )

    load_outcomes_fct = PythonOperator(

        task_id="load_outcomes_fct",
        python_callable=load_fact_data,
        op_kwargs={
            'table_file': 'fct_outcomes.csv',
            'table_name': 'fct_outcomes',
            's3' : s3
        }
    )

    extract2 >> transform >> [load_animals_dim, load_dates_dim, load_outcomes_types_dim] >> load_outcomes_fct