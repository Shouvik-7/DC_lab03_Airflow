import boto3
import pandas as pd
import requests
import json
import os


s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='AKIA46ITI7LDQLIPG2MV',
    aws_secret_access_key='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'
)


def extract_data(source_csv):
    response_API = requests.get(source_csv)
    df = pd.read_json(response_API.text)
    return df

scsv = 'https://data.austintexas.gov/resource/9t4d-g238.json'

df = extract_data(scsv)
df.to_csv('shelter.csv')

s3.Bucket('animalshelter').upload_file(Filename='shelter.csv',Key='shelter1.csv')

for obj in s3.Bucket('animalshelter').objects.all():
    print(obj)

obj = s3.Bucket('animalshelter').Object('shelter.csv').get()
df2 = pd.read_csv(obj['Body'], index_col=0)
print(df2.head())



# postgres
# postgres16
# shelter_db_1
# database-1.cvwmlpdligxr.us-east-2.rds.amazonaws.com