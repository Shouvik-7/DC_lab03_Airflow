import boto3
import pandas as pd
import requests
import json
import os
from pathlib import Path

def extract_data(source_csv, s3):
    response_API = requests.get(source_csv)
    df = pd.read_json(response_API.text)
    # target_dir = Path(target)
    # target_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv('shelter.csv')

    s3.Bucket('animalshelter').upload_file(Filename='shelter.csv',Key='shelter.csv')

    # for obj in s3.Bucket('animalshelter').objects.all():
    #     print(obj)
    #
    # obj = s3.Bucket('animalshelter').Object('shelter.csv').get()
    # df2 = pd.read_csv(obj['Body'], index_col=0)
    # print(df2.head())