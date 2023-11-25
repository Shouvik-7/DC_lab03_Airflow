import pandas as pd
import numpy as np
from collections import OrderedDict
from pathlib import Path


def get_sex(x):
    li = str(x).split()
    if len(li) > 1:
        return li[1]
    else:
        return "Unknown"


def get_gender_upon_outcome(x):
    li = str(x).split()
    if len(li) > 1:
        return li[0]
    else:
        return "Unknown"


def get_animal_dim(data):
    animal_dim = data[['animal_id', 'animal_name', 'dob', 'unix_dob', 'animal_type', 'breed', 'color', 'sex']]
    animal_dim = animal_dim.drop_duplicates(subset=['animal_id'], keep='first')
    return animal_dim


def get_date_dim(data):
    date_dim = data[['ts', 'unix_date']]
    date_dim['ts'] = pd.to_datetime(date_dim['ts'])
    date_dim['year'] = date_dim['ts'].apply(lambda x: int(x.year))
    date_dim['month'] = date_dim['ts'].apply(lambda x: int(x.month))
    date_dim['day'] = date_dim['ts'].apply(lambda x: int(x.day))
    date_dim['hour'] = date_dim['ts'].apply(lambda x: int(x.hour))
    date_dim['minute'] = date_dim['ts'].apply(lambda x: int(x.minute))
    date_dim['second'] = date_dim['ts'].apply(lambda x: int(x.second))
    date_dim = date_dim[['unix_date', 'year', 'month', 'day', 'hour', 'minute', 'second']]
    return date_dim


def get_outcome_dim(data):
    data['outcome_type'] = data['outcome_type'].apply(lambda x: str(x))
    outcome_type_li = data['outcome_type'].unique()
    outcome_type_dim = pd.DataFrame()
    outcome_type_dim['outcome_type'] = outcome_type_li
    return outcome_type_dim

def create_outcome_fct(data, date_df, outcome_df):
    data['date_id'] = date_df['date_id']
    outcome_df = outcome_df.set_index('outcome_type')
    outcome_map = outcome_df.to_dict()['outcome_type_id']
    data['outcome_type_id'] = data['outcome_type'].replace(outcome_map)
    return data

def prep_data(data):
    data.rename(columns={'animal_id': 'animal_id', 'name': 'animal_name', 'datetime': 'ts', 'date_of_birth': 'dob',
                         'outcome_type': 'outcome_type', 'outcome_subtype': 'outcome_subtype',
                         'animal_type': 'animal_type', 'sex_upon_outcome': 'sex_upon', 'age_upon_outcome': 'age',
                         'breed': 'breed', 'color': 'color'}, inplace=True)

    data['sex'] = data['sex_upon'].apply(get_sex)  # fetch sex from sex_upon_outcome
    data['gender_upon_outcome'] = data['sex_upon'].apply(get_gender_upon_outcome)  # fetch gender_upon_outcome from sex_upon_outcome
    data['animal_name'] = data['animal_name'].apply(lambda x: str(x).replace('*', ''))  # remove * from names
    data['dob'] = pd.to_datetime(data['dob'])
    data['ts'] = pd.to_datetime(data['ts'])
    data['unix_dob'] = data['dob'].apply(lambda x: pd.Timestamp(x).timestamp())
    data['unix_date'] = data['ts'].apply(lambda x: pd.Timestamp(x).timestamp())
    data = data[['animal_id', 'animal_name', 'ts', 'unix_date', 'dob', 'unix_dob', 'outcome_type', 'outcome_subtype',
                 'animal_type', 'breed', 'color', 'sex', 'gender_upon_outcome']]

    animal_dim = get_animal_dim(data)
    date_dim = get_date_dim(data)
    date_dim['date_id'] = list(range(1, len(date_dim) + 1))
    outcome_dim = get_outcome_dim(data)
    outcome_dim['outcome_type_id'] = list(range(1, len(outcome_dim) + 1))
    return data, animal_dim, date_dim, outcome_dim


def transform_data(filename, s3):


    obj = s3.Bucket('animalshelter').Object(filename).get()
    new_data = pd.read_csv(obj['Body'], index_col=0)
    data, animal_dim, date_dim, outcome_dim = prep_data(new_data)
    outcome_fct = create_outcome_fct(data, date_dim, outcome_dim)
    outcome_fct = outcome_fct[['animal_id', 'date_id', 'outcome_type_id', 'gender_upon_outcome', 'outcome_subtype']]
    outcome_fct['outcome_id'] = list(range(1, len(outcome_fct) + 1))
    animal_dim.to_csv('animal_dim.csv')
    date_dim.to_csv('date_dim.csv')
    outcome_dim.to_csv('outcome_dim.csv')
    outcome_fct.to_csv('outcome_fct.csv')

    s3.Bucket('animalshelter').upload_file(Filename='animal_dim.csv', Key='animal_dim.csv')
    s3.Bucket('animalshelter').upload_file(Filename='date_dim.csv', Key='date_dim.csv')
    s3.Bucket('animalshelter').upload_file(Filename='outcome_dim.csv', Key='outcome_dim.csv')
    s3.Bucket('animalshelter').upload_file(Filename='outcome_fct.csv', Key='outcome_fct.csv')
