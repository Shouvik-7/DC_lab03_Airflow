import pandas as pd
import numpy as np
from collections import OrderedDict
from pathlib import Path
# creating the global mapping for outcome types
outcomes_map = {'Rto-Adopt': 1,
                'Adoption': 2,
                'Euthanasia': 3,
                'Transfer': 4,
                'Return to Owner': 5,
                'Died': 6,
                'Disposal': 7,
                'Missing':8,
                'Relocate':9,
                'N/A':10,
                'Stolen':11}


def transform_data(filename, s3):


    obj = s3.Bucket('animalshelter').Object(filename).get()
    new_data = pd.read_csv(obj['Body'], index_col=0)
    #new_data = pd.read_csv(source_csv)
    new_data = prep_data(new_data)

    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)

    fct_outcomes = prep_outcomes_fct(new_data)


    dim_animal.to_csv('dim_animals.csv')
    dim_dates.to_csv('dim_dates.csv')
    dim_outcome_types.to_csv('dim_outcome_types.csv')
    fct_outcomes.to_csv('fct_outcomes.csv')

    s3.Bucket('animalshelter').upload_file(Filename='dim_animals.csv', Key='dim_animals.csv')
    s3.Bucket('animalshelter').upload_file(Filename='dim_dates.csv', Key='dim_dates.csv')
    s3.Bucket('animalshelter').upload_file(Filename='dim_outcome_types.csv', Key='dim_outcome_types.csv')
    s3.Bucket('animalshelter').upload_file(Filename='fct_outcomes.csv', Key='fct_outcomes.csv')


def prep_data(data):
    # remove stars from animal names. Need regex=False so that * isn't read as regex
    data['name'] = data['name'].str.replace("*", "", regex=False)

    # separate the "sex upon outcome" column into property of an animal (male or female)
    # and property of an outcome (was the animal spayed/neutered at the shelter or not)
    data['sex'] = data['sex_upon_outcome'].replace({"Neutered Male": "M",
                                                    "Intact Male": "M",
                                                    "Intact Female": "F",
                                                    "Spayed Female": "F",
                                                    "Unknown": np.nan})

    data['is_fixed'] = data['sex_upon_outcome'].replace({"Neutered Male": True,
                                                         "Intact Male": False,
                                                         "Intact Female": False,
                                                         "Spayed Female": True,
                                                         "Unknown": np.nan})

    # prepare the data table for introducing the date dimension
    # we'll use condensed date as the key, e.g. '20231021'
    # time can be a separate dimension, but here we'll keep it as a field
    data['ts'] = pd.to_datetime(data.datetime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time'] = data.ts.dt.time

    # prepare the data table for introducing the outcome type dimension:
    # introduce keys for the outcomes
    data['outcome_type_id'] = data['outcome_type'].fillna('N/A')
    data['outcome_type_id'] = data['outcome_type_id'].replace(outcomes_map)

    return data


def prep_animal_dim(data):
    # extract columns only relevant to animal dim
    animal_dim = data[['animal_id', 'name', 'date_of_birth', 'sex', 'animal_type', 'breed', 'color']]

    # rename the columns to agree with the DB tables
    animal_dim.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']

    # drop duplicate animal records
    return animal_dim.drop_duplicates()


def prep_date_dim(data):
    # use string representation as a key
    # separate out year, month, and day
    dates_dim = pd.DataFrame({
        'date_id': data.ts.dt.strftime('%Y%m%d'),
        'date': data.ts.dt.date,
        'year': data.ts.dt.year,
        'month': data.ts.dt.month,
        'day': data.ts.dt.day,
    })
    return dates_dim.drop_duplicates()


def prep_outcome_types_dim(data):
    # map outcome string values to keys
    outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()

    # keep only the necessary fields
    outcome_types_dim.columns = ['outcome_type', 'outcome_type_id']
    outcome_types_dim = outcome_types_dim.dropna()
    return outcome_types_dim


def prep_outcomes_fct(data):
    # pick the necessary columns and rename
    outcomes_fct = data[["animal_id", 'date_id', 'time', 'outcome_type_id', 'outcome_subtype', 'is_fixed']]
    return outcomes_fct