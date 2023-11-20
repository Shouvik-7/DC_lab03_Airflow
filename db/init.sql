CREATE TABLE IF NOT EXISTS dim_animals (
    animal_id VARCHAR(7) PRIMARY KEY,
    name VARCHAR,
    dob DATE,
    sex VARCHAR(1),
    animal_type VARCHAR NOT NULL,
    breed VARCHAR,
    color VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_outcome_types (
    outcome_type_id INT PRIMARY KEY,
    outcome_type VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id VARCHAR(8) PRIMARY KEY,
    date DATE NOT NULL,
    year INT2  NOT NULL,
    month INT2  NOT NULL,
    day INT2  NOT NULL
);

CREATE TABLE IF NOT EXISTS fct_outcomes (
    outcome_id SERIAL PRIMARY KEY,
    animal_id VARCHAR(7) NOT NULL,
    date_id VARCHAR(8) NOT NULL,
    time TIME NOT NULL,
    outcome_type_id INT NOT NULL,
    outcome_subtype VARCHAR,
    is_fixed BOOL,

);