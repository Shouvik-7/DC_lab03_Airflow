CREATE TABLE outcome_dim(
    outcome_type_id SERIAL PRIMARY KEY,
    outcome_type VARCHAR
);

CREATE TABLE animal_dim(
    animal_id VARCHAR PRIMARY KEY,
    animal_name VARCHAR,
    dob VARCHAR,
    animal_type VARCHAR,
    breed VARCHAR,
    color VARCHAR,
    sex VARCHAR,
    unix_dob BIGINT
);

CREATE TABLE date_dim(
    date_id SERIAL PRIMARY KEY,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    second INTEGER,
    unix _date BIGINT
);
CREATE TABLE outcome_fct(
    outcome_id BIGSERIAL PRIMARY KEY,
    outcome_type_id integer references outcome_dim,
    date_id integer references date_dim,
    animal_id VARCHAR references animal_dim,
    gender_upon_outcome VARCHAR,
    outcome_subtype VARCHAR
);


