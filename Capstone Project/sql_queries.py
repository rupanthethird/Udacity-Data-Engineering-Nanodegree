# DROP TABLES

immigration_table_drop = "DROP table if exists immigration"
demographic_table_drop = "DROP table if exists demographic"
crime_table_drop = "DROP table if exists crime"
date_table_drop = "DROP table if exists date"
temperature_table_drop = "DROP table if exists temperature"

# CREATE TABLES

immigration_table_create = ("""
CREATE TABLE IF NOT EXISTS immigration(
id int PRIMARY KEY,
created_date date,
admission_no int,
admitted_date date,
departure_date date,
arrival_date date,
port_name varchar,
airport_name varchar,
entry_state_id varchar,
entry_state_name varchar,
entry_county varchar,
airline varchar,
flight_no varchar,
gender varchar,
birth_year int,
age int,
origin_country_name varchar,
resident_country_name varchar,
visa_type bigint,
visa_type_name varchar,
state_code varchar,
current_state_name varchar
);
""")

demographic_table_create = ("""
CREATE TABLE IF NOT EXISTS demographic(
state varchar PRIMARY KEY,
state_code varchar NOT NULL,
population int NOT NULL,
median_age float NOT NULL,
male_population int NOT NULL,
female_population int NOT NULL,
number_of_veterans int NOT NULL,
foreign_born int NOT NULL,
average_household_size float NOT NULL,
AmericanIndian_and_AlaskaNative_population int NOT NULL,
Asian_population int NOT NULL,
Black_or_AfricanAmerican_population int NOT NULL,
Hispanic_or_Latino_population int NOT NULL,
White_population int NOT NULL
);
""")

crime_table_create = ("""
CREATE TABLE IF NOT EXISTS crime(
county varchar,
state varchar,
category varchar,
total_crime int NOT NULL,
violent_crime int NOT NULL,
murder_and_nonnegligent_manslaughter int NOT NULL,
robbery int NOT NULL,
aggravated_assault int NOT NULL,
property_crime int NOT NULL,
burglary int NOT NULL,
larceny_theft int NOT NULL,
motor_vehicle_theft int NOT NULL,
arson3 int NOT NULL,
rape int NOT NULL,
crime_rate float NOT NULL,
violent_crime_rate float NOT NULL,
property_crime_rate float NOT NULL,
PRIMARY KEY(county, state)
);
""")

date_table_create = ("""
CREATE TABLE IF NOT EXISTS date(
created_date date PRIMARY KEY,
day int NOT NULL,
week int NOT NULL,
month int NOT NULL,
year int NOT NULL,
weekday int NOT NULL
);
""")

temperature_table_create = ("""
CREATE TABLE IF NOT EXISTS temperature(
county varchar,
state varchar,
avg_temperature float NOT NULL,
max_of_monthly_avg_temperature float NOT NULL,
min_of_monthly_avg_temperature float NOT NULL,
PRIMARY KEY(county, state)
);
""")


# FINAL TABLES

immigration_table_insert = ("""
INSERT INTO immigration(
id,
created_date,
admission_no,
admitted_date,
departure_date,
arrival_date,
port_name,
airport_name,
entry_state_id,
entry_state_name,
entry_county,
airline,
flight_no,
gender,
birth_year,
age,
origin_country_name,
resident_country_name,
visa_type,
visa_type_name,
state_code,
current_state_name
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

demographic_table_insert = ("""
INSERT INTO demographic(
state,
state_code,
population,
median_age,
male_population,
female_population,
number_of_veterans,
foreign_born,
average_household_size,
AmericanIndian_and_AlaskaNative_population,
Asian_population,
Black_or_AfricanAmerican_population,
Hispanic_or_Latino_population,
White_population
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(state) DO NOTHING
""")

crime_table_insert = ("""
INSERT INTO crime(
county,
state,
category,
total_crime,
violent_crime,
murder_and_nonnegligent_manslaughter,
robbery,
aggravated_assault,
property_crime,
burglary,
larceny_theft,
motor_vehicle_theft,
arson3,
rape,
crime_rate,
violent_crime_rate,
property_crime_rate
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(county, state) DO NOTHING
""")

date_table_insert = ("""
INSERT INTO date(
created_date,
day,
week,
month,
year,
weekday
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT(created_date) DO NOTHING
""")

temperature_table_insert = ("""
INSERT INTO temperature(
county,
state,
avg_temperature,
max_of_monthly_avg_temperature,
min_of_monthly_avg_temperature
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(county, state) DO NOTHING
""")


# Data Quality Checks

immigration_table_check = ("""
SELECT COUNT(*) FROM immigration WHERE id is null
""")

demographic_table_check = ("""
SELECT COUNT(*) FROM demographic WHERE state is null
""")

crime_table_check = ("""
SELECT COUNT(*) FROM crime WHERE state is null OR county is null
""")

date_table_check = ("""
SELECT COUNT(*) FROM date WHERE created_date is null OR created_date is null
""")

temperature_table_check = ("""
SELECT COUNT(*) FROM temperature WHERE state is null OR county is null
""")


# QUERY LISTS

create_table_queries = [immigration_table_create, demographic_table_create, crime_table_create, date_table_create, temperature_table_create]
drop_table_queries = [immigration_table_drop, demographic_table_drop, crime_table_drop, date_table_drop, temperature_table_drop]
# data_check_queries = [immigration_table_check, demographic_table_check, crime_table_check, date_table_check, temperature_table_check]

data_check_queries = {
    'immigration_table_check': immigration_table_check, 
    'demographic_table_check': demographic_table_check, 
    'crime_table_check': crime_table_check,
    'date_table_check': date_table_check,
    'temperature_table_check': temperature_table_check
}