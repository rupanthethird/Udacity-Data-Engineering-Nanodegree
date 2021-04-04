import pandas as pd
import re
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
from datetime import datetime, timedelta
from sql_queries import *


def create_spark_cluster():
    """Create a spark cluster to process immigration file"""
    spark = SparkSession.builder.config(
        "spark.jars.packages",
        "saurfang:spark-sas7bdat:2.0.0-s_2.11"). enableHiveSupport().getOrCreate()
    return spark


def read_file():
    """Read in a file which is used for multiple tables"""
    df_label = pd.read_csv('uscities.csv')
    return df_label


def process_immigration_file(cluster, file):
    """Read immigration file and related files, process them, and convert to pandas dataframes"""
    # Read in immigration and related files
    immigration_data = cluster.read.parquet("sas_data")
    df_airport = pd.read_csv('airport-codes_csv.csv')
    with open("./I94_SAS_Labels_Descriptions.SAS") as f:
        textfile = f.readlines()

    def convert_datetime(x):
        """Convert original date column to datetime"""
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except BaseException:
            return None
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())

    # For those columns that have many missing values or that are not important for this project, deleted them,\
    # change column data types, and rename column names to make it more
    # understandable
    immigration_data = immigration_data.drop("i94mode")\
        .drop('occup')\
        .drop('entdepu')\
        .drop("insnum")\
        .drop('entdepa')\
        .drop('matflag')\
        .drop('count')\
        .drop('visatype')\
        .drop('visapost')\
        .drop('entdepd')\
        .drop('year')\
        .drop('month')\
        .withColumn("id", col("cicid").cast("integer")).drop("cicid") \
        .withColumn("year", col("i94yr").cast("integer")).drop("i94yr") \
        .withColumn("month", col("i94mon").cast("integer")).drop("i94mon") \
        .withColumn("origin_country", col("i94cit").cast("bigint")).drop("i94cit") \
        .withColumn("resident_country", col("i94res").cast("bigint")).drop("i94res") \
        .withColumnRenamed("i94port", "port_of_entry")\
        .withColumnRenamed("i94addr", "state_code")\
        .withColumn("birth_year", col("biryear").cast("integer")).drop("biryear") \
        .withColumn("visa_type", col("i94visa").cast("bigint")).drop("i94visa") \
        .withColumn("age", col("i94bir").cast("integer")).drop("i94bir") \
        .withColumn("admission_no", col("admnum").cast("integer")).drop("admnum") \
        .withColumnRenamed("fltno", "flight_no")\
        .withColumn('admitted_date', F.to_date(F.unix_timestamp(immigration_data.dtaddto, 'MMddyyyy').cast('timestamp'))).drop("dtaddto")\
        .withColumn('created_date', F.to_date(F.unix_timestamp(immigration_data.dtadfile, 'yyyyMMdd').cast('timestamp'))).drop("dtadfile")\
        .withColumn("departure_date", udf_datetime_from_sas("depdate")).drop("depdate")\
        .withColumn("arrival_date", udf_datetime_from_sas("arrdate")).drop("arrdate")

    # Delete null values
    immigration_data = immigration_data.dropna("any")

    # In order to add label columns from other tables to Immigration table for acquiring code lables,\
    # format I94_SAS_Labels_Descriptions.SAS and uscities.csv file and convert
    # to pyspark tables to join Immigration table.
    sqlContext = SQLContext(cluster)

    def equivalent_type(f):
        if f == 'datetime64[ns]':
            return TimestampType()
        elif f == 'int64':
            return LongType()
        elif f == 'int32':
            return IntegerType()
        elif f == 'float64':
            return FloatType()
        else:
            return StringType()

    def define_structure(string, format_type):
        try:
            typo = equivalent_type(format_type)
        except BaseException:
            typo = StringType()
        return StructField(string, typo)

    def pandas_to_spark(pandas_df):
        columns = list(pandas_df.columns)
        types = list(pandas_df.dtypes)
        struct_list = []
        for column, typo in zip(columns, types):
            struct_list.append(define_structure(column, typo))
        p_schema = StructType(struct_list)
        return sqlContext.createDataFrame(pandas_df, p_schema)

    # Create and format label tables for ports, countries, and visa
    def process_text_file(file):
        """Convert text file into a dataframe format"""
        df = list(map(lambda x: x.strip(), file))
        df = [re.sub(r'\t', '', x) for x in df]
        df = [x.replace('=', ',') for x in df]
        df = [x.replace('\'', '') for x in df]
        df = pd.DataFrame(df)
        df = df[0].str.split(",", expand=True)
        return df[[0, 1]]

    ports = textfile[302:962]
    df_ports = process_text_file(ports)
    df_ports = df_ports.rename(columns={0: 'entry_port_code', 1: 'port_name'})

    countries = textfile[10:298]
    df_countries = process_text_file(countries)
    df_countries[0] = df_countries[0].astype(int)
    df_countries_origin = df_countries.rename(
        columns={0: 'country_code', 1: 'origin_country_name'})
    df_countries_resident = df_countries.rename(
        columns={0: 'country_code', 1: 'resident_country_name'})

    visa = textfile[1046:1049]
    df_visa = process_text_file(visa)
    df_visa[0] = df_visa[0].astype(int)
    df_visa = df_visa.rename(
        columns={
            0: 'visa_type_code',
            1: 'visa_type_name'})

    # Format df_label table to join state names
    df_label_state = file[['state_name', 'city', 'state_id', 'county_name']]
    df_label_state = pd.DataFrame(df_label_state.groupby(
        ['state_id']).agg({'state_name': 'max'})).reset_index()
    df_label_state['state_name'] = df_label_state['state_name'].str.upper(
    ).str.strip()
    df_label_current = df_label_state.rename(
        columns={'state_name': 'current_state_name'})
    df_label_entry = df_label_state.rename(
        columns={'state_name': 'entry_state_name'})

    # Clean airport table for joining with Immigration table
    df_airport = df_airport.rename(
        columns={
            'name': 'airport_name',
            'iso_region': 'entry_state_id',
            'municipality': 'entry_county'})
    df_airport = df_airport[df_airport['entry_state_id'].str.contains('US-')]
    df_airport['entry_state_id'] = [
        x.replace('US-', '') for x in df_airport['entry_state_id']]
    df_airport = df_airport[['local_code',
                             'airport_name', 'entry_state_id', 'entry_county']]

    # Convert pandas dataframes to pyspark dataframes for joining with
    # Immigration table
    dfs = [
        df_ports,
        df_countries_origin,
        df_countries_resident,
        df_visa,
        df_airport,
        df_label_current,
        df_label_entry]
    dfs_new = [pandas_to_spark(file) for file in dfs]
    df_ports_for_join, df_countries_origin_for_join, df_countries_resident_for_join, \
        df_visa_for_join, df_airport_for_join, df_label_current_for_join, df_label_entry_for_join = dfs_new

    # Join converted dataframes with Immigration table to add columns
    immigration_data = immigration_data.join(
        df_ports_for_join,
        immigration_data.port_of_entry == df_ports_for_join.entry_port_code,
        "left")
    immigration_data = immigration_data.join(
        df_countries_origin_for_join,
        immigration_data.origin_country == df_countries_origin_for_join.country_code,
        "left")
    immigration_data = immigration_data.join(
        df_countries_resident_for_join,
        immigration_data.resident_country == df_countries_resident_for_join.country_code,
        "left")
    immigration_data = immigration_data.join(
        df_visa_for_join,
        immigration_data.visa_type == df_visa_for_join.visa_type_code,
        "left")
    immigration_data = immigration_data.join(
        df_airport_for_join,
        immigration_data.port_of_entry == df_airport_for_join.local_code,
        "left")
    immigration_data = immigration_data.join(
        df_label_current_for_join,
        immigration_data.state_code == df_label_current_for_join.state_id,
        "left")
    immigration_data = immigration_data.join(
        df_label_entry_for_join,
        immigration_data.entry_state_id == df_label_entry_for_join.state_id,
        "left")

    # Select and sort columns
    immigration_data = immigration_data.drop("country_code")\
        .drop('origin_country')\
        .drop('resident_country')\
        .drop("entry_port_code")\
        .drop("visa_type_code")\
        .drop("local_code")\
        .drop("state_id")

    immigration = immigration_data.select(
        "id",
        "created_date",
        "admission_no",
        "admitted_date",
        "departure_date",
        "arrival_date",
        "port_name",
        "airport_name",
        "entry_state_id",
        "entry_state_name",
        "entry_county",
        "airline",
        "flight_no",
        "gender",
        "birth_year",
        "age",
        "origin_country_name",
        "resident_country_name",
        "visa_type",
        "visa_type_name",
        "state_code",
        'current_state_name')

    # Shrink data into sample rows for the sake of a learning project -- this
    # row should be filtered out in actual environment
    immigration = cluster.createDataFrame(immigration.sample(0.01).collect())

    # Creating Date table using Immigration table
    date = immigration_data.withColumn(
        "day",
        dayofmonth("created_date")) .withColumn(
        "week",
        weekofyear("created_date")) .withColumn(
            "month",
            month("created_date")) .withColumn(
                "year",
                year("created_date")) .withColumn(
                    "weekday",
                    dayofweek("created_date")) .select(
                        "created_date",
                        "day",
                        "week",
                        "month",
                        "year",
        "weekday").drop_duplicates()

    # Converting to pandas dataframe
    immigration = immigration.toPandas()
    date = date.toPandas()

    return immigration, date


def process_crime_file(file):
    """Read and process crime file"""
    # Read in crime file
    df_crime = pd.read_excel(
        'Offenses_Known_to_Law_Enforcement.xls',
        header=4,
        names=[
            "State",
            "County",
            "Violent_crime",
            "Murder_and_nonnegligent_manslaughter",
            "Rape_1",
            "Rape_2",
            "Robbery",
            "Aggravated_assault",
            "Property_crime",
            "Burglary",
            "Larceny-theft",
            "Motor_vehicle_theft",
            "Arson3"])

    # Split a column with more than 1 elements into separate columns
    state = df_crime["State"].str.split("-", expand=True)
    df_crime["State"] = state[0]
    df_crime["category"] = state[1]

    # Change data types of each column
    df_crime = df_crime.fillna(0)
    df_crime = df_crime.astype({'Rape_1': int,
                                'Rape_2': int,
                                'Property_crime': int,
                                'Burglary': int,
                                'Larceny-theft': int,
                                'Motor_vehicle_theft': int,
                                'Arson3': int})

    # Sum up some of the column values to simplify the table. Also get a total
    # value for all crime numbers
    df_crime['Rape'] = df_crime['Rape_1'] + df_crime['Rape_2']
    df_crime = df_crime.drop(['Rape_1', 'Rape_2'], axis=1)
    df_crime['total_crime'] = df_crime['Violent_crime'] + df_crime['Murder_and_nonnegligent_manslaughter']\
        + df_crime['Robbery'] + df_crime['Burglary'] + df_crime['Larceny-theft']\
        + df_crime['Motor_vehicle_theft'] + df_crime['Arson3']

    # Cleaning County names and sum up
    df_crime['State'] = df_crime['State'].str.strip()
    df_crime['County'] = [re.sub(r'\d+|\,', '', x) for x in df_crime['County']]
    df_crime['County_fixed'] = df_crime['County'].replace(
        'Police| Department| Unified| County| Public Safety', "", regex=True)
    df_crime['County_fixed'] = df_crime['County_fixed'].str.strip()

    # Join US Demographic table and Crime table to get total population of
    # states.
    df_label = file
    df_label['county_new'] = df_label['county_name'].mask(
        (df_label['county_name'] == 'LaSalle') & (
            df_label['state_name'] == 'Illinois'), 'La Salle') .mask(
        (df_label['county_name'] == 'LaPorte') & (
            df_label['state_name'] == 'Indiana'), 'La Porte') .mask(
        (df_label['county_name'] == 'DeKalb') & (
            df_label['state_name'] == 'Indiana'), 'De Kalb') .mask(
        (df_label['county_name'] == 'LaSalle') & (
            df_label['state_name'] == 'Louisiana'), 'La Salle') .mask(
        (df_label['county_name'] == 'Lac qui Parle') & (
            df_label['state_name'] == 'Minnesota'), 'Lac Qui Parle') .mask(
        (df_label['county_name'] == 'Trousdale') & (
            df_label['state_name'] == 'Tennessee'), 'Hartsville/Trousdale') .mask(
        (df_label['county_name'] == 'Doña Ana') & (
            df_label['state_name'] == 'New Mexico'), 'Dona Ana') .mask(
        (df_label['county_name'] == 'Dutchess') & (
            df_label['state_name'] == 'New York'), 'Duchess') .mask(
        (df_label['county_name'] == 'LaMoure') & (
            df_label['state_name'] == 'North Dakota'), 'Lamoure')
    df_label_for_join = df_label[['state_name', 'county_new', 'population']]
    df_label_for_join = pd.DataFrame(df_label_for_join.groupby(
        ['state_name', 'county_new']). agg({'population': 'sum'})).reset_index()
    df_label_for_join['state_name'] = df_label_for_join['state_name'].str.upper()

    # Join population column from demographic table to crime table and
    # calculate crime rate
    crime = pd.merge(
        df_crime, df_label_for_join, left_on=[
            'State', 'County_fixed'], right_on=[
            'state_name', 'county_new'], how='inner')
    crime['crime_rate'] = (
        crime['total_crime'] /
        crime['population'] *
        100000).astype(float).round(1)
    crime['violent_crime_rate'] = (
        crime['Violent_crime'] /
        crime['population'] *
        100000).astype(float).round(1)
    crime['property_crime_rate'] = (
        crime['Property_crime'] /
        crime['population'] *
        100000).astype(float).round(1)
    crime = crime[['County_fixed',
                   'State',
                   'category',
                   'total_crime',
                   'Violent_crime',
                   'Murder_and_nonnegligent_manslaughter',
                   'Robbery',
                   'Aggravated_assault',
                   'Property_crime',
                   'Burglary',
                   'Larceny-theft',
                   'Motor_vehicle_theft',
                   'Arson3',
                   'Rape',
                   'crime_rate',
                   'violent_crime_rate',
                   'property_crime_rate']]
    crime = crime.rename(
        columns={
            'County_fixed': 'county',
            'State': 'state',
            'Violent_crime': 'violent_crime',
            'Murder_and_nonnegligent_manslaughter': 'murder_and_nonnegligent_manslaughter',
            'Robbery': 'robbery',
            'Aggravated_assault': 'aggravated_assault',
            'Property_crime': 'property_crime',
            'Burglary': 'burglary',
            'Larceny-theft': 'larceny-theft',
            'Motor_vehicle_theft': 'motor_vehicle_theft',
            'Arson3': 'arson3',
            'Rape': 'rape'})

    return crime


def process_demographic_file():
    """Read and process demographoc file"""
    # Read in demographic file
    df_demo = pd.read_csv('us-cities-demographics.csv', header=None)

    # Split a column which has multiple values into separate columns and set
    # it as a dataframe
    df_demo = df_demo[0].str.split(";", expand=True)
    df_demo = pd.DataFrame(df_demo)

    # Change headers
    headers = df_demo.iloc[0]
    df_demo = pd.DataFrame(df_demo.values[1:], columns=headers)

    # Change data type of each column
    cols = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size',
        'Count']
    df_demo[cols] = df_demo[cols].apply(
        pd.to_numeric, errors='coerce', axis=1).fillna(0.0)
    df_demo = df_demo.astype({'Median Age': float,
                              'Male Population': int,
                              'Female Population': int,
                              'Number of Veterans': int,
                              'Foreign-born': int,
                              'Average Household Size': int,
                              'Count': int})

    # Format a column to make it upper case to keep consistensy among other
    # tables
    df_demo['State'] = df_demo['State'].str.upper()

    # Break down 'race' column into separate columns for each racial groups
    # and get the sum numbers of each state
    df_demo_race = df_demo.groupby(['State', 'Race']).agg(
        {'Count': 'sum'}).unstack(fill_value=0).reset_index()
    df_demo_race.columns = df_demo_race.columns.droplevel(0)
    df_demo_race = df_demo_race.rename(columns={"": 'State'})
    df_demo = df_demo.groupby(
        ['State']).agg(
        {
            'State Code': 'max',
            'Count': 'sum',
            'Median Age': 'median',
            'Male Population': 'sum',
            'Female Population': 'sum',
            'Number of Veterans': 'sum',
            'Foreign-born': 'sum',
            'Average Household Size': 'mean'})
    df_demo['Average Household Size'] = df_demo['Average Household Size'].round(
        2)
    df_demo = pd.merge(
        df_demo,
        df_demo_race,
        left_on='State',
        right_on='State')

    # Select and sort columns
    demographic = df_demo[['State',
                           'State Code',
                           'Count',
                           'Median Age',
                           'Male Population',
                           'Female Population',
                           'Number of Veterans',
                           'Foreign-born',
                           'Average Household Size',
                           'American Indian and Alaska Native',
                           'Asian',
                           'Black or African-American',
                           'Hispanic or Latino',
                           'White']].sort_values('State')
    demographic = demographic.rename(
        columns={
            'State': 'state',
            'State Code': 'state_code',
            'Count': 'population',
            'Median Age': 'median_age',
            'Male Population': 'male_population',
            'Female Population': 'female_population',
            'Number of Veterans': 'number_of_veterans',
            'Foreign-born': 'foreign_born',
            'Average Household Size': 'average_household_size',
            'American Indian and Alaska Native': 'AmericanIndian_and_AlaskaNative_population',
            'Asian': 'Asian_population',
            'Black or African-American': 'Black_or_AfricanAmerican_population',
            'Hispanic or Latino': 'Hispanic_or_Latino_population',
            'White': 'White_population'})
    # Replace null value by 0
    demographic = demographic.fillna(0)

    return demographic


def process_temperature_file(file):
    """Read and process temperature file"""
    # Read in temperature file
    df_temp = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')

    # Format df_label table to join with temperature table
    df_county = pd.DataFrame(file.groupby(['city']).agg(
        {'county_name': 'max', 'state_name': 'max'})).reset_index()

    # Delete rows with null values, filter with only US data
    df_temp = df_temp.dropna()
    df_temp['dt'] = pd.to_datetime(df_temp['dt'])
    df_temp = df_temp[df_temp['Country'] == 'United States']

    # Aggregate table to get avg, min, and max value of temperature for each
    # city
    df_temp_mean = pd.DataFrame(df_temp.groupby(['City']).agg(
        {'AverageTemperature': 'mean'})).reset_index()
    df_temp_mean = df_temp_mean.rename(
        columns={'AverageTemperature': 'avg_temperature'})
    df_temp_mean['avg_temperature'] = df_temp_mean['avg_temperature'].round(2)
    df_temp_max = pd.DataFrame(df_temp.groupby(['City']).agg(
        {'AverageTemperature': 'max'})).reset_index()
    df_temp_max = df_temp_max.rename(
        columns={'AverageTemperature': 'max_of_monthly_avg_temperature'})
    df_temp_max['max_of_monthly_avg_temperature'] = df_temp_max['max_of_monthly_avg_temperature'].round(
        2)
    df_temp_min = pd.DataFrame(df_temp.groupby(['City']).agg(
        {'AverageTemperature': 'min'})).reset_index()
    df_temp_min = df_temp_min.rename(
        columns={'AverageTemperature': 'min_of_monthly_avg_temperature'})
    df_temp_min['min_of_monthly_avg_temperature'] = df_temp_min['min_of_monthly_avg_temperature'].round(
        2)
    mean_max = pd.merge(df_temp_mean, df_temp_max)
    mean_max_min = pd.merge(mean_max, df_temp_min)

    # Join with df_county table to add county column
    temp = pd.merge(
        mean_max_min,
        df_county,
        left_on='City',
        right_on='city',
        how='inner')
    temp = temp.groupby(['county_name',
                         'state_name']).agg({'avg_temperature': 'mean',
                                             'max_of_monthly_avg_temperature': 'max',
                                             'min_of_monthly_avg_temperature': 'min'}).reset_index()

    # Select and sort columns
    temperature = temp[['county_name',
                        'state_name',
                        'avg_temperature',
                        'max_of_monthly_avg_temperature',
                        'min_of_monthly_avg_temperature']].sort_values('county_name')
    temperature['state_name'] = temperature['state_name'].str.upper()
    temperature = temperature.rename(
        columns={
            'county_name': 'county',
            'state_name': 'state'})

    return temperature


def process_data(
        immigration,
        date,
        demographic,
        crime,
        temperature,
        cur,
        conn):
    """Insert the processed data into each table"""

    for index, row in immigration.iterrows():
        cur.execute(immigration_table_insert, list(row.values))
        conn.commit()

    for index, row in date.iterrows():
        cur.execute(date_table_insert, list(row.values))
        conn.commit()

    for index, row in demographic.iterrows():
        cur.execute(demographic_table_insert, list(row.values))
        conn.commit()

    for index, row in crime.iterrows():
        cur.execute(crime_table_insert, list(row.values))
        conn.commit()

    for index, row in temperature.iterrows():
        cur.execute(temperature_table_insert, list(row.values))
        conn.commit()
        
        
def data_check(cur, conn):
    """ 
    This data checks tables which data was inserted into to make sure they contain values,
    and raising error messages if they have no results or cantain no rows.
    """
    failing_tests = []
    for key in data_check_queries:
        cur.execute(data_check_queries[key])
        sql_result = cur.fetchone()
        
        exp_result = 0
        
        error_count = 0
        
        if exp_result != sql_result [0]:
            error_count += 1
            failing_tests.append(data_check_queries[key])
            
        if error_count > 0:
            print(f'{key}: SQL Tests Failed')
            raise ValueError('Data quality check failed')
            
        if error_count == 0:
            print(f'{key}: SQL Tests Passed')        
        

def main():

    # Define postgres connection and curser
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=immigrationdb user=student password=student")
    cur = conn.cursor()

    create_spark_cluster()

    read_file()

    process_immigration_file(create_spark_cluster(),read_file())
    process_crime_file(read_file())
    process_demographic_file()
    process_temperature_file(read_file())

    immigration, date = process_immigration_file(
        create_spark_cluster(),
        read_file())

    process_data(
        immigration, 
        date, 
        process_demographic_file(), 
        process_crime_file(read_file()), 
        process_temperature_file(read_file()), 
        cur, 
        conn)
    
    data_check(cur, conn)

    # Closing connection to database
    conn.close()
    

if __name__ == "__main__":
    main()
