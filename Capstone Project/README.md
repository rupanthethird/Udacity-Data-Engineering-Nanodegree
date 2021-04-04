# US Immigration Data with Detailed Profile of Entry/Destination States and Cities
## Data Engineering Capstone Project

### Project Summary
The purpose of this project is to gather and transform US immigration data and its related data including US demographic data, temperature data, and crime data, so that one can obtain more detailed analysis results on the US immigration data.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Scope
#### Solution goal:
The goal is to create a database schema that consists of 1 fact table and 4 dimension tables.  
- A **fact** table would be **immigration** table, created by combining Immigration data with different data sources whose primary key would be *id*.
- The first **dimension** table would be **demographic** table, whose primary key would be *state*.
- The second **dimension** table would be **crime** table, whose composite key would be *county_name* and *state_name*.
- The third **dimension** table would be **temperature** table, whose composite key would be *county* and *state*.
- The last **dimension** table would be **date** table, whose primary key would be *created_date*.


#### Data to use:
- `I94 Immigration Data` - US Immigration data of 2016 from "US National Tourism and Trade Office". It includes information about immigrants such as origin city, reason for immigration, visa type, gender, birth year, flight number and so on.
- `I94_SAS_Labels_Descriptions.SAS` - Text file which includes lists of codes and corresponding lables for the `I94 Immigration Data`.
- `us-cities-demographics.csv`   - Basic demographic information of the US of 2015 which comes from "OpenSoft". It includes statistics by state, city, race, age, gender and so on.
- `airport-codes.csv.csv`  -  List of all airport codes which comes from "DATA HUB". It includes airport information such as airpot type, name, municipality and so on.
- `Offenses_Known_to_Law_Enforcement.xls`   - Number of crimes by US state and metropolitan and non-metropolitan country of 2015 which comes from "data.world". It includes number of crimes in each crime category in the US.
- `GlobalLandTemperaturesByCity.csv` - Global temperature information since 1970s by country and cities which come from "Kaggle". It includes monthly average temperatures.
- `uscities.csv` - Basic information about US cities, counties, and states.


#### Tool to use:
- `Jupyter Notebook`
- `Python`
- `Pandas`
- `PySpark`
- `PostgreSQL`


### Step 2: Explore and Assess the Data
#### Explore the Data
Identify data quality issues, like missing values, duplicate data, etc.


#### Cleaning Steps

- US Demographic data
   (Data: `us-cities-demographics.csv`)
  1. Change data types of each column
  2. Format columns to keep consistensy among other tables
  3. Break down 'race' column into separate columns for each racial groups and sum up the numbers by each state
  4. Select and sort columns to finalize the Demographic table


- Crime data
   (Data: `Offenses_Known_to_Law_Enforcement.xls`, `uscities.csv`)
  1. Split a column which has more than 1 elements into separate columns
  2. Change data types of each column
  3. Sum up some of the column values to simplify the table. Also get a total value for all crime numbers
  4. Join with `uscities.csv` table to acquire total population of each county
  5. Calculate crime rate by county
  6. Select, sort and rename columns to finalize table


- Immigiration data
    (Data: `I94 Immigration Data`, `I94_SAS_Labels_Descriptions.SAS`, `uscities.csv`, - `airport-codes.csv.csv` )
  1. For those columns that have many missing values or that are not important for this project, deleted them, change column data types, and rename column names to make it more understandable
  2. In order to add label columns from other tables to Immigration table for acquiring code lables, format `I94_SAS_Labels_Descriptions.SAS` and `uscities.csv` file to join
  3. Create and format label tables for ports, countries, and visa
  4. Format `uscities.csv` table to join state names
  5. Convert pandas dataframes to pyspark dataframes for joining with `I94 Immigration Data` table
  6. Join converted dataframes with `I94 Immigration Data` table to add label columns
  7. Select and sort columns, filter to finalize the Immigiration table


- Temperature data
    (Data: `GlobalLandTemperaturesByCity.csv`, `uscities.csv`)
  1. Format `uscities.csv` table to join with `GlobalLandTemperaturesByCity.csv` table
  2. Delete rows with null values, filter with only US data
  3. Aggregate table to get avg, min, and max value of temperature for each city
  4. Join with uscities table to add county column
  5. Select and sort columns to finalize the Temperature table

  ### Step 3: Define the Data Model
  #### 3.1 Conceptual Data Model
  #### Tables:
  ##### Fact Table:  
  ###### 1. immigration
   - description: contains i94 immigrations data
   - columns: *id, created_date, year, month, admission_no, admitted_date, departure_date, arrival_date, port_name, airport_name, entry_state_id, entry_state_name, entry_county, airline, flight_no, gender, birth_year, age, origin_country_name, resident_country_name, visa_type, visa_type_name, state_code, current_state_name*

  ##### Dimension Tables:  
  ###### 2. demographic
   - description:  contains demographic information by US cities
   - columns:  *State(Primary key), State Code, City, Count, Race, Median Age, Male Population, Female Population, Number of Veterans, Foreign-born, Average Household Size*

  ###### 3. crime
   - description: contains crime information in different categories by US states
   - columns:  *county(Composite key), state(Composite key), category, total_crime, Violent_crime, Murder_and_nonnegligent_manslaughter, Robbery, Aggravated_assault, Property_crime, Burglary, Larceny-theft, Motor_vehicle_theft, Arson3, Rape, crime_rate, violent_crime_rate, property_crime_rate*

  ###### 4. temperature
   - description:  contains temperatures by US counties
   - columns:  *county(Composite key), state(Composite key), avg_temperature, max_of_monthly_avg_temperature, min_of_monthly_avg_temperature*

  ###### 5. date
   - description:  contains date information related to Immigration table
   - columns:  *created_date(Primary key), day, week, month, year, weekday*

  ##### Why I chose this model:
  Along with Immigration table which contains key information about each immigrant in the US, I provided with a variety of dimension tables that could be useful for exploring immigration table more in details.
  To do so, I have transformed all tables in a way that one can join them with immigration table by cleaning fact table to make foreign keys and by setting primary keys in each dimension table.


### Step 4: Run Pipelines to Model the Data
#### 4.1 Create the data model
1. Create database and tables by running 'create_tables.py'
2. Extract, transform, and load data by running 'etl.py'

#### 4.3 Data dictionary
###### 1. immigration
 - This table contains i94 immigrations data.
 - It comes from "US National Tourism and Trade Office".
 - It shows you information about each immigrant id whose resident country was Japan, and who was registered in i94 system during 2016.

| Field Name | Data Type | Description | Example |
| --- | --- | --- | --- |
| **id** *Primary key* | int | unique id of I94 immigration record | 4526055 |
| created_date *Foreign key* | date | date the data was created | 4/24/2016 |
| admission_no | int | admission no. of immigrants | 2147483647 |
| admitted_date | date | date the immigrants were admitted for the entry | 7/22/2016 |
| departure_date | date | date the immigrants departed the US | 4/28/2016 |
| arrival_date | date | date the immigrants arrived to the US | 4/24/2016 |
| port_name | varchar | airport code of immigrants's entry | NEWARK/TETERBORO |
| airport_name | varchar | airport name of immigrants's entry | Lakefront Airport |
| entry_state_id | varchar | state code of immigrants' entry | LA |
| **entry_state_name**　*Foreign key* | varchar | state name of immigrants' entry | LOUISIANA |
| **entry_county**　*Foreign key* | varchar | county name of immigrants' entry | New Orleans |
| airline | varchar | airline used by immigrants for entry | JL |
| flight_no | varchar | flight no. used by immigrants for entry | 60 |
| gender | varchar | gender of immigrants | M |
| birth_year | int | birth year of immigrants | 1971 |
| age | int | age of immigrants | 45 |
| origin_country_name | varchar | original country of immigrants | JAPAN |
| resident_country_name | varchar | resident country of immigrants | JAPAN |
| visa_type | bigint | immigrants' visa type code | 1 |
| visa_type_name | varchar | immigrants' visa type name | Business |
| state_code | varchar | state code of immigrants's final destination | AZ |
| **current_state_name**　*Foreign key* | varchar | state name of immigrants' final destination | ARIZONA |

###### 2. demographic
 - This table contains basic demographic information of the US of 2015.
 - It comes from "OpenSoft".
 - It allows you to join by Primary key 'state' with the fact table to dig in more deeply about the satistics of the places immigrants chose to enter or end up staying in the end.

| Field Name | Data Type | Description | Example |
| --- | --- | --- | --- |
| **state** *Primary key* | varchar | state name | ALABAMA |
| state_code | varchar | state code | AL |
| population | int | population | 1096154 |
| median_age | float | median age | 38 |
| male_population | int | male population | 2448200 |
| female_population | int | female population | 2715106 |
| number_of_veterans | int | number of veterans | 352896 |
| foreign_born | int | number of foreign-born | 252541 |
| average_household_size | float | avg of household size | 2 |
| AmericanIndian_and_AlaskaNative_population | int | Native American and Alaskan Native population | 8084 |
| Asian_population | int | Asian population | 28769 |
| Black_or_AfricanAmerican_population | int | Black of African Americn population | 521068 |
| Hispanic_or_Latino_population | int | Hispanic or Latino population | 39313 |
| White_population | int | White population| 498920 |

###### 3. crime
 - This table contains number of crimes by US state and county of 2015.
 - It comes from "data.world".
 - It allows you to join by Composite key 'state' and 'county' with the fact table to understand crime situations including crime rate of the places immigrants chose to enter or end up staying in the end.

| Field Name | Data Type | Description | Example |
| --- | --- | --- | --- |
| **county** *Composite key* | varchar | county name | Autauga |
| **state** *Composite key* | varchar | state name | ALABAMA |
| category | varchar | category of county | Metropolitan Counties |
| total_crime | int | total number of crimes | 2/22/1901 |
| violent_crime | int | number of violent crimes | 3/9/1900 |
| murder_and_nonnegligent_manslaughter | int | number of murders and nonnegligent manslaughters | 1/0/1900 |
| robbery | int | number of robberies | 6 |
| aggravated_assault | int | number of aggravated assaults | 50 |
| burglary | int | number of burglaries | 344 |
| property_crime | int | number of property crimes | 111 |
| larceny_theft | int | number of larceny thefts | 187 |
| motor_vehicle_theft | int | number of motor vehicle thefts | 46 |
| arson3 | int | number of arsons | 0 |
| rape | int | number of rapes | 13 |
| crime_rate | float | crime rate(per 100,000 people) | 972.5 |
| violent_crime_rate | float | crime rate of violent crimes(per 100,000 people) | 160.1 |
| property_crime_rate | float | crime rate of property crimes(per 100,000 people) | 798.4 |

###### 4. temperature
 - This table contains global temperature information since 1970s by US state and county.
 - It comes from "Kaggle".
 - It allows you to join by Composite key 'state' and 'county' with the fact table to understand the statistics of temperature of the places immigrants chose to enter or end up staying in the end.

| Field Name | Data Type | Description | Example |
| --- | --- | --- | --- |
| **county** *Composite key* | varchar | county name | Allegheny |
| **state** *Composite key* | varchar | state name | PENNSYLVANIA |
| avg_temperature | float | average of monthly average temperature in 1970-2016  | 9.61 |
| max_of_monthly_avg_temperature | float | max value of of monthly average temperature in 1970-2016 | 28.6 |
| min_of_monthly_avg_temperature | float | minimum value of of monthly average temperature in 1970-2016 | -11.42 |

###### 5. date
 - This table is the breakdown of immigration table's 'created date' column.
 - It allows you to join by Primary key 'created_date' with the fact table to analyze from date perspective.

| Field Name | Data Type | Description | Example |
| --- | --- | --- | --- |
| **created_date** *Primary key* | date | date the immigration record was created | 6/23/2016 |
| day | int | date of created_date | 23 |
| week | int | week of created_date | 25 |
| month | int | month of created_date | 6 |
| year | int | year of created_date | 2016 |
| weekday | int | weekday of created_date | 5 |

#### Step 5: Complete Project Write Up
###### 1. Why I chose the technologies I used for this project
- `Jupyter Notebook` - I used notebook to better understand the data as I process them visually.
- `Python` - Python is used for creating the actual scripts to execute the whole process because it is one of the simplest languages to work with.
- `Pandas` - I used pandas because pandas is convinient for checking, cleaning, merging and visualizing the results after each manipulation process.
- `PySpark` - I used PySpark to work with large data, i94 immigration data, making use of distributed processing for more efficiency. I did not create a EMR cluster to use PySpark for this project since the project workspace did not recquired an EMR cluster, but if you do it locally you would need to create one.
- `PostgreSQL` - I used PostgreSQL to create a database and tables because it is recommended to use for relational models, and also because I'm familiar with it.

###### 2. How often this data should be updated and why
* Monthly update is recommended since the original i94 data itself is updated on monthly basis.

###### 3. How I would approach the problem differently under the following scenarios:
* *The data was increased by 100x*.
  *  I would use PySpark instead of pandas for all the data, and for writing into tables I would still use PostgreSQL, but suggest upgrading a hardware(i.e. better CPU, RAM, and SSD) to execute. If I wanted to use cloud infrustructure, I would use AWS S3 to write and load into those tables.
* *The data populates a dashboard that must be updated on a daily basis by 7am every day.*
  *  I would implement Apache Airflow to create a DAG file that executes ETL pipelines to run everyday before 7 am.
* *The database needed to be accessed by 100+ people.*
  *  I would probably stick to using PostgreSQL as a database, however, if it is too slow I would move them to cloud data warehouse such as Redshift on AWS.
