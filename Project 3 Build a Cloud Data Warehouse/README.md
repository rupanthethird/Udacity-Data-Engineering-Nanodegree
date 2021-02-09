# Project 3: Sparkify - songplay analysis with S3 and Redshift on AWS

## Overview
A music app startup called Sparkify has been collecting songs and user activity
on their new music streaming app they have developed.
And now they want to move their ETL processes and data onto the cloud so that their analytics team can analyze user's song history.

Their data resides in S3, in a directory of JSON logs on user activity on the app,
as well as a directory with JSON metadata on the songs in their app.

This repository shows an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables.
The Redshift tables you create with these pipelines can be used to run queries and get an understanding on how Sparkify's app is used by its users.

## Schema

![ER diagram for Sparkfy schema - Redshift](https://user-images.githubusercontent.com/51218559/104693095-6c3cff80-574c-11eb-85be-05c1dc3df4a9.jpeg)

### Staging Table
These two staging tables are the copies of data in S3 buckets.

**staging_songs** - information about songs and artists

- artist (*varchar*)
- auth (*varchar*)
- firstName (*varchar*)
- gender (*varchar*)
- ItemInSession (*int*)
- lastName (*varchar*)
- length (*float*)
- level (*varchar*)
- location (*varchar*)
- method (*varchar*
- page (*varchar*)
- registration (*varchar*)
- sessionId (*int*)
- song (*varchar*)
- status (*int*
- ts (*bigint*)
- userAgent (*varchar*)
- userId (*int*)

**staging_events** - actions done by users

- song_id (*varchar*) PRIMARY KEY
- artist_id (*varchar*)
- artist_latitude (*float*
- artist_longitude (*float*)
- artist_location (*varchar*)
- artist_name (*text*
- duration (*float*)
- num_songs (*int*)
- title (*varchar*)
- year (*int*)

### Fact Table
**songplay_table**  -  records in log data associated with song plays i.e. records with page NextSong

- songplay_id (*int IDENTITY(0,1)*) PRIMARY KEY
- start_time (*timestamp*)
- user_id (*int*)
- level (*varchar*)
- song_id (*varchar*)
- artist_id (*varchar*) distkey
- session_id (*varchar*)
- location (*varchar*)
- user_agent (*varchar*)

### Dimension Tables
**song_table**  -  songs in music database

- song_id (*varchar*)  PRIMARY KEY
- title (*varchar*)
- artist_id (*varchar*) distkey
- year (*int*)
- duration (*float*)

**artist_table** - artists in music database

- artist_id (*varchar*)  PRIMARY KEY distkey
- name (*varchar*)
- location (*varchar*)
- latitude (*float*)
- longitude (*float*)

**user_table** - users in the app

- user_id(*int*) PRIMARY KEY
- first_name(*varchar*)
- last_name(*varchar*)
- gender(*varchar*)
- level(*varchar*)

**time_table** - timestamps of records in songplays broken down into specific units

- start_time(*timestamp*) PRIMARY KEY
- hour(*int*)
- day(*int*)
- week(*int*)
- month(*int*)
- year(*int*)
- weekday(*int*)<br></p>

## Required AWS Configurations
1. Create an IAM user in your AWS account which has AdministratorAccess.
2. Use access key and secret key to create clients for EC2, S3, IAM, and Redshift.
3. Create an IAM Role and attach policy that makes Redshift able to access S3 bucket (ReadOnly)
4. Create a security group to use for Redshift cluster.
5. Create a RedShift cluster and get the DWH_ENDPOIN(Host address) and DWH_ROLE_ARN and fill the config file.(FYI: host should look like 'redshift-cluster-1.*hidden*.us-west-2.redshift.amazonaws.com', and role should look like 'arn:aws:iam::*hidden*:role/MyRedshiftRole1')
 - You need to change the Publicly Accessible setting from "No" to "Yes".

NOTE: Make sure the region is set to "us-west-2" to match the region of S3 bucket

## ETL Pipeline
1. Create staging tables and analytics(fact and dimension) tables on Redshift to store the data from S3 buckets.
2. Load the data from S3 buckets to "staging" tables on Redshift.
3. Insert data into analytics(fact and dimension) tables on Redshift from the staging tables.<br>

*Why we create the staging tables?:*<br>
*(reference from AWS) The staging table is a temporary table that holds all of the data that will be used to make changes to the target table, including both updates and inserts.*


## Files
- dhw.cfg - Configuration file with information about Redshift cluster, IAM role, and S3.
- create_tables.py - Python script to drop tables (if exists) and create tables.
- etl.py - Python script to execute queries to extract JSON data from the S3 bucket and ingest them into Redshift tables.
- sql_queries.py - Python script that contains a set of SQL queries to DROP, CREATE, and INSERT tables accordingly with the schema shown above.
- README.me - this file
##### How to Run:
1. Create tables by running create_tables.py.
2. Execute ETL process by running etl.py.

## Example Query to run on Redshift console
The following query on Redshift console would give you the answer to the question of how many distinct users listened to the songs of the artist "Maroon5" with their app:

<code>
SELECT COUNT(DISTINCT user_id) FROM songplay_table WHERE artist LIKE '%maroon5%';
</code>
