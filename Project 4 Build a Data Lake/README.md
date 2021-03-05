# Project 4: Sparkify - songplay analysis with Data Lake using Spark on AWS

## Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task as a data engineer is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

This repository shows an ETL pipeline that extracts their data from S3, process the data into analytics tables using Spark, and load them back into S3. To do this, we have deployed this Spark process on a cluster using AWS.

## Pre-requisites/Installation
- Python 3.6
- pyspark - Apache Spark Python API([install from here](https://pypi.org/project/pyspark/))
- AWS account (access key, secret access key)

### Schema
#### Fact Table
**1.songplays**  -  records in log data associated with song plays i.e. records with page NextSong

- songplay_id
- start_time
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent

#### Dimension Tables
**2.songs**  -  songs in music database

- song_id
- title
- artist_id
- year
- duration

**3.artists** - artists in music database

- artist_id
- name
- location
- latitude
- longitude

**4.users** - users in the app

- user_id
- first_name
- last_name
- gender
- level

**5.time_table** - timestamps of records in songplays broken down into specific units

- start_time
- hour
- day
- week
- month
- year
- weekday<br></p>

## Required AWS Configurations
1. Create an S3 bucket in AWS account to store the output(processed) files.
2. (If you haven't already,) Create an IAM Role and attach policy that has full access(read and write) to S3 bucket

NOTE: Make sure the region is set to "us-west-2" to match the region of S3 bucket where the original files reside

## ETL Pipeline
- Extract data from S3 buckets and process data using Spark.
- Load the data back to the created S3 bucket.

## Project Files
- `etl.py` - Python script to execute the queries to extract JSON data from the S3 bucket to process using Spark, and writes them back to S3
- `dl.cfg` - Configuration file with information about AWS key pairs (*Make sure not to include quotations for the key pairs to avoid errors)
- `README.md` - this file

## How to Run
Execute ETL process by running `etl.py`.
