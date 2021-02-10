# Project 1: Sparkfy - Relational Data Model

## Overview
A music app startup called Sparkify has a mission to analyze the data they've been collecting on songs and user activity in their new music streaming app they have developed.
However, they did not have a database with which they could do analysis until now.<br>

Since the analytics team is particularly interested in understanding what songs users are listening to, we have worked on modelling and processing of song play activity data
which they have been saving as JSON files on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
This process was done by using an ETL pipeline written in Python language, and the DBMS used is PostgreSQL so that they can use the Relational Database which is preferable for data analysis purposes.</p>

This repository shows an ETL-pipeline to create tables, and load the data of JSON files into the database.<br>
The database we created with these pipelines can be used to run queries and get an understanding on how Sparkify's app is used by the users.</p>

## Pre-requisites/Installation
- Python 3.6
- PostgreSQL
- psycopg2 - Python-PostgreSQL Database Adapter([install from here](https://pypi.org/project/psycopg2/#files))
- sql_queries - Build simple SQL queries fast and clean([install from here](https://pypi.org/project/sql-queries/))

## Schema

![ER diagram for Sparkfy schema](https://user-images.githubusercontent.com/51218559/103168751-41603980-4879-11eb-938f-e5186d41779f.png)

### Fact Table
**1. songplays**  -  records in log data associated with song plays i.e. records with page NextSong

- songplay_id (*serial*) PRIMARY KEY
- start_time (*timestamp*)
- user_id (*int*)
- level (*varchar*)
- song_id (*varchar*)
- artist_id (*varchar*)
- session_id (*varchar*)
- location (*varchar*)
- user_agent (*varchar*)

### Dimension Tables

**2. songs**  -  songs in music database

- song_id (*varchar*)  PRIMARY KEY
- title (*varchar*)
- artist_id (*varchar*)
- year (*int*)
- duration (*float*)

**3. artists** - artists in music database

- artist_id (*varchar*)  PRIMARY KEY
- name (*varchar*)
- location (*varchar*)
- latitude (*float*)
- longitude (*float*)

**4. users** - users in the app

- user_id(*int*) PRIMARY KEY
- first_name(*varchar*)
- last_name(*varchar*)
- gender(*varchar*)
- level(*varchar*)

**5. time** - timestamps of records in songplays broken down into specific units

- start_time(*timestamp*) PRIMARY KEY
- hour(*int*)
- day(*int*)
- week(*int*)
- month(*int*)
- year(*int*)
- weekday(*int*)<br></p>

## Files

- create_tables.py - Python script to drop and create tables. It is used to reset tables before each time running the ETL scripts.
- etl.ipynb - Jupyter Notebook which reads and processes a single file from song_data and log_data, and loads the data into the tables with detailed instructions on each process.
- etl.py - Python script which reads and processes files from song_data and log_data and loads them into the tables.
- test.ipynb - Jupyter Notebook to display the first few rows of each table to check the database.
- README.md - This file.
- sql_queries.py - Python script which contains all of sql queries, and is imported into the last three files above.
- data - Folder containing sample data that is a subset of the Million Song Dataset.</p>

### Example Query

The following query on the database would give you the answer to the question of how many distinct users listened to the songs of the artist "Maroon5" with their app:

<code>SELECT COUNT(DISTINCT user_id) FROM songplays WHERE artist LIKE '%maroon5%';</code>
