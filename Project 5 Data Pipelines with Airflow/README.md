## Data Pipelines with Airflow

## Overview
A music streaming company, Sparkify, has decided to introduce more automation and monitoring to their data warehouse ETL pipelines using Apache Airflow.

The goal is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They also put importance in the data quality and run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

This repository shows an automated ETL pipelines that extracts their data from S3, process the data into analytics tables, and load them on Redshift using custom operators on Apache Airflow to run hourly.

## Dag task dependencies
<img width="977" alt="example-dag" src="https://user-images.githubusercontent.com/51218559/110060435-407fe280-7da9-11eb-9dce-7bda6428bec7.png">
