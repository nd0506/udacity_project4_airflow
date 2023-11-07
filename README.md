# Project: Data Pipelines with Airflow

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

### Requirements: 
1. Create a data pipelines that are dynamic and build from reusable tasks, can be monitored, and allow easy backfills.
2. Ensure data quality of the final data in data warehouse.

### Data context:
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Structure

### DAG
[sparkify_pipline_dag.py](https://github.com/nd0506/Udacity_project4/blob/main/DAG/sparkify_pipline_dag.py)

### Plugin Operators

1. [stage_redshift.py](https://github.com/nd0506/Udacity_project4/blob/main/Plugins/Operators/stage_redshift.py)
2. [load_fact.py](https://github.com/nd0506/Udacity_project4/blob/main/Plugins/Operators/load_fact.py)
3. [load_dimension.py](https://github.com/nd0506/Udacity_project4/blob/main/Plugins/Operators/load_dimension.py)
4. [data_quality.py](https://github.com/nd0506/Udacity_project4/blob/main/Plugins/Operators/data_quality.py)


