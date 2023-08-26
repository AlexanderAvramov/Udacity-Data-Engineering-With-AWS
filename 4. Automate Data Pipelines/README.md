# Project 4 - Airflow

## Project Intro
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring us into the project and expect us to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Details
This project will introduce us to the core concepts of Apache Airflow. To complete the project, we will need to create our own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

[image](https://video.udacity-data.com/topher/2019/January/5c48a861_example-dag/example-dag.png)

## Data
For this project, we'll be working with two datasets. Here are the s3 links for each:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

## Structure
This repo has three subfolders, one for dags, one for plugins, and one for images.

### DAGs
This folder contains the ETL Pyton file. This file structures the DAG.

#### Table Creation
There is a subfolder titled 'table_creation'. This subfolder contains SQL queries that are to be executed. There is one query per file, since AWS did not allow multiple statements to be executed by one task.

### Plugins

#### Helpers
This subfolder contains the SQL queries that are used to generate the fact and dimension tables.

#### Operators
This is where the operators are housed. 

* The stage_redshift.py operator copies the raw data to staging tables in Redshift.
* The load_dimension.py operator loads a dimension table from a staging table.
* The load_fact.py operator loads a fact table from a staging table.
* The data_quality.py operator runs data quality checks.


### Images
This folder contains the the graph of the sucessful DAG.
[dag](/airflow/images/dag.png)

## Steps
1. I copied the song and events data from the Udacity S3 Bucket into my own project bucket. I also copied over the manifest file for the events data.
2. Within the Workspace, I started Airflow using /opt/airflow/start-services.sh, then /opt/airflow/start.sh, then created a user, then sh set_connections.sh (not included in this repo since it contains sensitive information)
3. I entered into the Airflow UI and triggered the execution of the DAG. 

## Results
[all_tables](/airflow/images/all_tables.png)
[artists](/airflow/images/artists.png)
[songplays](/airflow/images/songplays.png)
[songs](/airflow/images/songs.png)
[time](/airflow/images/time.png)
[users](/airflow/images/users.png)