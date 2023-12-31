# Data Modeling with Cassandra

--------------------------------------------

### Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Our role is to create a database for this analysis. We'll be able to test your database by running queries given to us by the analytics team from Sparkify to create the results.

--------------------------------------------

### Data

For this project, we'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
1. event_data/2018-11-08-events.csv
2. event_data/2018-11-09-events.csv


---------------------------------------------
