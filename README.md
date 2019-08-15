# Data Lake With Spark

## Objective
To build an ETL pipeline for a data lake hosted on S3. 


## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Files 
### etl.py 
It reads data from S3, processes that data using Spark, and writes them back to S3

### dwh.cfg 
It stores information about config, and connections to my AWS Account. Due to account security consideration, I've deleted the data in the config file. 

### Input and output S3 files 
Input data are from "s3a://udacity-dend/" bucket. Output data in parquet file will be saved to my own bucket - "s3a://datalake-project/".

## Tables
There are two types of tables here in this project. 

1. For fact tables 
As I used star schema for designing my database, the fact table I created is for song play event. It stores data about artist_id, level, location, session_id, song_id, start_time, user_agent, user_id. 
2. For dimension tables 
I created four dimension tables - users, artists, songs, and time. 

## How to run the scripts to make the ETL happen? 
Simply run the etl.py, and it will execute all the steps from loading data, processing data, to saving data. 
