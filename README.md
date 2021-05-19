# MUSIC ETL Project
## General info
In this project, I build an ETL pipeline for a database hosted on Redshift. 
I load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.
A simple source for ETL using AWS and python boto3 library (Project from Udacity)
## Technologies
Project is created with:
* python
* AWS service
## Setup
* Install all requirement library in all files.
* Files:
  * iac_setup.py: Run this file if you want to setup aws infrastructure automatically. It will create IAM user, role, Redshift, EC2 instance on AWS service.
    Before running iac_setup.py: Fill in required fields in dwh.cfg including: KEY, SECRET, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT.
    After running iac_setup.py: IAM Role and AWS Redshift instance will be successfully created. Copy Redshift endpoint and ARN role then paste into HOST field and ARN field.
  * Run create_tables.py to create database and tables in Redshift
  * Run etl.py to perform extract data and insert into tables in Redshift. It can take up to several minutes.
## Result
After successful executing create_tables.py and etl.py, it should create tables: songplays, times, users, songs, artists, staging_events, staging_songs.

