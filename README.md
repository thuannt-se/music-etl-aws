# MUSIC ETL Project
## General info
A simple source for ETL using AWS and python boto3 library (Project from Udacity)
## Technologies
Project is created with:
* python
* AWS service
## Setup
* Install all requirement library for source
* Optional: You can run iac_setup.py for auto setup infrastructure in AWS for easy setup (Remember to fill in KEY & SECRET in dwh.cfg)
* Fill in needed info in dwh.cfg, included: HOST, DB_NAME,DB_USER,DB_PASSWORD,DB_PORT and ARN
* Run create_tables.py to create database and tables.
* Run etl.py to perform extract data and insert into tables.
## Result
After successful executing create_tables.py and etl.py, it should create tables: songplays, times, users, songs, artists, staging_events, staging_songs.

