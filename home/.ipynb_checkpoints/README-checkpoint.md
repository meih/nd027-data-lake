# Sparkify Database ETL on PostgreSQL

## Overview

<!-- The purpose of this database in the context of the startup, Sparkify, and their analytical goals. -->

Sparkify Database will allow us to analyze when and how, what songs are played by each customer in the service, in a form of `songplay` data. By identifying the quantitative characteristics of the `songplays`, it will help in making business decisions such as how to design paid plans, what songs should be used in campaigns, etc.

The ETL script will extract data from song data and log data in S3, and creates parquet files for analytic purposes.

## How to run the Python scripts

Run ETL process
```
python etl.py
```

## An explanation of the files in the repository

This repository contains the following:

```
data/
  log-data.zip   - Sample log data of each song play peformed by customers
  song-data.zip  - Sample data of artists and songs that can be played by customers
etl.py           - Script for ETL process
dl.cfg           - Configuration for AWS connectivity
```

### `song_data` example

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### `log_data` example

```
{"artist":"Survivor","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":245.36771,"level":"free","location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong","registration":1541033612796.0,"sessionId":100,"song":"Eye Of The Tiger","status":200,"ts":1541110994796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}
```

## Database schema design and ETL pipeline

This ETL pipeline has the following process:
1. Create a Spark session
2. Read `song_data` and create parquet files `songs` and `artists`
3. Read `log_data` and create parquet files `users`, `time` and `songplays`

The `songplays` table will allow us to analyze when and how songs are played on Sparkify according to your business needs.

### Fact Table

* songplays - records in log data associated with song plays i.e. records with page NextSong
   * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

* users - users in the app
    * user_id, first_name, last_name, gender, level

* songs - songs in music database
    * song_id, title, artist_id, year, duration

* artists - artists in music database
    * artist_id, name, location, latitude, longitude

* time - timestamps of records in songplays broken down into specific units
    * start_time, hour, day, week, month, year, weekday
