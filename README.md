# Yelp Review Data Engineering DataLake Solution

Tech Stack: Python, SQL, Spark, AWS S3, Databricks

## Overview

#### Project Scoping:
The goal of this data engineering solution is to model and analyze user reviews data from the popular business rating platform, Yelp. The source dataset is well over 1 million rows and can be accessed in this publicly available [s3 bucket](https://s3.console.aws.amazon.com/s3/buckets/yelp-raw). There source files includes the business-related CSV files (name,location,rating.etc), the user-related JSON files (name, city, yelping_since.etc) and the review-related JSON files in which each record describe a real-life review from an authenticated Yelp user.

The project takes in those source files and models the data into a ReviewInfo fact table and 4 dimention tables that store extracted information with regards to the business, the user, the review and the time of the review. With this model, the downstream consumer can gain a clear picture on info that's realated to each user and business registered on yelp and their related reviews.

Note: Details on the source data and the data models are outlined in later sections.

#### Technology Choice
This project uses a AWS S3/Databricks datalake architecture design to minimize computation efficiency and lower cost. Databricks offers  a fully mananged spark instance and customizable cluster configuration. It's highly scalable with great reliability. It also offer seemless integration with AWS S3.

#### Addressing Possible Future Scenarios:
- The data was increased by 100x: choose a bigger cluster in databricks with more worker nodes.
- The pipelines would be run on a daily basis by 7 am every day: Schedule spark jobs using the Databricks job function.
- The database needed to be accessed by 100+ people: Databricks is a collaberative workspace. We can add more users to the workspace if more people need to get access.


## Source Data

The source Data for this project are two sets of JSON files that contains information realted to the Yelp users and Yelp reviews respectively and a CSV file that contains information related to the registered businesses on Yelp.

#### User JSON files
The user JSON files are pulled directly from the Yelp platform. The following filepath and its content are given as an example:

> yelp-raw/users/user_dataset_yelp.000.json

> {"user_id":"QaELAmRcDc5TfJEylaaP8g","name":"John","review_count":206,"yelping_since":"2008-09-20 00:08:14","useful":233,"funny":160,"cool":84,"elite":"2009","friends":"iog3Nyg1i4jeumiTVG_BSA,M92xWY2Vr9w0xoH8bPplfQ","fans":6,"average_stars":3.08,"compliment_hot":2,"compliment_more":1,"compliment_profile":0,"compliment_cute":0,"compliment_list":0,"compliment_note":7,"compliment_plain":14,"compliment_cool":7,"compliment_funny":7,"compliment_writer":2,"compliment_photos":0}


#### Review JSON files

The Review JSON files are pulled directly from the Yelp platform. The following filepath and its content are given as an example:

> yelp-raw/reviews/dataset_reviews_yelp.001.json

> {"review_id":"8TEkoterQ2-VkT9VfZ3Sfw","user_id":"i7jVRyOWCFuU577kg81hwg","business_id":"n3a06UfiqY7HX3XD4bfVwg","stars":3.0,"useful":3,"funny":5,"cool":0,"text":"Back again and looks like the characters have left for greener pastures. For $17 plus $3.00 tip, you'd think the barbers would at least pretend that it's worth it. Maybe leave hot towel on for more than fifteen seconds and give a head massage. If it means paying more, suggest what would be good for to customer","date":"2014-08-05 00:11:08"}

#### Business CSV files

The Business CSV files describe information related to the businesses registered on Yelp. The following filepath and its content are given as an example:

> yelp-raw/businesses.csv

> business_id	name,	city,	state	categories,	postal_code,	review_count,	stars,
f9NumwFMBDn751xgFiRbNA,	The Range At Lake Norma,	Cornelius	NC	Active Life, Gun/Rifle Ranges, Guns & Ammo, Shopping	28031	36	3.5


## Table Schema

#### Fact Table

| ReviewInfo |

| Column Name | Data Type |
| user_id | string |
| business_id | string |
| review_cool | long |
| review_date | timestamp |
| review_funny | long |
| review_id | string |
| review_stars | double |
| review_text | string |
| review_useful | long |
| business_name | string |
| business_city | string |
| business_state | string |
| business_categories | string |
| business_postal_code | string |
| business_review_count | integer |
| business_stars | double |
| user_average_stars | double |
| user_compliment_cool | long |
| user_compliment_cute | long |
| user_compliment_funny | long |
| user_compliment_hot | long |
| user_compliment_list | long |
| user_compliment_more | long |
| user_compliment_note | long |
| user_compliment_photos | long |
| user_compliment_plain | long |
| user_compliment_profile | long | 
| user_compliment_writer | long |
| user_cool | long |
| user_elite | string |
| user_fans | long |
| user_friends | string |
| user_funny | long |
| user_name | string |
| user_review_count | long |
| user_usefu | long |
| user_yelping_since | timestamp |


#### Dimension Tables

| Business |
| Column Name | Data Type |
| business_id | string |
| business_name | string |
| business_city | string |
| business_categories | string |
| business_stars | double |

| users  |
| --- |
| user_id |
| first_name |
| last_name |
| gender |
| level |
> App users 

| songs   |
| --- |
| song_id |
| title |
| artist_id |
| year |
| duration |
> Songs in music database

| artists    |
| --- |
| artist_id |
| name |
| location |
| lattitude |
| longitude |
> Artists in music database

| time     |
| --- |
| start_time |
| hour |
| day |
| week |
| month |
| year |
| weekday |

## File descriptions

`data` folder contains partitioned song and log JSON files.

`sql_queries.py` contains SQL queries that are execued by other files.

`create_tables.py` drops and reinitialises the database environment using queries from `sql_queries.py`.

`etl.py` performs the massive data extraction from JSON files in `data` folder, transforms it into the defined types and tables groups then uploads it to the postgres Database.

`Data_Engineering_Cassandra.py` model the data from `event_datafile_new.csv` by 3 different frequently used queries and store the data into a Apache Cassandra Keyspace.

`event_datafile_new.csv` is an extracted log sample file in csv format.

## Running the ETL pipeline for Postgres DB

1  Ensure the `data` folder and all project files are downloaded and that all dependencies are met. Replace the given connection strings in `create_tables.py` and `etl.py` with your own, pointing to a postgres database server you have set up.Replace the JSON data file location in `etl.py` with your own loacl directory.

2  Run `create_tables.py` to reinitialise the database.

3  Run `etl.py` to start the ETL data pipeline from JSON logs in the `data` folder to the postgres database
