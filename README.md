# 📊 Big Data Linguistics: Reddit Comment Pipeline

A cloud-based data pipeline and interactive application for ingesting, transforming, and analyzing Reddit comments using AWS, Spark, PostgreSQL, and containerized Python services.

![Status](https://img.shields.io/badge/Status-active-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)
![Python](https://img.shields.io/badge/Python-3.8-blue)
![Dockerized](https://img.shields.io/badge/Dockerized-yes-blue)
![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688?logo=fastapi&logoColor=white)
![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-ff4b4b?logo=streamlit&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791?logo=postgresql&logoColor=white)
![Spark](https://img.shields.io/badge/Spark-3.3.0-orange)
![AWS S3](https://img.shields.io/badge/Storage-S3-yellow?logo=amazon-aws&logoColor=white)
![AWS EMR](https://img.shields.io/badge/Compute-EMR-blue?logo=amazon-aws&logoColor=white)


## 🌐 Application Link

[Click here to access the application!](http://54.234.10.122:8501)

## 📌 Overview
This project processes ~1 billion Reddit comments from raw JSON to structured insights using a modular pipeline architecture. It supports scalable transformation via PySpark on EMR, and eventually serves a web application to interact with the transformed data.

## 🏗️ Architecture
![architecture_diagram.png](https://github.com/lderr4/big-data-linguistics/blob/master/architecture_diagram.png)
#### 1. Torrent Dataset: 
The data is a subset of the following [torrent dataset](https://academictorrents.com/details/ba051999301b109eab37d16f027b3f49ade2de13). 
It contains Reddit Comments that are partitioned by year and month from October 2007 to May 2015. 
In total, the dataset contains ~1.7 billion comments and is about 160 GB compressed in bzip2 format, amounting to over 1 TB uncompressed.
#### 2. Data Ingestion Server / EBS Volume
A C5.large EC2 instance with a 200GB gp3 general purpose volume is used to extract the data and upload it to S3. 
[These commands](https://github.com/lderr4/big-data-linguistics/blob/master/scripts/extract_to_s3/download_script.sh)  setup and download the dataset using the [Transmission](https://transmissionbt.com/) BitTorrent client.
The data is then uploaded to S3 using the [AWS CLI S3 CP command](https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html).
#### 3. Raw Data (S3)
The raw data in S3 is physically partitioned by year and month and compressed to ~160GB in [bzip2](https://en.wikipedia.org/wiki/Bzip2) file format.
#### 4. Spark Jobs (EMR)
There are three Spark jobs that ran in EMR Clusters:
1. **[Convert from bzip2 to parquet](https://github.com/lderr4/big-data-linguistics/tree/master/transform_emr_jobs/convert_to_parquet):** As a preliminary step, files are converted from bzip2 to parquet to take advantage of Spark optimizations.
2. **[Word Frequencies and Counts](https://github.com/lderr4/big-data-linguistics/tree/master/transform_emr_jobs/word_freqs):** A PySpark script is run to get the word counts and frequencies for each word that appears in the dataset, partitioned by year and month.
3. **[Sentiment Analysis](https://github.com/lderr4/big-data-linguistics/tree/master/transform_emr_jobs/sentiment-batch):** The [Vader](https://github.com/cjhutto/vaderSentiment) sentiment model is used to extract the average sentiment of words, partitioned by year and month.
#### 5. Transformed Data (S3)
The results of the Spark jobs are persisted to S3 in parquet format. They retain their partitioning by year and month.
#### 6. Data Ingestion Container (Pyarrow / Pandas)
[These scripts](https://github.com/lderr4/big-data-linguistics/tree/master/load_to_postgres) ingest the transformed data and push it to a local postgres container.
#### 7. PostgreSQL Container
The local Postgres container is used only as a development environment for web application development and for analytical querying.
#### 8. Backend Container (FastAPI)
FastAPI is used inside a Python container for the backend to query Postgres. [Here is the code](https://github.com/lderr4/big-data-linguistics/tree/master/web_app/backend).
#### 9. Frontend Container (Streamlit)
Streamlit is used inside a Python container for the frontend interface. [Here is the code](https://github.com/lderr4/big-data-linguistics/tree/master/web_app/frontend).
#### 10. Analytical Queries (DBeaver)
DBeaver connects to the local postgres database for analytical querying / exploration. [Here are some of the queries](https://github.com/lderr4/big-data-linguistics/tree/master/postgres). 
#### 11. Github Repository
This repository is used to push code to the production webserver and run the frontend and backend containers.
#### 12. Production Database (RDS)
Data is replicated from the local postgres container and pushed to RDS using [this script](https://github.com/lderr4/big-data-linguistics/blob/master/scripts/upload_to_rds.sh).
#### 13. Production Webserver (Lightsail)
The frontend and backend containers run on a Lightsail Ubuntu instance to serve the application. [Here is the setup for the instance](https://github.com/lderr4/big-data-linguistics/blob/master/scripts/webserver_setup.sh).

## 🚧 Future Work
- Add sentiment analysis module
- Deploy with Terraform
- Add CI/CD workflows for ETL jobs
- Integrate language change visualizations
