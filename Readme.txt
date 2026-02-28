End-to-End Automated Weather ETL Pipeline

Project Overview

This project demonstrates a production-grade Data Engineering pipeline that automates the extraction, transformation, and loading (ETL) of weather data. It utilizes Apache Airflow for orchestration and AWS Cloud Services for scalable, event-driven processing.

Architecture Diagram

Pipeline Workflow

Orchestration (Apache Airflow): A DAG runs on a schedule to fetch real-time weather data from the OpenWeatherMap API.

Raw Storage (Amazon S3): The extracted data is saved as a CSV file in the raw/ folder of an S3 bucket.

Event-Driven Processing (AWS Lambda): An S3 ObjectCreated trigger invokes a Lambda function as soon as the CSV lands.

Transformation & Cleaning: The Lambda function handles schema inconsistencies (like temp_cels vs temp_celsius) and converts the data into a standardized JSON format.

Processed Storage: The cleaned data is stored in a processed/ folder, ready for analytical tools like AWS Glue and Amazon Athena.

Repository Structure
Weather_dag.py: The Airflow DAG defining the extraction and S3 upload logic.

lambda_function.py: Python code for the serverless transformation layer.

iam_policy.json: The Least-Privilege security policy governing AWS resource access.

.gitignore: Prevents sensitive local files and credentials from being committed.

Tech Stack
Orchestration: Apache Airflow (Dockerized).

Compute: AWS Lambda (Python 3.12).

Storage: Amazon S3.

Security: AWS IAM (Custom JSON Policies).

Data Formats: CSV (Raw), JSON (Processed).

How to Deploy
AWS Setup: Create an S3 bucket and an IAM Role with the permissions provided in iam_policy.json.

Lambda: Create a function, paste lambda_function.py, and add an S3 trigger for your bucket.

Airflow: Add the Weather_dag.py to your DAGs folder and configure your aws_default connection and owm_api_key Variable.