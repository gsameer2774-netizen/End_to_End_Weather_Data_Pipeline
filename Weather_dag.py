from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



# Docker path: This maps to your Windows project folder's 'dags' directory
OUTPUT_PATH = '/opt/airflow/data/weather_data.csv'

def extract_weather(**kwargs):
    api_key = Variable.get("owm_api_key")
    URL = f"https://api.openweathermap.org/data/2.5/weather?q=London&appid={api_key}"
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API Error: {response.status_code}")

def transform_weather(ti, **kwargs):
    raw_data = ti.xcom_pull(task_ids='extract_weather')
    transformed = {
        'city' : raw_data.get('name'),
        'temp_celsius' : raw_data.get('main', {}).get('temp'),
        'humidity' : raw_data.get('main', {}).get('humidity'),    
        'description' : raw_data.get('weather', [{}])[0].get('description') if raw_data.get('weather') else None,    
        'timestamp' : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    return transformed

def load_weather_to_s3(ti, **kwargs):
    transformed_data = ti.xcom_pull(task_ids='transform_weather')
    df = pd.DataFrame([transformed_data])
    csv_data = df.to_csv(index = False)

    hook = S3Hook(aws_conn_id= 'aws_default')
    bucket_name = 'idris-weather-data-pipeline'   
    key = f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    try:
        hook.load_string(
            string_data = csv_data,
            key = key,
            bucket_name = bucket_name,
            replace = True
        )
        print(f"Successfully uploaded {key} to S3 bucket {bucket_name}")
    except Exception as e:
        print(f"Error uploading to s3: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='weather_pipeline_owm',
    default_args=default_args,
    description='Weather pipeline using OpenWeatherMap API',
    schedule='@hourly',  # FIXED: Removed '_interval' for Airflow 3.x compatibility
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_weather', python_callable=extract_weather)
    t2 = PythonOperator(task_id='transform_weather', python_callable=transform_weather)
    t3 = PythonOperator(task_id='load_weather', python_callable=load_weather_to_s3)

    t1 >> t2 >> t3