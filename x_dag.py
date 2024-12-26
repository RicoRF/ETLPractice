from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime
from x_etl import run_x_etl
from dotenv import load_dotenv
import os

# Loading env file

load_dotenv()

# Getting X credentials
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

# Declaring default arguments for the AirFlow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024,12,26),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=16)
}

# Define DAG object
dag = DAG(
    'x_dag',
    default_args=default_args,
    description="Test ETL using X"
)

# Define ETL task
run_etl = PythonOperator(
    task_id="complete_x_etl",
    python_callable=run_x_etl(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, BEARER_TOKEN),
    dag=dag
)

# Run ETL
run_etl