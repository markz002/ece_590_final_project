from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import os
import requests
import json
import boto3
import psycopg2
from psycopg2.extras import Json

# --- Configuration ---
NOAA_TOKEN = os.getenv("NOAA_TOKEN", "change-me-to-noaa-token")
S3_BUCKET = "your-s3-bucket-name"
DB_CONFIG = {
    "host": "mydatabase.cpeqs8o8koho.us-east-2.rds.amazonaws.com",
    "database": "postgres",
    "user": "postgre",
    "password": "590deproject5"
}

# --- Default args for the DAG ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
dag = DAG(
    dag_id="noaa_ingest_dag",
    default_args=default_args,
    description="NOAA Data Ingestion Pipeline",
    schedule=None,
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
    tags=["noaa", "ingestion"],
)

with dag:

    def fetch_noaa_data(**context):
        conf = context["dag_run"].conf
        datasetid = conf["datasetid"]
        startdate = conf["startdate"]
        enddate = conf["enddate"]
        limit = conf.get("limit", 1000)

        headers = {"token": NOAA_TOKEN}
        params = {
            "datasetid": datasetid,
            "startdate": startdate,
            "enddate": enddate,
            "limit": limit
        }
        url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            raise Exception(f"Failed NOAA request: {resp.text}")

        data = resp.json()
        context["ti"].xcom_push(key="noaa_data", value=data)
        context["ti"].xcom_push(key="metadata", value={
            "dataset": datasetid,
            "startdate": startdate,
            "enddate": enddate,
            "limit": limit
        })

    def upload_to_s3(**context):
        data = context["ti"].xcom_pull(key="noaa_data")
        metadata = context["ti"].xcom_pull(key="metadata")

        s3 = boto3.client("s3")
        key = f"noaa/{metadata['dataset']}_{metadata['startdate']}_{metadata['enddate']}.json"
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(data))
        context["ti"].xcom_push(key="s3_key", value=key)

    def save_metadata(**context):
        metadata = context["ti"].xcom_pull(key="metadata")
        s3_key = context["ti"].xcom_pull(key="s3_key")

        metadata["s3_key"] = s3_key
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO metadata (dataset, metadata)
            VALUES (%s, %s)
        """, (metadata["dataset"], Json(metadata)))
        conn.commit()
        cur.close()
        conn.close()

    fetch_task = PythonOperator(
        task_id="fetch_noaa_data",
        python_callable=fetch_noaa_data
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    save_task = PythonOperator(
        task_id="save_metadata",
        python_callable=save_metadata
    )

    fetch_task >> upload_task >> save_task
