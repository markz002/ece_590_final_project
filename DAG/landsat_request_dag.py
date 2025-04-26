from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
from my_noaa_api.Landsat_s3 import get_landsat_scenes, process_scene_assets
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../my_noaa_api'))
from app import parse_scene_id, save_landsat_scene

# --- Default args for the DAG ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
with DAG(
    dag_id="landsat_request_dag",
    default_args=default_args,
    description="Landsat Data Ingestion Pipeline (PostgreSQL)",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["landsat", "ingestion"],
) as dag:

    def search_landsat(**context):
        conf = context["dag_run"].conf
        path = conf["path"]
        row = conf["row"]
        start_date = conf.get("start_date", "2016-01-01")
        end_date = conf.get("end_date", "2016-12-31")
        collection = conf.get("collection", "landsat-c2-l2")
        limit = conf.get("limit", 10)
        scenes = get_landsat_scenes(path, row, start_date, end_date, collection, limit)
        context["ti"].xcom_push(key="landsat_scenes", value=scenes)

    def store_metadata_and_upload(**context):
        scenes = context["ti"].xcom_pull(key="landsat_scenes")
        inserted_count = 0
        skipped_count = 0
        for scene in scenes:
            try:
                parsed = parse_scene_id(scene["id"])
                inserted = save_landsat_scene(parsed)
                if inserted:
                    inserted_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                print(f"Failed to save scene {scene['id']}: {str(e)}")
            # Upload all assets for the scene
            process_scene_assets(scene)
        print(f" Landsat DAG: {inserted_count} scenes inserted, {skipped_count} scenes skipped.")

    search_task = PythonOperator(
        task_id="search_landsat",
        python_callable=search_landsat,
        provide_context=True
    )

    store_task = PythonOperator(
        task_id="store_metadata_and_upload",
        python_callable=store_metadata_and_upload,
        provide_context=True
    )

    search_task >> store_task