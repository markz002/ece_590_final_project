from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import os
from my_noaa_api.Landsat_s3 import get_landsat_scenes, process_scene_assets
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../my_noaa_api'))
from my_noaa_api.app import parse_scene_id, save_landsat_scene

# --- Default args for the DAG ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
dag = DAG(
    dag_id="landsat_request_dag",
    default_args=default_args,
    description="Landsat Data Ingestion Pipeline (PostgreSQL)",
    schedule=None,
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
    tags=["landsat", "ingestion"],
)

with dag:

    def search_landsat(**context):
        conf = context["dag_run"].conf
        print(f"[search_landsat] INPUT conf: {conf}")
        # Set default values for path and row if not provided
        path = conf.get("path")
        row = conf.get("row")
        if path is None or row is None:
            # Use defaults based on search_landsat_output sample
            # path=200, row=115 are common in the sample data
            path = path if path is not None else "200"
            row = row if row is not None else "115"
        start_date = conf.get("start_date", "2016-01-01")
        end_date = conf.get("end_date", "2016-12-31")
        collection = conf.get("collection", "landsat-c2-l2")
        limit = conf.get("limit", 10)
        scenes = get_landsat_scenes(path, row, start_date, end_date, collection, limit)
        print(f"[search_landsat] OUTPUT scenes: {scenes}")
        context["ti"].xcom_push(key="landsat_scenes", value=scenes)

    def store_metadata_and_upload(**context):
        scenes = context["ti"].xcom_pull(key="landsat_scenes")
        print(f"[store_metadata_and_upload] INPUT scenes: {scenes}")
        if not scenes:
            print("No scenes found in XCom. Skipping metadata storage and upload.")
            return
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
        print(f"[store_metadata_and_upload] OUTPUT: {inserted_count} scenes inserted, {skipped_count} scenes skipped.")

    search_task = PythonOperator(
        task_id="search_landsat",
        python_callable=search_landsat
    )

    store_task = PythonOperator(
        task_id="store_metadata_and_upload",
        python_callable=store_metadata_and_upload
    )

    search_task >> store_task