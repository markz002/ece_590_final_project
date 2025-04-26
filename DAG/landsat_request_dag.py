from airflow import DAG
from airflow.operators.python import PythonOperator
<<<<<<< HEAD
import pendulum
=======
from airflow.utils.dates import days_ago
>>>>>>> 5ee880e (airflow dev123 branch)
from datetime import timedelta
import os
from my_noaa_api.Landsat_s3 import get_landsat_scenes, process_scene_assets
import sys
<<<<<<< HEAD
from itertools import product
sys.path.append(os.path.join(os.path.dirname(__file__), '../my_noaa_api'))
from my_noaa_api.app import parse_scene_id, save_landsat_scene
=======
sys.path.append(os.path.join(os.path.dirname(__file__), '../my_noaa_api'))
from app import parse_scene_id, save_landsat_scene
>>>>>>> 5ee880e (airflow dev123 branch)

# --- Default args for the DAG ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
<<<<<<< HEAD
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
        # Accept either bbox (min_lon, min_lat, max_lon, max_lat) or path/row
        min_lon = conf.get("min_lon", -122.0)
        min_lat = conf.get("min_lat", 36.0)
        max_lon = conf.get("max_lon", -121.0)
        max_lat = conf.get("max_lat", 37.0)
        path = conf.get("path", "200")
        row = conf.get("row", "115")
=======
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
>>>>>>> 5ee880e (airflow dev123 branch)
        start_date = conf.get("start_date", "2016-01-01")
        end_date = conf.get("end_date", "2016-12-31")
        collection = conf.get("collection", "landsat-c2-l2")
        limit = conf.get("limit", 10)
<<<<<<< HEAD
        all_scenes = []
        # Prioritize path/row if provided
        if path is not None and row is not None:
            scenes = get_landsat_scenes(
                path=str(path),
                row=str(row),
                start_date=start_date,
                end_date=end_date,
                collection=collection,
                limit=limit
            )
            all_scenes.extend(scenes)
        elif None not in (min_lon, min_lat, max_lon, max_lat):
            # Use bbox search
            scenes = get_landsat_scenes(
                min_lon=float(min_lon),
                min_lat=float(min_lat),
                max_lon=float(max_lon),
                max_lat=float(max_lat),
                start_date=start_date,
                end_date=end_date,
                collection=collection,
                limit=limit
            )
            all_scenes.extend(scenes)
        else:
            raise ValueError("You must provide either min_lon, min_lat, max_lon, max_lat or both path and row.")
        print(f"[search_landsat] OUTPUT scenes: {all_scenes}")
        context["ti"].xcom_push(key="landsat_scenes", value=all_scenes)

    def store_metadata_and_upload(**context):
        scenes = context["ti"].xcom_pull(task_ids="search_landsat", key="landsat_scenes")
        print(f"[store_metadata_and_upload] INPUT scenes: {scenes}")
        if not scenes:
            print("No scenes found in XCom. Skipping metadata storage and upload.")
            return
=======
        scenes = get_landsat_scenes(path, row, start_date, end_date, collection, limit)
        context["ti"].xcom_push(key="landsat_scenes", value=scenes)

    def store_metadata_and_upload(**context):
        scenes = context["ti"].xcom_pull(key="landsat_scenes")
>>>>>>> 5ee880e (airflow dev123 branch)
        inserted_count = 0
        skipped_count = 0
        for scene in scenes:
            try:
                parsed = parse_scene_id(scene["id"])
<<<<<<< HEAD
                # Check for duplicate by metadata (scene_id)
                if not save_landsat_scene(parsed):
                    print(f"Duplicate scene, skipping metadata insert: {parsed['scene_id']}")
                    skipped_count += 1
                    continue
                inserted_count += 1
=======
                inserted = save_landsat_scene(parsed)
                if inserted:
                    inserted_count += 1
                else:
                    skipped_count += 1
>>>>>>> 5ee880e (airflow dev123 branch)
            except Exception as e:
                print(f"Failed to save scene {scene['id']}: {str(e)}")
            # Upload all assets for the scene
            process_scene_assets(scene)
<<<<<<< HEAD
        print(f"[store_metadata_and_upload] OUTPUT: {inserted_count} scenes inserted, {skipped_count} scenes skipped.")

    search_task = PythonOperator(
        task_id="search_landsat",
        python_callable=search_landsat
=======
        print(f" Landsat DAG: {inserted_count} scenes inserted, {skipped_count} scenes skipped.")

    search_task = PythonOperator(
        task_id="search_landsat",
        python_callable=search_landsat,
        provide_context=True
>>>>>>> 5ee880e (airflow dev123 branch)
    )

    store_task = PythonOperator(
        task_id="store_metadata_and_upload",
<<<<<<< HEAD
        python_callable=store_metadata_and_upload
=======
        python_callable=store_metadata_and_upload,
        provide_context=True
>>>>>>> 5ee880e (airflow dev123 branch)
    )

    search_task >> store_task