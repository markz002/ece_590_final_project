from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import json
from my_noaa_api.Landsat_s3 import get_landsat_scenes, process_scene_assets
from my_noaa_api.landsat_metadata_db import exists_metadata, update_metadata_on_s3_success

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
    description="Landsat Data Ingestion Pipeline",
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

    def lookup_metadata_db(**context):
        scenes = context["ti"].xcom_pull(key="landsat_scenes")
        filtered = []
        for scene in scenes:
            scene_id = scene["id"]
            for band in scene["assets"]:
                s3_key = f"landsat_scenes/path{scene['properties']['landsat:wrs_path']}/row{scene['properties']['landsat:wrs_row']}/{scene['properties']['datetime'].split('T')[0]}/{scene_id}_{band}.TIF"
                if not exists_metadata(scene_id, s3_key):
                    filtered.append((scene, band, s3_key))
        context["ti"].xcom_push(key="to_upload", value=filtered)

    def upload_to_s3(**context):
        to_upload = context["ti"].xcom_pull(key="to_upload")
        uploaded = []
        for scene, band, s3_key in to_upload:
            url = scene["assets"][band]
            # Use process_scene_assets for full scene upload, but here, upload one asset at a time for tracking
            from my_noaa_api.Landsat_s3 import upload_to_s3_from_url
            success = upload_to_s3_from_url(url, s3_key)
            if success:
                uploaded.append({
                    "scene": scene,
                    "band": band,
                    "s3_key": s3_key,
                    "url": url
                })
        context["ti"].xcom_push(key="uploaded", value=uploaded)

    def store_metadata(**context):
        uploaded = context["ti"].xcom_pull(key="uploaded")
        for item in uploaded:
            scene = item["scene"]
            band = item["band"]
            s3_key = item["s3_key"]
            acquisition_date = scene["properties"]["datetime"].split("T")[0]
            metadata = {
                "scene_id": scene["id"],
                "satellite": scene["properties"].get("platform"),
                "wrs_path": scene["properties"]["landsat:wrs_path"],
                "wrs_row": scene["properties"]["landsat:wrs_row"],
                "acquisition_date": acquisition_date,
                "s3_key": s3_key,
                "s3_url": f"s3://590debucket/{s3_key}"
            }
            update_metadata_on_s3_success(metadata)

    search_task = PythonOperator(
        task_id="search_landsat",
        python_callable=search_landsat,
        provide_context=True
    )

    lookup_task = PythonOperator(
        task_id="lookup_metadata_db",
        python_callable=lookup_metadata_db,
        provide_context=True
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True
    )

    store_metadata_task = PythonOperator(
        task_id="store_metadata",
        python_callable=store_metadata,
        provide_context=True
    )

    search_task >> lookup_task >> upload_task >> store_metadata_task