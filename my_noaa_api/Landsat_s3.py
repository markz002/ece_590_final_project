import boto3
from botocore.config import Config
from pystac_client import Client
from typing import List, Dict, Any
from datetime import datetime
import requests
import psycopg2
from planetary_computer import sign
import os
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# S3 configuration (recommended to use environment variables or IAM roles)
S3_BUCKET = os.getenv("S3_BUCKET", "590debucket")
S3_PREFIX = os.getenv("S3_PREFIX", "landsat_scenes")  # Default only for prefix

# --- Configure S3 client with short timeouts and retries ---
s3_config = Config(
    connect_timeout=5,  # seconds
    read_timeout=10,    # seconds
    retries={
        'max_attempts': 2,  # fail fast
        'mode': 'standard'
    }
)
s3 = boto3.client("s3", config=s3_config)


def get_db_conn():
    # Set connect_timeout for psycopg2 (in seconds)
    return psycopg2.connect(connect_timeout=5, **DB_CONFIG)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mydatabase.cpeqs8o8koho.us-east-2.rds.amazonaws.com"),
    "database": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "590degroup5")
}


def get_landsat_scenes(path: str, row: str, start_date: str = "2016-01-01", 
                      end_date: str = "2016-12-31", collection: str = "landsat-c2-l2", 
                      limit: int = 100) -> List[Dict[str, Any]]:
    """Get Landsat scenes and signed URLs"""
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    search = catalog.search(
        collections=[collection],
        query={"landsat:wrs_path": {"eq": int(path)}, "landsat:wrs_row": {"eq": int(row)}},
        datetime=f"{start_date}/{end_date}",
        limit=limit
    )
    return [{
        "id": item.id,
        "properties": dict(item.properties.items()),
        "assets": {key: asset.href for key, asset in sign(item).assets.items()}
    } for item in search.get_items()]


def get_landsat_scenes_bbox(min_lon: float, min_lat: float, max_lon: float, max_lat: float, start_date: str = "2016-01-01", end_date: str = "2016-12-31", collection: str = "landsat-c2-l2", limit: int = 100) -> List[Dict[str, Any]]:
    """Get Landsat scenes using bounding box search and sign URLs."""
    catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    search = catalog.search(
        collections=[collection],
        bbox=[min_lon, min_lat, max_lon, max_lat],
        datetime=f"{start_date}/{end_date}",
        limit=limit
    )
    return [{
        "id": item.id,
        "properties": dict(item.properties.items()),
        "assets": {key: asset.href for key, asset in sign(item).assets.items()}
    } for item in search.get_items()]


def upload_to_s3_from_url(url: str, s3_key: str) -> None:
    """Upload to S3"""
    try:
        # Add a short timeout to requests.get
        with requests.get(url, stream=True, timeout=10) as response:
            response.raise_for_status()
            s3.upload_fileobj(
                Fileobj=response.raw,
                Bucket=S3_BUCKET,
                Key=s3_key
            )
        print(f"Successfully uploaded to s3://{S3_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        print(f"Upload failed {url}: {str(e)}")
        return False


def update_landsat_s3link(scene_id: str, s3link: str) -> None:
    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE landsat_scenes
            SET s3link = %s
            WHERE scene_id = %s
        """, (s3link, scene_id))
        conn.commit()
        print(f"Updated s3link for scene {scene_id}")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error updating s3link for {scene_id}: {str(e)}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def process_scene_assets(scene: Dict[str, Any]) -> None:
    """Process all assets of a single scene and update the main s3link"""
    scene_id = scene["id"]
    acquisition_date = scene["properties"]["datetime"].split("T")[0]
    
    updated = False  # Only update s3link once

    for band, url in scene["assets"].items():
        # Skip virtual/visualization assets if needed
        if band.lower() in ["tilejson", "rendered_preview"]:
            continue
        # Extract filename from URL (before any query string)
        filename = os.path.basename(urlparse(url).path)
        s3_key = f"{S3_PREFIX}/path{scene['properties']['landsat:wrs_path']}/row{scene['properties']['landsat:wrs_row']}/{acquisition_date}/{filename}"
        s3_link = upload_to_s3_from_url(url, s3_key)
        if s3_link:
            print(f"Uploaded {band} for scene {scene_id} -> {s3_key}")
            if not updated:
                update_landsat_s3link(scene_id, s3_key)
                updated = True


if __name__ == "__main__":
    # Example: Search and download data for Path 200/Row 115 in 2016
    scenes = get_landsat_scenes(
        path="200", 
        row="115", 
        start_date="2016-06-01", 
        end_date="2016-12-31"
    )
    print(f"Found {len(scenes)} scenes")
    for scene in scenes:
        print(f"\nProcessing scene: {scene['id']}")
        process_scene_assets(scene)