import boto3
from botocore.config import Config
from planetary_computer import sign  
from pystac_client import Client
from typing import List, Dict, Any
from datetime import datetime
import requests
import psycopg2

# S3 configuration
S3_BUCKET = "590debucket"
S3_PREFIX = "landsat_scenes"  

# AWS configuration
AWS_ACCESS_KEY = "AKIATS75TQG6K3WVJZCX"  
AWS_SECRET_KEY = "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"  
REGION = "us-east-2"
BUCKET = "590debucket"

# Database configuration
DB_CONFIG = {
    "host": "mydatabase.cpeqs8o8koho.us-east-2.rds.amazonaws.com",
    "database": "postgres",
    "user": "postgres",
    "password": "590degroup5"
}

import pystac_client
import shapely.geometry
from typing import List, Dict, Tuple

def get_landsat_pathrows_from_bbox(
    min_lon: float,
    min_lat: float,
    max_lon: float,
    max_lat: float,
    collection: str = "landsat-c2-l2"  # Landsat Collection 2 Level-2
) -> Dict[Tuple[int, int], str]:
    """
    根据输入的经纬度范围（最小经度、最小纬度、最大经度、最大纬度），
    返回该范围内所有 Landsat Path/Row 的键值对。

    Args:
        min_lon (float): 最小经度（WGS84）
        min_lat (float): 最小纬度（WGS84）
        max_lon (float): 最大经度（WGS84）
        max_lat (float): 最大纬度（WGS84）
        collection (str): STAC 数据集名称（默认 "landsat-c2-l2"）

    Returns:
        Dict[Tuple[int, int], str]: 返回格式如 {(path, row): "path-row"} 的字典
    """
    # 1. 连接 Microsoft Planetary Computer STAC API
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    # 2. 构建查询范围（GeoJSON 格式）
    bbox = [min_lon, min_lat, max_lon, max_lat]
    search_geom = shapely.geometry.box(*bbox)

    # 3. 查询 Landsat 数据，获取所有匹配的 Path/Row
    search_results = catalog.search(
        collections=[collection],
        intersects=search_geom
    )

    # 4. 提取所有唯一的 Path/Row
    pathrows = set()
    for item in search_results.get_all_items():
        path = item.properties["landsat:wrs_path"]
        row = item.properties["landsat:wrs_row"]
        pathrows.add((path, row))

    # 5. 返回格式化的字典
    return {
        (path, row): f"{path:03d}-{row:03d}"
        for path, row in pathrows
    }

def get_landsat_scenes(path: str, row: str, start_date: str = "2016-01-01", 
                      end_date: str = "2016-12-31", collection: str = "landsat-c2-l2", 
                      limit: int = 100) -> List[Dict[str, Any]]:
    """get Landsat scenes and URL"""
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

def upload_to_s3_from_url(url: str, s3_key: str) -> None:
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION,
        config=Config(signature_version='s3v4')
    )
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            s3.upload_fileobj(
                Fileobj=response.raw,
                Bucket=BUCKET,
                Key=s3_key
            )
        print(f"✅Success: s3://{BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌Failed: {str(e)}")


def process_scene_assets(scene: Dict[str, Any]) -> None:
    """deal with one single scene"""
    scene_id = scene["id"]
    acquisition_date = scene["properties"]["datetime"].split("T")[0]
    
    for band, url in scene["assets"].items():
        # S3 path example: landsat_scenes/path200/row115/2016-06-01/LC08_L2SP_200115_B1.TIF
        s3_key = f"{S3_PREFIX}/path{scene['properties']['landsat:wrs_path']}/row{scene['properties']['landsat:wrs_row']}/{acquisition_date}/{scene_id}_{band}.TIF"
        upload_to_s3_from_url(url, s3_key)

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def parse_scene_id(scene_id: str) -> dict:
    parts = scene_id.split("_")
    if len(parts) != 6:
        raise ValueError(f"Invalid Landsat scene ID format: {scene_id}")
    return {
        "scene_id": scene_id,
        "satellite": parts[0],
        "processing_level": parts[1],
        "wrs_path": parts[2][:3],
        "wrs_row": parts[2][3:],
        "acquisition_date": datetime.strptime(parts[3], "%Y%m%d").date(),
        "processing_version": parts[4],
        "data_category": parts[5]
    }

def save_landsat_scene(parsed: Dict[str, Any]) -> bool:
    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1 FROM landsat_scenes WHERE scene_id = %s
        """, (parsed["scene_id"],))
        
        if cursor.fetchone() is not None:
            # scene did exist
            print(f"Scene {parsed['scene_id']} already exists in database, skipping...")
            return False
        
        # scene didn't exist
        cursor.execute("""
            INSERT INTO landsat_scenes (
                scene_id, satellite, processing_level,
                wrs_path, wrs_row, acquisition_date,
                processing_version, data_category
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            parsed["scene_id"], parsed["satellite"], parsed["processing_level"],
            parsed["wrs_path"], parsed["wrs_row"], parsed["acquisition_date"],
            parsed["processing_version"], parsed["data_category"]
        ))
        
        conn.commit()
        print(f"Successfully inserted scene {parsed['scene_id']} into database")
        return True  
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error inserting Landsat scene {parsed['scene_id']}: {str(e)}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    # example: 2016, Path 200/Row 115
    # scenes = get_landsat_scenes(
    #     path="200", 
    #     row="115", 
    #     start_date="2016-01-01", 
    #     end_date="2016-08-31"
    # )
    # print(f"Find {len(scenes)} scenes...")
    # for scene in scenes:
    #     print(f"\nDealing the scene: {scene['id']}")
    #     save_scene = parse_scene_id(scene['id'])
    #     save_success = save_landsat_scene(save_scene)
    #     if save_success:
    #         process_scene_assets(scene)
                

    # North Carolina range
    # "min_lon": -84.3219
    # "min_lat": 33.8361
    # "max_lon": -75.4606
    # "max_lat": 36.5882 
    min_lon = float(input("minimum longtitude: "))
    min_lat = float(input("minimum latitude: "))
    max_lon = float(input("maximum longtitude: "))
    max_lat = float(input("minimum latitude: "))

    pathrows = get_landsat_pathrows_from_bbox(min_lon, min_lat, max_lon, max_lat)

    print("\n该范围内的 Landsat Path/Row:")
    for (path, row), pathrow_str in pathrows.items():
        print(f"Path {path}, Row {row} -> {pathrow_str}")
        