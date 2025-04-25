import boto3
from botocore.config import Config
from pystac_client import Client
from typing import List, Dict, Any
from datetime import datetime
import requests

# S3配置（建议使用环境变量或IAM角色）
S3_BUCKET = "590debucket"
S3_PREFIX = "landsat_scenes"  # S3存储前缀

def get_landsat_scenes(path: str, row: str, start_date: str = "2016-01-01", 
                      end_date: str = "2016-12-31", collection: str = "landsat-c2-l2", 
                      limit: int = 100) -> List[Dict[str, Any]]:
    """获取Landsat场景及签名URL"""
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
    """upload to S3"""
    s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            s3.upload_fileobj(
                Fileobj=response.raw,
                Bucket=S3_BUCKET,
                Key=s3_key
            )
        print(f"✅ 成功上传到 s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ 上传失败 {url}: {str(e)}")

def process_scene_assets(scene: Dict[str, Any]) -> None:
    """处理单个场景的所有资产"""
    scene_id = scene["id"]
    acquisition_date = scene["properties"]["datetime"].split("T")[0]
    
    for band, url in scene["assets"].items():
        # 生成S3路径示例: landsat_scenes/path200/row115/2016-06-01/LC08_L2SP_200115_B1.TIF
        s3_key = f"{S3_PREFIX}/path{scene['properties']['landsat:wrs_path']}/row{scene['properties']['landsat:wrs_row']}/{acquisition_date}/{scene_id}_{band}.TIF"
        upload_to_s3_from_url(url, s3_key)

if __name__ == "__main__":
    # 示例：搜索并下载2016年Path 200/Row 115的数据
    scenes = get_landsat_scenes(
        path="200", 
        row="115", 
        start_date="2016-06-01", 
        end_date="2016-12-31"
    )
    print(f"找到 {len(scenes)} 个场景")
    for scene in scenes:
        print(f"\n处理场景: {scene['id']}")
        process_scene_assets(scene)