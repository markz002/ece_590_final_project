import os
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Header, HTTPException, Depends, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
import secrets
import uuid
from pystac_client import Client
from planetary_computer import sign
import asyncio
from datetime import datetime, timezone
from Landsat_s3 import process_scene_assets, upload_to_s3_from_url, update_landsat_s3link

# --- Configuration ---
API_KEY_NAME = "X-API-Key"
API_KEY = os.getenv("API_KEY", "your-api-key")
NOAA_TOKEN = os.getenv("NOAA_TOKEN", "change-me-to-noaa-token")
NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "DPWaznMsCWPgAKHp")

DB_CONFIG = {
    "host": "mydatabase.cpeqs8o8koho.us-east-2.rds.amazonaws.com",
    "database": "postgres",
    "user": "postgres",
    "password": "590degroup5"
}

app = FastAPI(title="NOAA Data Proxy API")

# --- Pydantic Models ---
class NOAADataResponse(BaseModel):
    metadata: Dict[str, Any]
    results: List[Dict[str, Any]]

class CreateUserRequest(BaseModel):
    email: str
    access_scope: str = Field(default="basic", description="User access level: admin, researcher, or basic")

class CreateUserResponse(BaseModel):
    user_id: str
    email: str
    api_key: str
    access_scope: str
    created_at: datetime

class LandsatScene(BaseModel):
    id: str
    properties: Dict[str, Any]
    assets: Dict[str, str]

class LandsatSearchResponse(BaseModel):
    scenes: List[LandsatScene]
    total_count: int

# --- Dependencies ---
def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def get_user_info(api_key: str):
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM users WHERE api_key = %s", (api_key,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized: invalid API Key")
    return user

# --- DEVELOPMENT OVERRIDE: Disable API key check ---
async def verify_api_key(x_api_key: str = None):
    # return get_user_info(x_api_key)
    # In development, skip actual API key checking and return a dummy user dict
    return {"id": "dev-user", "role": "developer"}

# --- Helpers ---
def fetch_noaa_data(datasetid: str, startdate: str, enddate: str, limit: int = 25) -> Dict[str, Any]:
    headers = {"token": NOAA_TOKEN}
    params = {
        "datasetid": datasetid,
        "startdate": startdate,
        "enddate": enddate,
        "limit": limit
    }
    url = f"{NOAA_BASE_URL}/data"
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=f"NOAA API error: {resp.text}")
    return resp.json()

def log_request(user_email: str, endpoint: str, params: Dict[str, Any]):
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO logs (user_email, request_type, dataset, timestamp, payload)
        VALUES (%s, %s, %s, NOW(), %s)
    """, (user_email, endpoint, params.get("datasetid"), json.dumps(params)))
    conn.commit()
    cursor.close()
    conn.close()

def check_duplicate_noaa(datasetid: str, startdate: str, enddate: str) -> bool:
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT * FROM metadata
        WHERE dataset = %s AND metadata @> %s::jsonb
    """, (datasetid, json.dumps({"startdate": startdate, "enddate": enddate})))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return len(result) > 0

def save_metadata(dataset: str, metadata: Dict[str, Any]) -> bool:
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO metadata (dataset, metadata)
        VALUES (%s, %s)
    """, (dataset, json.dumps(metadata)))
    conn.commit()
    cursor.close()
    conn.close()
    return True

def trigger_airflow(datasetid: str, startdate: str, enddate: str, limit: int = 100):
    airflow_endpoint = f"{AIRFLOW_URL}/api/v1/dags/noaa_ingest_dag/dagRuns"
    payload = {
        "conf": {
            "datasetid": datasetid,
            "startdate": startdate,
            "enddate": enddate,
            "limit": limit
        }
    }
    response = requests.post(
        airflow_endpoint,
        json=payload,
        auth=(AIRFLOW_USER, AIRFLOW_PASS)
    )
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Airflow trigger failed: {response.text}")

def execute_schema_sql():
    """
    Execute SQL statements from all SQL files in the RDS_schema_code directory.
    This function should be called once during setup.
    SQL files will be executed in the specific order: users, logs, metadata
    """
    try:
        # Connect to the database
        conn = get_db_conn()
        cursor = conn.cursor()
        
        # Get list of SQL files in RDS_schema_code directory
        import os
        schema_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../RDS_schema_code')
        
        # Define the specific order for SQL files
        sql_files_order = [
            # 'create_user_table.sql',
            # 'create_logs_table.sql',
            # 'create_metadata_table.sql',
            'create_meta_landsat.sql'
        ]
        
        # Execute SQL from each file in the specified order
        for sql_file in sql_files_order:
            print(f"Executing SQL from {sql_file}...")
            file_path = os.path.join(schema_dir, sql_file)

            with open(file_path, 'r') as file:
                sql_statements = file.read()
                print(f"SQL statements from {sql_file}: {sql_statements}")
            
            # Split SQL statements by semicolon
            statements = sql_statements.split(';')
            
            # Execute each statement
            for statement in statements:
                if statement.strip():  # Skip empty statements
                    cursor.execute(statement + ';')
        
        # Commit the changes
        conn.commit()
        print("All schema SQL files executed successfully in order: users, logs, metadata")
        
    except Exception as e:
        print(f"Error executing schema SQL: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        

def scene_exists(scene_id: str) -> bool:
    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM landsat_scenes WHERE scene_id = %s LIMIT 1", (scene_id,))
        exists = cursor.fetchone() is not None
        return exists
    except Exception as e:
        print(f"❌ Error checking scene existence {scene_id}: {str(e)}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()


def scene_exists(scene_id: str) -> bool:
    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM landsat_scenes WHERE scene_id = %s LIMIT 1", (scene_id,))
        exists = cursor.fetchone() is not None
        return exists
    except Exception as e:
        print(f"❌ Error checking scene existence {scene_id}: {str(e)}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()


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
        "data_category": parts[5],
        "s3link": None
    }

def save_landsat_scene(parsed: Dict[str, Any]) -> bool:
    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO landsat_scenes (
                scene_id, satellite, processing_level,
                wrs_path, wrs_row, acquisition_date,
                processing_version, data_category,
                s3link
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (scene_id) DO NOTHING
        """, (
            parsed["scene_id"],
            parsed["satellite"],
            parsed["processing_level"],
            parsed["wrs_path"],
            parsed["wrs_row"],
            parsed["acquisition_date"],
            parsed["processing_version"],
            parsed["data_category"],
            parsed.get("s3link", None) 
        ))
        
        conn.commit()
        
        if cursor.rowcount > 0:
            print(f" Inserted scene: {parsed['scene_id']}")
            return True
        else:
            print(f" Skipped existing scene: {parsed['scene_id']}")
            return False

    except Exception as e:
        if conn:
            conn.rollback()
        print(f" Error inserting Landsat scene {parsed.get('scene_id', 'Unknown')}: {str(e)}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()

# --- Proper Logging Middleware ---
@app.middleware("http")
async def db_logging_middleware(request: Request, call_next):
    endpoint = request.url.path
    params = dict(request.query_params)
    if endpoint == "/logs":
        # Do not log /logs endpoint
        return await call_next(request)
    user_email = "anonymous"
    try:
        if hasattr(request, 'state') and hasattr(request.state, 'user') and request.state.user:
            user_email = request.state.user.get("email", "anonymous")
        else:
            api_key = request.headers.get("x-api-key")
            if api_key:
                try:
                    user = get_user_info(api_key)
                    user_email = user.get("email", "anonymous")
                except Exception:
                    user_email = "unauthorized"
    except Exception:
        user_email = "anonymous"
    try:
        log_request(user_email, endpoint, params)
    except Exception as e:
        print(f"Log error: {e}")
    response = await call_next(request)
    return response

# --- Exception Handlers ---
@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(status_code=400, content={"error": "Bad Request", "details": exc.errors()})

# --- API Endpoints ---
@app.get("/noaa/data", response_model=NOAADataResponse)
async def get_noaa_data(
    datasetid: str = Query(..., description="NOAA dataset ID, e.g. GHCND"),
    startdate: str = Query(..., description="Start date YYYY-MM-DD"),
    enddate: str = Query(..., description="End date YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=1000, description="Max records to return"),
    user=Depends(verify_api_key)
):
    log_request(user["email"], "/noaa/data", {
        "datasetid": datasetid,
        "startdate": startdate,
        "enddate": enddate,
        "limit": limit
    })
    if check_duplicate_noaa(datasetid, startdate, enddate):
        raise HTTPException(status_code=409, detail="Duplicate request already stored")
    data = fetch_noaa_data(datasetid, startdate, enddate, limit)
    save_metadata(datasetid, {
        "startdate": startdate,
        "enddate": enddate,
        "limit": limit
    })
    trigger_airflow(datasetid, startdate, enddate, limit)
    return data

@app.post("/users", response_model=CreateUserResponse)
async def create_user(user_data: CreateUserRequest):
    """
    Create a new user with a randomly generated API key.
    
    Returns:
        user_id: UUID of the created user
        email: User's email address
        api_key: Generated API key
        access_scope: User's access level
        created_at: Timestamp of user creation
    """
    try:
        # First check if user already exists
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE email = %s", (user_data.email,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            # If user exists, return their information
            return CreateUserResponse(
                user_id=existing_user[0],
                email=existing_user[1],
                api_key=existing_user[2],
                access_scope=existing_user[3],
                created_at=existing_user[4]
            )
        
        # Generate a secure random API key
        api_key = secrets.token_urlsafe(32)
        
        # Insert new user
        cursor.execute("""
            INSERT INTO users (email, api_key, access_scope)
            VALUES (%s, %s, %s)
            RETURNING id, email, access_scope, created_at
        """, (user_data.email, api_key, user_data.access_scope))
        
        # Get the created user data
        result = cursor.fetchone()
        
        # Commit the transaction
        conn.commit()
        
        # Return response with the generated API key
        return {
            "user_id": str(result[0]),
            "email": result[1],
            "api_key": api_key,
            "access_scope": result[2],
            "created_at": result[3]
        }
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error creating user: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.get("/logs")
async def get_logs(
    user_email: Optional[str] = Query(None),
    request_type: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    user=Depends(verify_api_key)
):
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = "SELECT * FROM logs WHERE TRUE"
    params = []
    if user_email:
        query += " AND user_email = %s"
        params.append(user_email)
    if request_type:
        query += " AND request_type = %s"
        params.append(request_type)
    if start:
        query += " AND timestamp >= %s"
        params.append(start)
    if end:
        query += " AND timestamp <= %s"
        params.append(end)
    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

@app.get("/landsat/search", response_model=LandsatSearchResponse)
async def search_landsat(
    min_lon: float = Query(None, description="Minimum longitude (west)"),
    min_lat: float = Query(None, description="Minimum latitude (south)"),
    max_lon: float = Query(None, description="Maximum longitude (east)"),
    max_lat: float = Query(None, description="Maximum latitude (north)"),
    path: str = Query(None, description="WRS-2 path number"),
    row: str = Query(None, description="WRS-2 row number"),
    start_date: str = Query("2016-01-01", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2016-12-31", description="End date in YYYY-MM-DD format"),
    collection: str = Query("landsat-c2-l2", description="STAC collection name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    user=Depends(verify_api_key)
):
    """
    Search for Landsat scenes by bounding box or path/row. User must provide either bbox or both path and row.
    """
    try:
        catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        # Mode 1: bbox
        if None not in (min_lon, min_lat, max_lon, max_lat):
            bbox = [min_lon, min_lat, max_lon, max_lat]
            search = catalog.search(
                collections=[collection],
                bbox=bbox,
                datetime=f"{start_date}/{end_date}",
                limit=limit
            )
            items = list(search.get_items())
        # Mode 2: path/row
        elif path is not None and row is not None:
            search = catalog.search(
                collections=[collection],
                query={
                    "landsat:wrs_path": {"eq": int(path)},
                    "landsat:wrs_row": {"eq": int(row)}
                },
                datetime=f"{start_date}/{end_date}",
                limit=limit
            )
            items = list(search.get_items())
        else:
            raise HTTPException(status_code=400, detail="You must provide either min_lon, min_lat, max_lon, max_lat or both path and row.")
        scenes = [LandsatScene(
            id=item.id,
            properties=dict(item.properties.items()),
            assets={key: asset.href for key, asset in sign(item).assets.items()}
        ) for item in items]
        return LandsatSearchResponse(scenes=scenes, total_count=len(scenes))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching Landsat scenes: {str(e)}")



from Landsat_s3 import process_scene_assets

@app.post("/landsat/request", response_model=LandsatSearchResponse)
async def landsat_request(
    min_lon: float = Query(None, description="Minimum longitude (west)"),
    min_lat: float = Query(None, description="Minimum latitude (south)"),
    max_lon: float = Query(None, description="Maximum longitude (east)"),
    max_lat: float = Query(None, description="Maximum latitude (north)"),
    path: str = Query(None, description="WRS-2 path number"),
    row: str = Query(None, description="WRS-2 row number"),
    start_date: str = Query("2016-01-01", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2016-12-31", description="End date in YYYY-MM-DD format"),
    collection: str = Query("landsat-c2-l2", description="STAC collection name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    user=Depends(verify_api_key)
):
    """
    Search for Landsat scenes and upload their assets to S3 for all scenes matching the bounding box or path/row. User must provide either bbox or both path and row.
    """
    try:
        all_scene_dicts = []
        inserted_count = 0
        skipped_count = 0
        catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        search = catalog.search(
            collections=[collection],
            query={
                "landsat:wrs_path": {"eq": int(path)},
                "landsat:wrs_row": {"eq": int(row)}
            },
            datetime=f"{start_date}/{end_date}",
            limit=limit
        )
        inserted_count = 0
        skipped_count = 0

        items = list(search.get_items())
        total_count = len(items)
        print(f"Total number of scenes: {total_count}")

        scenes = []         # ✅ 这个收集所有搜索到的
        scenes_to_upload = []  # ✅ 这个只收集需要上传的新的 scene

        for item in items:
            signed_item = sign(item)
            scene = {
                "id": item.id,
                "properties": dict(item.properties.items()),
                "assets": {key: asset.href for key, asset in signed_item.assets.items()}
            }
            scenes.append(scene)   # ✅ 不管是否新scene，全部加到返回列表

            scene_id = item.id

            if scene_exists(scene_id):
                print(f"⚡ Scene {scene_id} already exists, skipping upload and save.")
                skipped_count += 1
                continue  # 不上传，不存数据库，但是加入 scenes

            try:
                parsed = parse_scene_id(scene_id)
                save_landsat_scene(parsed)
                inserted_count += 1
                scenes_to_upload.append(scene)  # ✅ 新的scene要处理
            except Exception as e:
                print(f"❌ Failed to save scene {scene_id}: {str(e)}")
        
        # 🌟 并行上传新的 scenes
        import asyncio
        await asyncio.gather(*(asyncio.to_thread(process_scene_assets, scene) for scene in scenes_to_upload))

        print(f"🌟 Landsat request completed: {inserted_count} scenes inserted and uploaded, {skipped_count} scenes skipped.")

        # ✅ 返回所有 scenes，不管是否新插入
        scenes_response = [LandsatScene(**scene) for scene in scenes]
        return LandsatSearchResponse(scenes=scenes_response, total_count=len(scenes_response))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching or uploading Landsat scenes: {str(e)}")

@app.post("/landsat/dag_request")
async def landsat_dag_request(
    min_lon: float = Query(None, description="Minimum longitude (west)"),
    min_lat: float = Query(None, description="Minimum latitude (south)"),
    max_lon: float = Query(None, description="Maximum longitude (east)"),
    max_lat: float = Query(None, description="Maximum longitude (north)"),
    path: str = Query(None, description="WRS-2 path number"),
    row: str = Query(None, description="WRS-2 row number"),
    start_date: str = Query("2016-01-01", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2016-12-31", description="End date in YYYY-MM-DD format"),
    collection: str = Query("landsat-c2-l2", description="STAC collection name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    user=Depends(verify_api_key)
):
    """
    Trigger the Airflow DAG for Landsat request ingestion with the provided parameters using the Airflow v2 API.
    """
    airflow_url = "http://localhost:8080/api/v2/dags/landsat_request_dag/dagRuns"
    now_iso = datetime.now(timezone.utc).isoformat()
    dag_run_id = f"landsat_request_{now_iso}"
    payload = {
        "dag_run_id": dag_run_id,
        "logical_date": now_iso,
        "conf": {
            "min_lon": min_lon,
            "min_lat": min_lat,
            "max_lon": max_lon,
            "max_lat": max_lat,
            "path": path,
            "row": row,
            "start_date": start_date,
            "end_date": end_date,
            "collection": collection,
            "limit": limit
        },
        "note": "Triggered via landsat_dag_request endpoint"
    }
    # --- Airflow OAuth2 Token Retrieval ---
    airflow_api_url = os.getenv("AIRFLOW_URL", "http://localhost:8080")
    airflow_user = os.getenv("AIRFLOW_USER", "admin")
    airflow_pass = os.getenv("AIRFLOW_PASS", "dpRHnfWubMgtZ3F6")
    token_endpoint = f"{airflow_api_url}/auth/token"
    try:
        token_resp = requests.post(token_endpoint, json={"username": airflow_user, "password": airflow_pass})
        # if token_resp.status_code != 200:
        #     raise HTTPException(status_code=500, detail=f"Failed to get Airflow token: {token_resp.text}")
        try:        
            token_json = token_resp.json()
            airflow_token = token_json.get("access_token")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get Airflow token: {str(e)}")
        if not airflow_token:
            raise HTTPException(status_code=500, detail="No access_token returned from Airflow /auth/token endpoint.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obtaining Airflow token: {str(e)}")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {airflow_token}"
    }
    try:
        response = requests.post(airflow_url, json=payload, headers=headers)
        if response.status_code not in (200, 201):
            raise HTTPException(status_code=response.status_code, detail=f"Airflow DAG trigger failed: {response.text}")
        return {"message": "DAG triggered successfully", "airflow_response": response.json()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger Airflow DAG: {str(e)}")

@app.get("/landsat/retrieve")
async def landsat_retrieve(
    min_lon: float = Query(None, description="Minimum longitude (west)"),
    min_lat: float = Query(None, description="Minimum latitude (south)"),
    max_lon: float = Query(None, description="Maximum longitude (east)"),
    max_lat: float = Query(None, description="Maximum longitude (north)"),
    path: str = Query(None, description="WRS-2 path number"),
    row: str = Query(None, description="WRS-2 row number"),
    start_date: str = Query("2016-01-01", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2016-12-31", description="End date in YYYY-MM-DD format"),
    user=Depends(verify_api_key)
):
    """
    Retrieve S3 links for Landsat scenes matching the query from the metadata database. Prioritize path/row if provided, else use bbox. If all scenes are found, redirect to their S3 links (multi-redirect JSON if >1).
    """
    from itertools import product
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        scenes = []
        # Prioritize path/row
        if path is not None and row is not None:
            query = """
                SELECT scene_id, s3link FROM landsat_scenes
                WHERE wrs_path = %s AND wrs_row = %s
                AND acquisition_date >= %s AND acquisition_date <= %s
            """
            cursor.execute(query, (path, row, start_date, end_date))
            rows = cursor.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="No matching scenes found in metadata database.")
            scenes = rows
        elif None not in (min_lon, min_lat, max_lon, max_lat):
            raise HTTPException(status_code=400, detail="BBox search not supported as landsat_scenes table does not have geometry columns.")
        else:
            raise HTTPException(status_code=400, detail="You must provide either path/row.")
        # Check all scenes have s3link
        missing = [s for s in scenes if not s.get("s3link")]
        if missing:
            raise HTTPException(status_code=404, detail="Some scenes are missing S3 links in metadata database.")
        # Redirect if only one scene, else return JSON with links
        if len(scenes) == 1:
            return RedirectResponse(scenes[0]["s3link"])
        else:
            return JSONResponse({"scene_links": [{"scene_id": s["scene_id"], "s3link": s["s3link"]} for s in scenes]})
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    import argparse
    
    # Create argument parser
    parser = argparse.ArgumentParser(description='NOAA Data Proxy API')
    parser.add_argument('--setup-db', action='store_true',
                      help='Execute database schema setup before starting the server')
    args = parser.parse_args()
    
    # Execute schema SQL if requested
    if args.setup_db:
        execute_schema_sql()
    
    # Start the FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=8000)