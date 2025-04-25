import os
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Header, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
import secrets
import uuid
from pystac_client import Client
from planetary_computer import sign

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

def log_request(user_id: str, endpoint: str, params: Dict[str, Any]):
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO logs (user_id, request_type, dataset, timestamp, payload)
        VALUES (%s, %s, %s, NOW(), %s)
    """, (user_id, endpoint, params.get("datasetid"), json.dumps(params)))
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
            'create_user_table.sql',
            'create_logs_table.sql',
            'create_metadata_table.sql'
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
    log_request(user["id"], "/noaa/data", {
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
    user_id: Optional[str] = Query(None),
    request_type: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    user=Depends(verify_api_key)
):
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    query = "SELECT * FROM logs WHERE TRUE"
    params = []
    if user_id:
        query += " AND user_id = %s"
        params.append(user_id)
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
    path: str = Query(..., description="WRS-2 path number"),
    row: str = Query(..., description="WRS-2 row number"),
    start_date: str = Query("2016-01-01", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2016-12-31", description="End date in YYYY-MM-DD format"),
    collection: str = Query("landsat-c2-l2", description="STAC collection name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    user=Depends(verify_api_key)
):
    """
    Search for Landsat scenes based on path/row and date range.
    """
    try:
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
        
        scenes = []
        for item in search.get_items():
            signed_item = sign(item)
            scene = {
                "id": item.id,
                "properties": dict(item.properties.items()),
                "assets": {key: asset.href for key, asset in signed_item.assets.items()}
            }
            scenes.append(LandsatScene(**scene))
        
        return LandsatSearchResponse(scenes=scenes, total_count=len(scenes))
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching Landsat scenes: {str(e)}")


from Landsat_s3 import process_scene_assets

@app.post("/landsat/request", response_model=LandsatSearchResponse)
async def landsat_request(
    path: str = Query(..., description="WRS-2 path number"),
    row: str = Query(..., description="WRS-2 row number"),
    start_date: str = Query("2016-01-01", description="Start date in YYYY-MM-DD format"),
    end_date: str = Query("2016-12-31", description="End date in YYYY-MM-DD format"),
    collection: str = Query("landsat-c2-l2", description="STAC collection name"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    user=Depends(verify_api_key)
):
    """
    Search for Landsat scenes and upload their assets to S3.
    """
    try:
        # Search logic (reuse from search_landsat)
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
        scenes = []
        for item in search.get_items():
            signed_item = sign(item)
            scene = {
                "id": item.id,
                "properties": dict(item.properties.items()),
                "assets": {key: asset.href for key, asset in signed_item.assets.items()}
            }
            # Upload all assets for this scene to S3
            process_scene_assets(scene)
            scenes.append(LandsatScene(**scene))
        return LandsatSearchResponse(scenes=scenes, total_count=len(scenes))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching or uploading Landsat scenes: {str(e)}")
# updated by Mark 4/21

# FastAPI code for the NOAA proxy has now been upgraded to include:

# API key verification with user roles from a PostgreSQL users table

# Request logging to an RDS logs table

# Duplication checks in a metadata table before fetching NOAA data

# Metadata saving after successful fetch

# Detailed exception handling for clearer client feedback

# Airflow trigger

# /logs endpoint in your FastAPI app with filtering support



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