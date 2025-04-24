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

# --- Configuration ---
API_KEY_NAME = "X-API-Key"
API_KEY = os.getenv("API_KEY", "change-me-to-a-secure-key")
NOAA_TOKEN = os.getenv("NOAA_TOKEN", "change-me-to-noaa-token")
NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "your-password")

DB_CONFIG = {
    "host": "mydatabase.cpeqs8o8koho.us-east-2.rds.amazonaws.com",
    "database": "postgres",
    "user": "postgre",
    "password": "590deproject5"
}

app = FastAPI(title="NOAA Data Proxy API")

# --- Pydantic Models ---
class NOAADataResponse(BaseModel):
    metadata: Dict[str, Any]
    results: List[Dict[str, Any]]

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

async def verify_api_key(x_api_key: str = Header(..., alias=API_KEY_NAME)):
    return get_user_info(x_api_key)

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
    uvicorn.run(app, host="0.0.0.0", port=8000)