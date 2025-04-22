# tests/test_app.py
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
import requests
from app import app, API_KEY_NAME, API_KEY, fetch_noaa_data
from datetime import datetime
from unittest.mock import MagicMock

client = TestClient(app)
VALID_HEADERS = {API_KEY_NAME: API_KEY}
INVALID_HEADERS = {API_KEY_NAME: "bad-key"}

# Mock NOAA API JSON response
def mock_noaa_json():
    return {
        "metadata": {"resultset": {"offset": 1, "count": 0, "limit": 25}},
        "results": []
    }

# Helper class for mock response
class DummyResp:
    def __init__(self, status_code=200, json_data=None, text="OK"):
        self.status_code = status_code
        self._json = json_data or mock_noaa_json()
        self.text = text

    def json(self):
        return self._json

# --- Tests ---
def test_no_api_key():
    r = client.get('/noaa/data?datasetid=GHCND&startdate=2020-01-01&enddate=2020-01-02')
    assert r.status_code == 422

def test_invalid_key():
    r = client.get('/noaa/data?datasetid=GHCND&startdate=2020-01-01&enddate=2020-01-02', headers=INVALID_HEADERS)
    assert r.status_code == 401
    assert r.json()["error"] == "Unauthorized: invalid API Key"

def test_fetch_noaa_error(monkeypatch):
    monkeypatch.setattr(requests, 'get', lambda *args, **kwargs: DummyResp(status_code=500, text="Internal Error"))
    with pytest.raises(Exception) as exc:
        fetch_noaa_data('GHCND', '2020-01-01', '2020-01-02')
    assert "NOAA API error" in str(exc.value)

@patch("app.get_user_info", return_value={"id": "test-user"})
@patch("app.log_request")
@patch("app.check_duplicate_noaa", return_value=False)
@patch("app.save_metadata", return_value=True)
@patch("app.trigger_airflow")
def test_get_noaa_success(mock_trigger, mock_save, mock_dup_check, mock_log, mock_user, monkeypatch):
    monkeypatch.setattr(requests, 'get', lambda *args, **kwargs: DummyResp())
    r = client.get('/noaa/data?datasetid=GHCND&startdate=2020-01-01&enddate=2020-01-02', headers=VALID_HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert "metadata" in data and "results" in data
    assert isinstance(data["results"], list)
    mock_trigger.assert_called_once()

@patch("app.get_user_info", return_value={"id": "test-user"})
@patch("app.log_request")
@patch("app.check_duplicate_noaa", return_value=True)
def test_get_noaa_duplicate(mock_log, mock_dup_check, mock_user):
    r = client.get('/noaa/data?datasetid=GHCND&startdate=2020-01-01&enddate=2020-01-02', headers=VALID_HEADERS)
    assert r.status_code == 409
    assert "Duplicate request already stored" in r.json()["error"]

@patch("app.get_user_info", return_value={"id": "test-user"})
@patch("app.get_db_conn")
def test_get_logs_no_filters(mock_conn, mock_user):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        {
            "id": 1,
            "user_id": "test-user",
            "request_type": "/noaa/data",
            "dataset": "GHCND",
            "timestamp": str(datetime.now()),
            "payload": {"datasetid": "GHCND", "startdate": "2020-01-01"}
        }
    ]
    mock_conn.return_value.cursor.return_value = mock_cursor

    r = client.get("/logs", headers=VALID_HEADERS)
    assert r.status_code == 200
    logs = r.json()
    assert isinstance(logs, list)
    assert logs[0]["request_type"] == "/noaa/data"

@patch("app.get_user_info", return_value={"id": "test-user"})
@patch("app.get_db_conn")
def test_get_logs_with_filters(mock_conn, mock_user):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.return_value.cursor.return_value = mock_cursor

    r = client.get("/logs?user_id=test-user&request_type=/noaa/data&start=2020-01-01&end=2020-12-31", headers=VALID_HEADERS)
    assert r.status_code == 200
    logs = r.json()
    assert isinstance(logs, list)
    assert len(logs) == 0

# updated by Mark 4/21

# Update test accordingly to test duplicate issue

# mock airflow trigger

# mock log get with/out filter