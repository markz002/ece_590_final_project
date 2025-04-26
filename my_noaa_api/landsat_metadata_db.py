import sqlite3
from typing import Dict, Any, Optional
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'landsat_metadata.db')

# Schema for the landsat metadata table
SCHEMA = '''
CREATE TABLE IF NOT EXISTS landsat_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scene_id TEXT NOT NULL,
    satellite TEXT NOT NULL,
    wrs_path TEXT NOT NULL,
    wrs_row TEXT NOT NULL,
    acquisition_date TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    s3_url TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scene_id, s3_key)
);
'''

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(SCHEMA)
    conn.commit()
    conn.close()


def insert_metadata(metadata: Dict[str, Any]):
    """
    Insert new metadata after successful S3 upload. Expects keys: scene_id, satellite, wrs_path, wrs_row, acquisition_date, s3_key, s3_url
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO landsat_metadata (scene_id, satellite, wrs_path, wrs_row, acquisition_date, s3_key, s3_url)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''',
                  (
                      metadata['scene_id'],
                      metadata['satellite'],
                      metadata['wrs_path'],
                      metadata['wrs_row'],
                      metadata['acquisition_date'],
                      metadata['s3_key'],
                      metadata['s3_url']
                  ))
        conn.commit()
    except sqlite3.IntegrityError:
        # Redundant entry
        pass
    finally:
        conn.close()


def exists_metadata(scene_id: str, s3_key: str) -> bool:
    """
    Check if a record with the same scene_id and s3_key exists (redundancy check).
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT 1 FROM landsat_metadata WHERE scene_id = ? AND s3_key = ?''', (scene_id, s3_key))
    exists = c.fetchone() is not None
    conn.close()
    return exists


def update_metadata_on_s3_success(metadata: Dict[str, Any]):
    """
    Redundancy check, then update metadata DB after S3 upload success.
    Returns True if inserted, False if redundant.
    """
    if exists_metadata(metadata['scene_id'], metadata['s3_key']):
        return False
    insert_metadata(metadata)
    return True


# Call this once at startup
init_db()
