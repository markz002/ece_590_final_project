CREATE TABLE IF NOT EXISTS landsat_scenes (
    id SERIAL PRIMARY KEY,
    scene_id TEXT UNIQUE NOT NULL,
    satellite TEXT NOT NULL,
    processing_level TEXT NOT NULL,
    wrs_path TEXT NOT NULL,
    wrs_row TEXT NOT NULL,
    acquisition_date DATE NOT NULL,
    processing_version TEXT NOT NULL,
    data_category TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    s3link TEXT
);
