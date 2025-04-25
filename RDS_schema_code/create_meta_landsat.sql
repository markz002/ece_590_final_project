drop table if exists landsat_scenes;

CREATE TABLE landsat_scenes (
    id SERIAL PRIMARY KEY,
    scene_id TEXT UNIQUE NOT NULL,       -- 原始场景ID，全字符串
    satellite TEXT NOT NULL,             -- LC08
    processing_level TEXT NOT NULL,      -- L2SR
    wrs_path TEXT NOT NULL,              -- 前3位：200
    wrs_row TEXT NOT NULL,               -- 后3位：115
    acquisition_date DATE NOT NULL,      -- 2016-11-07
    processing_version TEXT NOT NULL,    -- 02
    data_category TEXT NOT NULL,         -- T1 or T2
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
