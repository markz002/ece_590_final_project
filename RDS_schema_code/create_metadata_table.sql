drop table if exists metadata;

CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    dataset TEXT NOT NULL, -- e.g., 'NOAA'
    metadata JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE UNIQUE INDEX unique_noaa_request
ON metadata (dataset, (metadata->>'startdate'), (metadata->>'enddate'))
WHERE dataset = 'NOAA';