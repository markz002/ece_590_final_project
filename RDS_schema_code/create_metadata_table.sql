CREATE TABLE metadata (
    id SERIAL PRIMARY KEY,
    dataset TEXT NOT NULL, -- e.g., 'NOAA'
    metadata JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stores information about each unique data request.
-- for duplication checks, enforce a unique index:

CREATE UNIQUE INDEX unique_noaa_request
ON metadata (dataset, (metadata->>'startdate'), (metadata->>'enddate'))
WHERE dataset = 'NOAA';