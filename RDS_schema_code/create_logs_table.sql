CREATE TABLE if not exists logs (
    id SERIAL PRIMARY KEY,
    user_email TEXT, -- store user email instead of UUID
    request_type TEXT NOT NULL, -- e.g., "/noaa/data"
    dataset TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);
