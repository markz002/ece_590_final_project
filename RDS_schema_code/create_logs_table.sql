CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    request_type TEXT NOT NULL, -- e.g., "/noaa/data"
    dataset TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSONB
);

-- Tracks requests made to the API for auditing/debugging purposes.