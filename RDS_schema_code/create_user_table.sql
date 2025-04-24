CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT UNIQUE NOT NULL,
    api_key TEXT UNIQUE NOT NULL,
    access_scope TEXT NOT NULL DEFAULT 'basic', -- e.g., 'admin', 'researcher', 'basic'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);