-- Migration: Create project_external_connections table
-- Run against the default project Postgres database.

CREATE TABLE IF NOT EXISTS project_external_connections (
    id BIGSERIAL PRIMARY KEY,
    project_name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    s3_config JSONB,
    database_config JSONB,
    qdrant_config JSONB,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS pec_project_name_idx
    ON project_external_connections USING hash (project_name);
