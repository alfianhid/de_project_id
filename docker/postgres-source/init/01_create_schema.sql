-- 01_create_schema.sql
-- Creates the application schema and enables required extensions.
-- Runs automatically on first container start.

CREATE SCHEMA IF NOT EXISTS app;

-- uuid_generate_v4() used in default values for some tables
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Set default search path so all subsequent scripts find the 'app' schema
ALTER DATABASE deproject_db SET search_path TO app, public;