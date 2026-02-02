-- Database Schema for Regulatory Records Ingestion Pipeline

-- Users table with subscription plans
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    plan VARCHAR(50) NOT NULL CHECK (plan IN ('starter', 'pro', 'team')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Records table for regulatory data
CREATE TABLE records (
    id SERIAL PRIMARY KEY,
    source_key VARCHAR(255) UNIQUE NOT NULL,
    published_at TIMESTAMP NOT NULL,
    title TEXT NOT NULL,
    entity_name_raw VARCHAR(255) NOT NULL,
    entity_name_norm VARCHAR(255) NOT NULL,
    region VARCHAR(10) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    document_url TEXT,
    raw_json JSONB NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    last_source_type TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on source_key for faster lookups
CREATE INDEX idx_records_source_key ON records(source_key);
CREATE INDEX idx_records_content_hash ON records(content_hash);
CREATE INDEX idx_records_entity_name_norm ON records(entity_name_norm);
CREATE INDEX idx_records_region ON records(region);

-- Ingestion runs logging table
CREATE TABLE ingestion_runs (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    records_fetched INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    error TEXT
);

-- Alert rules table
CREATE TABLE alert_rules (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    entity_name_norm VARCHAR(255),
    region VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_at_least_one_filter CHECK (
        entity_name_norm IS NOT NULL OR region IS NOT NULL
    )
);

-- Alert logs table to track triggered alerts
CREATE TABLE alert_logs (
    id SERIAL PRIMARY KEY,
    alert_rule_id INTEGER NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    action_type VARCHAR(20) NOT NULL CHECK (action_type IN ('insert', 'update'))
);

-- Insert sample users
INSERT INTO users (email, plan) VALUES 
    ('starter@example.com', 'starter'),
    ('pro@example.com', 'pro'),
    ('team@example.com', 'team');
