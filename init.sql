#---
# init.sql
#---
-- PostgreSQL initialization script
CREATE DATABASE IF NOT EXISTS transaction_db;

-- Connect to the database
\c transaction_db;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    amount_usd DECIMAL(15, 2),
    country VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create FX rates table
CREATE TABLE IF NOT EXISTS fx_rates (
    id SERIAL PRIMARY KEY,
    currency VARCHAR(3) NOT NULL,
    rate_to_usd DECIMAL(10, 6) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create processed transactions table
CREATE TABLE IF NOT EXISTS processed_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    amount_usd DECIMAL(15, 2) NOT NULL,
    country VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    fx_rate DECIMAL(10, 6) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_country ON transactions(country);
CREATE INDEX IF NOT EXISTS idx_transactions_currency ON transactions(currency);
CREATE INDEX IF NOT EXISTS idx_processed_transactions_timestamp ON processed_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_transactions_country ON processed_transactions(country);
CREATE INDEX IF NOT EXISTS idx_processed_transactions_user ON processed_transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_fx_rates_currency_timestamp ON fx_rates(currency, timestamp);

-- Insert sample FX rates
INSERT INTO fx_rates (currency, rate_to_usd, timestamp) VALUES
('EUR', 0.85, NOW()),
('GBP', 0.75, NOW()),
('JPY', 110.0, NOW()),
('THB', 35.0, NOW()),
('SGD', 1.35, NOW()),
('AUD', 1.45, NOW()),
('CAD', 1.25, NOW())
ON CONFLICT DO NOTHING;
