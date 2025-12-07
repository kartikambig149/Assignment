CREATE TABLE IF NOT EXISTS stock_quotes (id SERIAL PRIMARY KEY, symbol TEXT, timestamp TIMESTAMP, open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume BIGINT);
