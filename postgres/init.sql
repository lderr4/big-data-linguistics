CREATE SCHEMA IF NOT EXISTS reddit_linguistics;
SET search_path TO reddit_linguistics;

CREATE TABLE IF NOT EXISTS words (
    id SERIAL PRIMARY KEY,
    word TEXT,
    count INTEGER,
    frequency DOUBLE PRECISION,
    avg_sentiment DOUBLE PRECISION,
    stddev_sentiment DOUBLE PRECISION,
    year_month DATE, -- Year-month as date type (e.g., '2014-01-01')
    UNIQUE(word, year_month)
);
