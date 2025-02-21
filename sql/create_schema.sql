-- sql/create_schema.sql

-- Drop or create staging tables (if needed).
-- For final usage, you might keep just "IF NOT EXISTS" or separate "DROP" scripts.

CREATE TABLE IF NOT EXISTS stg_listens (
    user_id      TEXT,
    track_id     TEXT,
    track_name   TEXT,
    artist_name  TEXT,
    listened_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_listens_dedup (
    user_id      TEXT,
    track_id     TEXT,
    track_name   TEXT,
    artist_name  TEXT,
    listened_at  TIMESTAMP
);

-- Dimension tables
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk      BIGINT,  -- We'll fill with a row_number approach
    user_id      TEXT NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_sk)
);

CREATE TABLE IF NOT EXISTS dim_track (
    track_sk     BIGINT,
    track_id     TEXT NOT NULL,
    track_name   TEXT,
    artist_name  TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (track_sk)
);

-- Fact table
CREATE TABLE IF NOT EXISTS fact_listen (
    listen_sk       BIGINT,
    user_sk         BIGINT NOT NULL,
    track_sk        BIGINT NOT NULL,
    listened_at     TIMESTAMP NOT NULL,
    ingestion_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (listen_sk)
);
