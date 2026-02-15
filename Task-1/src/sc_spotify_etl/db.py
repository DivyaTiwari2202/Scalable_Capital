from __future__ import annotations

import duckdb

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_listens (
  source_file VARCHAR,
  line_number BIGINT,
  raw_json VARCHAR,
  parsed_ok BOOLEAN,
  error_message VARCHAR,
  ingested_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY(source_file, line_number)
);

CREATE TABLE IF NOT EXISTS stg_listens (
  source_file VARCHAR,
  user_name VARCHAR,
  listened_at BIGINT,
  listened_at_ts TIMESTAMP,

  artist_name VARCHAR,
  track_name VARCHAR,
  release_name VARCHAR,

  recording_msid VARCHAR,
  artist_msid VARCHAR,
  release_msid VARCHAR,

  track_key VARCHAR,
  listen_id VARCHAR
);

CREATE TABLE IF NOT EXISTS track (
  track_key VARCHAR PRIMARY KEY,
  recording_msid VARCHAR,
  artist_msid VARCHAR,
  release_msid VARCHAR,
  artist_name VARCHAR,
  track_name VARCHAR,
  release_name VARCHAR
);

CREATE TABLE IF NOT EXISTS listens (
  listen_id VARCHAR PRIMARY KEY,
  user_name VARCHAR NOT NULL,
  listened_at BIGINT NOT NULL,
  listened_at_ts TIMESTAMP,
  track_key VARCHAR NOT NULL,
  source_file VARCHAR,
  ingested_at TIMESTAMP DEFAULT now(),
  FOREIGN KEY(track_key) REFERENCES track(track_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_user ON listens(user_name);
CREATE INDEX IF NOT EXISTS idx_listensed_at ON listens(listened_at);
"""


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(SCHEMA_SQL)