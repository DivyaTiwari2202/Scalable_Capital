from __future__ import annotations

import json
import os
from typing import Optional, Tuple

import duckdb


def capture_corrupt_lines(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    source_name: str,
    max_errors: Optional[int] = None,
) -> int:
    """
    Scan JSONL and insert only corrupted lines into raw_listens.
    Idempotent via PK(source_file, line_number).
    """
    insert_stmt = """
      INSERT OR IGNORE INTO raw_listens(source_file, line_number, raw_json, parsed_ok, error_message)
      VALUES (?, ?, ?, ?, ?)
    """

    bad = 0
    with open(input_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            raw = line.rstrip("\n")
            try:
                json.loads(raw)
            except Exception as e:
                con.execute(insert_stmt, [source_name, i, raw, False, str(e)])
                bad += 1
                if max_errors is not None and bad >= max_errors:
                    break
    return bad


def ingest_export_file(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    source_name: Optional[str] = None,
) -> Tuple[int, int, int]:
    """
    ETL function that reads the export-file and writes data into DuckDB.

    Properties:
    - Idempotent:
        * raw_listens: PK(source_file, line_number) prevents duplicates on rerun
        * track: upsert on track_key (fills missing attrs over time)
        * listens: PK listen_id + ON CONFLICT DO NOTHING prevents duplicate listens
    - Duplicate-safe: duplicates generate the same listen_id and are ignored in fact table
    - Corrupted-safe: corrupted JSON lines are recorded in raw_listens and skipped from facts

    Returns: (rows_staged_valid, new_fact_rows, corrupted_lines_recorded)
    """
    if source_name is None:
        source_name = os.path.basename(input_path)

    con.execute("BEGIN TRANSACTION")
    try:
        # 1) Capture corrupted lines for auditability (writes only bad lines)
        corrupted = capture_corrupt_lines(con, input_path, source_name)

        # 2) Bulk-load valid JSON lines into stg_listens (fast)
        #    We clear this source on rerun to keep staging deterministic.
        con.execute("DELETE FROM stg_listens WHERE source_file = ?", [source_name])

        con.execute(
            """
            INSERT INTO stg_listens
            SELECT
              ? AS source_file,
              user_name::VARCHAR AS user_name,
              listened_at::BIGINT AS listened_at,
              to_timestamp(listened_at::BIGINT) AS listened_at_ts,

              track_metadata.artist_name::VARCHAR AS artist_name,
              track_metadata.track_name::VARCHAR AS track_name,
              track_metadata.release_name::VARCHAR AS release_name,

              COALESCE(recording_msid, track_metadata.additional_info.recording_msid)::VARCHAR AS recording_msid,
              track_metadata.additional_info.artist_msid::VARCHAR AS artist_msid,
              track_metadata.additional_info.release_msid::VARCHAR AS release_msid,

              -- Deterministic track key
              sha256(
                COALESCE(COALESCE(recording_msid, track_metadata.additional_info.recording_msid)::VARCHAR, '') || '|' ||
                COALESCE(track_metadata.additional_info.artist_msid::VARCHAR, '') || '|' ||
                COALESCE(track_metadata.additional_info.release_msid::VARCHAR, '') || '|' ||
                COALESCE(track_metadata.artist_name::VARCHAR, '') || '|' ||
                COALESCE(track_metadata.track_name::VARCHAR, '') || '|' ||
                COALESCE(track_metadata.release_name::VARCHAR, '')
              ) AS track_key,

              -- Deterministic listen id for idempotency/deduplication
              sha256(
                COALESCE(user_name::VARCHAR, '') || '|' ||
                (listened_at::BIGINT)::VARCHAR || '|' ||
                sha256(
                  COALESCE(COALESCE(recording_msid, track_metadata.additional_info.recording_msid)::VARCHAR, '') || '|' ||
                  COALESCE(track_metadata.additional_info.artist_msid::VARCHAR, '') || '|' ||
                  COALESCE(track_metadata.additional_info.release_msid::VARCHAR, '') || '|' ||
                  COALESCE(track_metadata.artist_name::VARCHAR, '') || '|' ||
                  COALESCE(track_metadata.track_name::VARCHAR, '') || '|' ||
                  COALESCE(track_metadata.release_name::VARCHAR, '')
                )
              ) AS listen_id
            FROM read_json_auto(
              ?,
              format='newline_delimited',
              ignore_errors=true
            )
            WHERE user_name IS NOT NULL AND listened_at IS NOT NULL
            """,
            [source_name, input_path],
        )

        staged = con.execute(
            "SELECT COUNT(*) FROM stg_listens WHERE source_file = ?", [source_name]
        ).fetchone()[0]

        # 3) Upsert track dimension
        con.execute(
            """
            INSERT INTO track(track_key, recording_msid, artist_msid, release_msid, artist_name, track_name, release_name)
            SELECT DISTINCT
              track_key,
              recording_msid,
              artist_msid,
              release_msid,
              artist_name,
              track_name,
              release_name
            FROM stg_listens
            WHERE source_file = ?
            ON CONFLICT(track_key) DO UPDATE SET
              recording_msid = COALESCE(excluded.recording_msid, track.recording_msid),
              artist_msid    = COALESCE(excluded.artist_msid, track.artist_msid),
              release_msid   = COALESCE(excluded.release_msid, track.release_msid),
              artist_name    = COALESCE(excluded.artist_name, track.artist_name),
              track_name     = COALESCE(excluded.track_name, track.track_name),
              release_name   = COALESCE(excluded.release_name, track.release_name)
            """,
            [source_name],
        )

        # 4) Insert listen events (fact) idempotently
        before = con.execute("SELECT COUNT(*) FROM listens").fetchone()[0]
        con.execute(
            """
            INSERT INTO listens(listen_id, user_name, listened_at, listened_at_ts, track_key, source_file)
            SELECT
              listen_id,
              user_name,
              listened_at,
              listened_at_ts,
              track_key,
              source_file
            FROM stg_listens
            WHERE source_file = ?
            ON CONFLICT(listen_id) DO NOTHING
            """,
            [source_name],
        )
        after = con.execute("SELECT COUNT(*) FROM listens").fetchone()[0]

        con.execute("COMMIT")
        return int(staged), int(after - before), int(corrupted)
    except Exception:
        con.execute("ROLLBACK")
        raise