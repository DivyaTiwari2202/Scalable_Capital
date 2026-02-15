from __future__ import annotations

import json
from typing import Optional
import duckdb


def capture_corrupt_lines(
    con: duckdb.DuckDBPyConnection,
    input_path: str,
    source_name: str,
    max_errors: Optional[int] = None,
) -> int:
    """
    Scans JSONL and inserts only corrupted lines into raw_listens.
    Returns number of corrupted lines found.
    """
    stmt = """
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
                con.execute(stmt, [source_name, i, raw, False, str(e)])
                bad += 1
                if max_errors is not None and bad >= max_errors:
                    break
    return bad