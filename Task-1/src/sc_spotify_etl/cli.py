from __future__ import annotations

import argparse
import os
import sys

from sc_spotify_etl.db import connect, init_schema
from sc_spotify_etl.etl import ingest_export_file


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Spotify export-file ETL into DuckDB (fast, idempotent, dedupes, captures corrupt lines)."
    )
    p.add_argument("--input", required=True, help="Path to newline-delimited JSON export file (JSONL).")
    p.add_argument("--db", default="listens.duckdb", help="DuckDB file path (default: listens.duckdb).")
    p.add_argument(
        "--source-name",
        default=None,
        help="Logical source name stored in DB (default: basename of input).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not os.path.exists(args.input):
        print(f"ERROR: input file not found: {args.input}", file=sys.stderr)
        return 2

    db_dir = os.path.dirname(args.db)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    con = connect(args.db)
    init_schema(con)

    staged, inserted, corrupted = ingest_export_file(con, args.input, args.source_name)

    print("ETL complete.")
    print(f"DB: {args.db}")
    print(f"Input: {args.input}")
    print(f"Rows staged (valid parsed rows): {staged}")
    print(f"New listens rows inserted:   {inserted}")
    print(f"Corrupted lines recorded:        {corrupted}")
    print("Idempotency: fact dedup by listen_id; dim upsert by track_key; raw keyed by (source_file,line_number).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())