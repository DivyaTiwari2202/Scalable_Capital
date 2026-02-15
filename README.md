## How to Run

### 1. Install dependencies

Make sure you have Python 3.10+ installed. Then, install the required dependencies:

```sh
pip install .
```

Or, for development:

```sh
python3 -m venv .venv                                                 
source .venv/bin/activate 
pip install -e .
```

### 2. Prepare your input

Ensure you have a newline-delimited JSON file (JSONL) with Spotify listen data.

### 3. Run the ETL

You can run the ETL process using the CLI script defined in `pyproject.toml`:

```sh
sc-spotify-etl  --input /path/to/your/input.jsonl --db /path/to/listens.duckdb
```

- `--input`: Path to your JSONL dataset file.
- `--db`: Path to the DuckDB database file (will be created if it doesn't exist).

Example:

```sh
sc-spotify-etl  --input /Users/Divya/Downloads/dataset_test.txt --db duckdb/listens.duckdb
```

This will ingest the data and print a summary of the operation.

### 4. Notes

- Corrupted JSON lines are skipped automatically.
- The database schema is initialized automatically if not present.
