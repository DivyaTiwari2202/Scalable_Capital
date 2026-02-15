
python3 -m venv .venv                                                 
source .venv/bin/activate

pip install -e .                                                      


sc-spotify-etl --input "/Users/Divya/Downloads/dataset_test.txt" --db "duckdb/listens.duckdb"




