#!/usr/bin/env bash
# Convert Favorita CSV files to Parquet for faster downstream queries.

set -euo pipefail


RAW_DIR="../data/raw/favorita"
BRONZE_DIR="../data/bronze/favorita"

# Check if raw directory exists
if [ ! -d "${RAW_DIR}" ]; then
  echo "Raw directory not found: $RAW_DIR"
  exit 1
fi

mkdir -p "$BRONZE_DIR"





python - <<'PY'
import duckdb, os, glob

raw_dir = os.environ.get("RAW_DIR", "../data/raw/favorita")
bronze_dir = os.environ.get("BRONZE_DIR", "../data/bronze/favorita")

con = duckdb.connect()

for csv in glob.glob(f"{raw_dir}/*.csv"):
    name = os.path.basename(csv).replace(".csv", ".parquet")
    out = os.path.join(bronze_dir, name)

    if os.path.exists(out):
        print(f"skip  {name}")
        continue

    print(f"write {name}")
    con.execute(
        f"""
        COPY (
          SELECT * FROM read_csv_auto('{csv}', sample_size=200000)
        )
        TO '{out}' (FORMAT PARQUET);
        """
    )

print("done")
PY