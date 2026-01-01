import duckdb
import os
from pathlib import Path

# Recommended setup for relative pathing
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent 
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "favorita"

os.makedirs(WAREHOUSE_DIR, exist_ok=True)

db= duckdb.connect(database=str(WAREHOUSE_DIR / "favorita.duckdb"))
db.execute("CREATE SCHEMA IF NOT EXISTS raw")

# Load the tables
tables = ["holidays_events", "items", "oil", "stores", "test", "train", "transactions"]
for table in tables:
    db.execute(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM read_parquet('{BRONZE_DIR}/{table}.parquet')")
    print(f" Loaded raw.{table} table ")

#echo loaded raw tables from db
print("\nTables loaded:")
for row in db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='raw'").fetchall():
    count = db.execute(f"SELECT COUNT(*) FROM raw.{row[0]}").fetchone()[0]
    print(f"  raw.{row[0]:20s} {count:,} rows")


db.close()
print("\n✓ Warehouse ready at warehouse/retailops.duckdb")
