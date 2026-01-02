import duckdb
import os

warehouse_db= '../warehouse/favorita.duckdb'
sql_dir= '../warehouse/cleaning'

db= duckdb.connect(warehouse_db)

clean_schema_name='clean'

db.execute(f"create schema if not exists {clean_schema_name}")

# tables to be created in clean schema
# tables = ["holidays_events", "items", "oil", "stores", "test", "train", "transactions"]
tables = ["clean_holidays", "clean_items", "clean_oil", "clean_stores", "clean_test", "clean_train", "clean_transactions"]

# build each table
for table_name in tables:
    filepath = f"{sql_dir}/{table_name}.sql"
    if not os.path.exists(filepath):
        print(f"⚠ {filepath} not found, skipping")
        continue
    
    with open(filepath, 'r') as file:
        sql_script = file.read()
    
    try:
        db.execute(f"CREATE OR REPLACE TABLE {clean_schema_name}.{table_name} AS {sql_script}")
        count = db.execute(f"SELECT COUNT(*) FROM {clean_schema_name}.{table_name}").fetchone()[0]
        print(f"✓ {clean_schema_name}.{table_name} - {count:,} rows")
    except Exception as e:
        print(f"✗ {clean_schema_name}.{table_name} - Error: {e}")

db.close()
print("\n✓ Done")