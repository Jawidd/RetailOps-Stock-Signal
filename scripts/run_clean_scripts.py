import duckdb
import os

warehouse_db= '../warehouse/favorita.duckdb'
sql_dir= '../warehouse/sql'
clean_schema_name='clean'
fact_schema_name='fact'


db= duckdb.connect(warehouse_db)
db.execute(f"create schema if not exists {clean_schema_name}")
db.execute(f"create schema if not exists {fact_schema_name}")

# tables to be created in clean schema
tables = [("clean_holidays",clean_schema_name),
          ("clean_items", clean_schema_name),
          ("clean_oil", clean_schema_name),
          ("clean_stores", clean_schema_name),
          ("clean_test", clean_schema_name),
          ("clean_train", clean_schema_name),
          ("clean_transactions", clean_schema_name),
          ("fct_store_daily_sales", fact_schema_name)]


# build each table
for table_name,schema in tables:
    filepath = f"{sql_dir}/{table_name}.sql"
    if not os.path.exists(filepath):
        print(f"⚠ {filepath} not found, skipping")
        continue
    
    with open(filepath, 'r') as file:
        sql_script = file.read()
    
    try:
        db.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS {sql_script}")
        count = db.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
        print(f"✓ {schema}.{table_name} - {count:,} rows")
    except Exception as e:
        print(f"✗ {schema}.{table_name} - Error: {e}")


db.close()
print("\n✓ Done")