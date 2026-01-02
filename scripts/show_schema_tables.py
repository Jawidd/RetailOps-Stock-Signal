import duckdb

from pathlib import Path
# Recommended setup for relative pathing
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent 

WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"

con=duckdb.connect(str(WAREHOUSE_DIR/"favorita.duckdb"))


tables_df= con.execute("""
select table_schema, table_name
from information_schema.tables
where table_type = 'BASE_TABLE'
""").fetchdf()

results = []

for index,row in tables_df.iterrows():
    schema= row["table_schema"]
    table = row["table_name"]
    
    query = f'select count(*) from "{schema}"."{table}"'
    row_count = con.execute(query).fetchone()[0]

    results.append({"schema": schema, "table":table, "rows":row_count})

# ----------------------------
# Display results
# ----------------------------
import pandas as pd

pd.set_option("display.max_columns", None)


print("-" * 50)
for r in results:
    print(f"{r['schema']} | {r['table']} | {r['rows']}")


print("\n=== 20 ROWS OF fct_store_daily_sales ===")
print("-" * 70)

display_df = con.execute("""
    SELECT *
    FROM fact.fct_store_daily_sales
    ORDER BY  sales_date
    LIMIT 2
""").fetchdf()

print(display_df)
# print("\n=== 20 ROWS OF CLEAN TRAIN ===")
# print("-" * 70)

# clean_train_df = con.execute("""
#     SELECT *
#     FROM clean.clean_train
#     where is_wage_day is True
#     and is_earthquake_period is True
#     ORDER BY  date
#     LIMIT 20
# """).fetchdf()

# print(clean_train_df)

con.close()