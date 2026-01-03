#!/usr/bin/env python3
import os
import psycopg2
from psycopg2 import sql

# --- Configuration ---
CSV_DIR = os.getenv("CSV_DIR", "../data/raw/favorita")
SCHEMA = os.getenv("PG_SCHEMA", "raw")
TABLES = ["stores", "items", "transactions", "oil", "holidays_events", "test", "train"]

def connect_pg():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "retailops"),
        user=os.getenv("PG_USER", "retailops"),
        password=os.getenv("PG_PASS", "retailops123"),
    )

def ensure_schema(cur):
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))

def create_table(cur, table, columns):
    col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in columns]
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(table)))
    cur.execute(
        sql.SQL("CREATE TABLE {}.{} ({});").format(
            sql.Identifier(SCHEMA),
            sql.Identifier(table),
            sql.SQL(", ").join(col_defs)
        )
    )

def get_columns(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        header = f.readline().strip()
    return [h.strip() for h in header.split(",")]

def copy_csv(cur, table, csv_path):
    cmd = sql.SQL(
        "COPY {}.{} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '', QUOTE '\"', ESCAPE '\"')"
    ).format(sql.Identifier(SCHEMA), sql.Identifier(table))
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(cmd.as_string(cur), f)

def main():
    conn = connect_pg()
    conn.autocommit = False
    cur = conn.cursor()
    ensure_schema(cur)
    conn.commit()

    for table in TABLES:
        csv_path = os.path.join(CSV_DIR, f"{table}.csv")
        if not os.path.exists(csv_path):
            print(f"⚠ {csv_path} not found, skipping.")
            continue

        print(f"Loading {table}...")
        columns = get_columns(csv_path)
        create_table(cur, table, columns)
        copy_csv(cur, table, csv_path)
        conn.commit()

        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(sql.Identifier(SCHEMA), sql.Identifier(table)))
        count = cur.fetchone()[0]
        print(f"✓ {table}: {count} rows loaded")

    cur.close()
    conn.close()
    print("All CSVs loaded successfully.")

if __name__ == "__main__":
    main()
