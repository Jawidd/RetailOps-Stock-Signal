import os
import io
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta, timezone

# IMPORTANT: import your generator module (same folder)
from generate_synthetic_data import RetailDataGenerator

s3 = boto3.client("s3")

BUCKET = os.environ["S3_BUCKET"]

# dimension keys (match your existing S3 layout)
PRODUCTS_KEY = os.environ.get("PRODUCTS_KEY", "raw/products/products.csv")
STORES_KEY = os.environ.get("STORES_KEY", "raw/stores/stores.csv")
SUPPLIERS_KEY = os.environ.get("SUPPLIERS_KEY", "raw/suppliers/suppliers.csv")


def _yesterday_utc() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def _read_csv_s3(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"].read().decode("utf-8", errors="replace")
    return pd.read_csv(io.StringIO(body))


def _upload_df_csv(df: pd.DataFrame, key: str) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def lambda_handler(event, context):
    """
    Event input example:
      { "date": "2025-01-13" }

    If date not provided, defaults to yesterday (UTC).
    """
    date_str = event.get("date") or _yesterday_utc()

    # 1) Load dimensions (already in S3)
    products_df = _read_csv_s3(PRODUCTS_KEY)
    stores_df = _read_csv_s3(STORES_KEY)
    suppliers_df = _read_csv_s3(SUPPLIERS_KEY)

    # 2) Make sure column names match your Glue tables (if needed)
    # Your Glue table uses "sub_category" but generator produces "subcategory"
    if "subcategory" in products_df.columns and "sub_category" not in products_df.columns:
        products_df = products_df.rename(columns={"subcategory": "sub_category"})

    # 3) Generate ONE day of facts using your generator
    # NOTE: your generator's date_range includes BOTH start_date and end_date.
    # Setting days=0 makes exactly one day.
    gen = RetailDataGenerator(start_date=date_str, days=0)

    sales_df = gen.generate_sales(products_df, stores_df)
    inventory_df = gen.generate_inventory(products_df, stores_df, sales_df)
    shipments_df = gen.generate_shipments(products_df, stores_df, suppliers_df, inventory_df)

    # 4) Ensure date columns are strings (your Glue schema is string)
    sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"]).dt.strftime("%Y-%m-%d")
    inventory_df["snapshot_date"] = pd.to_datetime(inventory_df["snapshot_date"]).dt.strftime("%Y-%m-%d")
    shipments_df["order_date"] = pd.to_datetime(shipments_df["order_date"]).dt.strftime("%Y-%m-%d")
    shipments_df["expected_date"] = pd.to_datetime(shipments_df["expected_date"]).dt.strftime("%Y-%m-%d")
    shipments_df["received_date"] = pd.to_datetime(shipments_df["received_date"]).dt.strftime("%Y-%m-%d")

    # 5) Upload to partitioned S3 paths expected by Athena/Glue
    _upload_df_csv(sales_df, f"raw/sales/dt={date_str}/sales.csv")
    _upload_df_csv(inventory_df, f"raw/inventory/dt={date_str}/inventory.csv")
    _upload_df_csv(shipments_df, f"raw/shipments/dt={date_str}/shipments.csv")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "date": date_str,
            "sales_rows": int(len(sales_df)),
            "inventory_rows": int(len(inventory_df)),
            "shipments_rows": int(len(shipments_df)),
            "bucket": BUCKET
        })
    }
