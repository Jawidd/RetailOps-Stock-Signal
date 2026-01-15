import os
import io
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta, timezone

from generator import RetailDataGenerator

s3 = boto3.client("s3")

BUCKET = os.environ.get("S3_BUCKET")
if not BUCKET:
    raise RuntimeError("S3_BUCKET env var must be set to the RetailOps data lake bucket name")


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
    date_str = event.get("date") or _yesterday_utc()

    products_df = _read_csv_s3(PRODUCTS_KEY)
    stores_df = _read_csv_s3(STORES_KEY)
    suppliers_df = _read_csv_s3(SUPPLIERS_KEY)

    # If your generator expects sub_category but csv has subcategory, fix here (optional)
    if "subcategory" in products_df.columns and "sub_category" not in products_df.columns:
        products_df = products_df.rename(columns={"subcategory": "sub_category"})

    gen = RetailDataGenerator(start_date=date_str, days=0)

    sales_df = gen.generate_sales(products_df, stores_df)
    inventory_df = gen.generate_inventory(products_df, stores_df, sales_df)
    shipments_df = gen.generate_shipments(products_df, stores_df, suppliers_df, inventory_df)

    # Ensure dates are yyyy-mm-dd strings
    if "sale_date" in sales_df.columns:
        sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"]).dt.strftime("%Y-%m-%d")
    if "snapshot_date" in inventory_df.columns:
        inventory_df["snapshot_date"] = pd.to_datetime(inventory_df["snapshot_date"]).dt.strftime("%Y-%m-%d")

    for c in ["order_date", "expected_date", "received_date"]:
        if c in shipments_df.columns:
            shipments_df[c] = pd.to_datetime(shipments_df[c]).dt.strftime("%Y-%m-%d")

    _upload_df_csv(sales_df, f"raw/sales/dt={date_str}/sales.csv")
    _upload_df_csv(inventory_df, f"raw/inventory/dt={date_str}/inventory.csv")
    _upload_df_csv(shipments_df, f"raw/shipments/dt={date_str}/shipments.csv")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "date": date_str,
            "sales_rows": int(len(sales_df)),
            "inventory_rows": int(len(inventory_df)),
            "shipments_rows": int(len(shipments_df))
        })
    }
