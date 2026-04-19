"""
Shared Athena query helper used by all ml/ scripts.
Reads config from environment / .env — no hardcoded values.
"""

import os
import time
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

REGION     = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")
WORKGROUP  = "retailops-primary"
DATABASE   = "retailops"
BUCKET     = f"retailops-data-lake-{REGION}"
POLL_SEC   = 2


def get_athena_client() -> boto3.client:
    return boto3.client("athena", region_name=REGION)


def get_s3_client() -> boto3.client:
    return boto3.client("s3", region_name=REGION)


def run_query(sql: str, label: str = "", client: boto3.client = None) -> pd.DataFrame:
    """Execute SQL on Athena, block until done, return DataFrame."""
    if client is None:
        client = get_athena_client()
    if label:
        print(f"  [athena] {label}")

    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    qid = resp["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena {label!r} {state}: {reason}")
        time.sleep(POLL_SEC)

    paginator = client.get_paginator("get_query_results")
    rows, header = [], None
    for page in paginator.paginate(QueryExecutionId=qid):
        result_rows = page["ResultSet"]["Rows"]
        if header is None:
            header = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([c.get("VarCharValue") for c in row["Data"]])

    df = pd.DataFrame(rows, columns=header)
    if label:
        print(f"         {len(df):,} rows")
    return df
