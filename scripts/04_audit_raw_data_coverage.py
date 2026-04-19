#!/usr/bin/env python3
"""Audit raw RetailOps date coverage in S3 fact partitions.

Scans partition prefixes:
  raw/sales/dt=YYYY-MM-DD/
  raw/inventory/dt=YYYY-MM-DD/
  raw/shipments/dt=YYYY-MM-DD/

Outputs:
- first/last date seen per table
- date range audited
- dates with complete data (all tables present)
- dates with missing data (one or more tables missing)
- optional JSON report for downstream automation
"""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Set

import boto3
from botocore.exceptions import BotoCoreError, ClientError

DT_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/?$")
DEFAULT_TABLES = ["sales", "inventory", "shipments"]


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    except Exception:
        pass


def _cloudformation_export_value(cf_client, export_name: str) -> str | None:
    try:
        paginator = cf_client.get_paginator("list_exports")
        for page in paginator.paginate():
            for item in page.get("Exports", []):
                if item.get("Name") == export_name:
                    return item.get("Value")
    except (ClientError, BotoCoreError):
        return None
    return None


def resolve_bucket_name(region: str, explicit_bucket: str | None) -> str:
    if explicit_bucket:
        return explicit_bucket

    env_bucket = os.getenv("RAW_DATA_BUCKET") or os.getenv("S3_BUCKET")
    if env_bucket:
        return env_bucket

    cf = boto3.client("cloudformation", region_name=region)
    export_bucket = _cloudformation_export_value(cf, "retailops-DataLakeBucketName")
    if export_bucket:
        return export_bucket

    raise RuntimeError(
        "Could not resolve S3 bucket. Pass --bucket or set RAW_DATA_BUCKET/S3_BUCKET."
    )


def list_partition_dates(s3_client, bucket: str, table: str) -> Set[date]:
    prefix = f"raw/{table}/"
    paginator = s3_client.get_paginator("list_objects_v2")

    dates: Set[date] = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part = cp.get("Prefix", "").rstrip("/").split("/")[-1]
            m = DT_RE.match(part)
            if m:
                dates.add(_parse_date(m.group(1)))
    return dates


def build_report(
    bucket: str,
    region: str,
    tables: List[str],
    start: date | None,
    end: date | None,
) -> Dict:
    s3 = boto3.client("s3", region_name=region)

    dates_by_table: Dict[str, Set[date]] = {
        table: list_partition_dates(s3, bucket, table) for table in tables
    }

    union_dates: Set[date] = set()
    for dset in dates_by_table.values():
        union_dates.update(dset)

    if not union_dates:
        raise RuntimeError(
            f"No partitions found under raw/{'|'.join(tables)}/ in s3://{bucket}"
        )

    start_date = start or min(union_dates)
    end_date = end or max(union_dates)
    if start_date > end_date:
        raise ValueError("start date must be <= end date")

    missing_by_table: Dict[str, List[str]] = {}
    complete_days: List[str] = []
    missing_days: List[str] = []
    missing_detail: Dict[str, List[str]] = {}

    for day in _date_range(start_date, end_date):
        day_str = day.isoformat()
        absent_tables = [t for t in tables if day not in dates_by_table[t]]

        if absent_tables:
            missing_days.append(day_str)
            missing_detail[day_str] = absent_tables
        else:
            complete_days.append(day_str)

    all_days = set(_date_range(start_date, end_date))
    for table in tables:
        missing = sorted(all_days - dates_by_table[table])
        missing_by_table[table] = [d.isoformat() for d in missing]

    table_stats = {}
    for table, dset in dates_by_table.items():
        if dset:
            table_stats[table] = {
                "days_present": len(dset),
                "first_date": min(dset).isoformat(),
                "last_date": max(dset).isoformat(),
            }
        else:
            table_stats[table] = {
                "days_present": 0,
                "first_date": None,
                "last_date": None,
            }

    return {
        "bucket": bucket,
        "region": region,
        "tables": tables,
        "audit_start_date": start_date.isoformat(),
        "audit_end_date": end_date.isoformat(),
        "total_days_in_range": (end_date - start_date).days + 1,
        "table_stats": table_stats,
        "complete_days": complete_days,
        "missing_days_any_table": missing_days,
        "missing_detail_by_day": missing_detail,
        "missing_days_by_table": missing_by_table,
    }


def main() -> None:
    _load_dotenv_if_available()

    parser = argparse.ArgumentParser(description="Audit raw S3 date coverage.")
    parser.add_argument("--bucket", help="S3 data lake bucket name")
    parser.add_argument(
        "--region", default=os.getenv("AWS_DEFAULT_REGION", "eu-west-2"), help="AWS region"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        help="Fact tables to audit (default: sales inventory shipments)",
    )
    parser.add_argument("--start-date", type=_parse_date, help="Audit start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=_parse_date, help="Audit end date (YYYY-MM-DD)")
    parser.add_argument(
        "--out-json",
        default="scripts/raw_data_coverage_report.json",
        help="Output JSON report path",
    )
    parser.add_argument(
        "--print-limit",
        type=int,
        default=40,
        help="How many missing days to print inline (default: 40)",
    )
    args = parser.parse_args()

    bucket = resolve_bucket_name(args.region, args.bucket)
    report = build_report(
        bucket=bucket,
        region=args.region,
        tables=args.tables,
        start=args.start_date,
        end=args.end_date,
    )

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Bucket: s3://{report['bucket']}")
    print(f"Audit range: {report['audit_start_date']} -> {report['audit_end_date']}")
    print(f"Total days in range: {report['total_days_in_range']}")
    print()

    for table, stats in report["table_stats"].items():
        print(
            f"[{table}] present={stats['days_present']} first={stats['first_date']} last={stats['last_date']}"
        )

    print()
    print(f"Complete days (all tables present): {len(report['complete_days'])}")
    print(f"Missing days (any table missing): {len(report['missing_days_any_table'])}")

    missing_days = report["missing_days_any_table"]
    if missing_days:
        preview = missing_days[: max(0, args.print_limit)]
        print("Missing days preview:", ", ".join(preview))
        if len(missing_days) > len(preview):
            print(f"... and {len(missing_days) - len(preview)} more")

    print(f"\nJSON report written: {out_path}")


if __name__ == "__main__":
    main()
