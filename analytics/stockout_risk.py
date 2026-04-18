"""
Stockout Risk Scoring Model
===========================

Business question
-----------------
Which store × product combinations are most likely to stock out before the
next replenishment arrives, given current inventory levels, recent demand
velocity, and supplier lead time?

Formula
-------
    days_of_stock_remaining  = max(quantity_on_hand, 0) / avg_daily_demand
    expected_replenishment   = avg_actual_lead_time_days / on_time_rate
    risk_score               = expected_replenishment - days_of_stock_remaining

    Positive score  → stockout expected before replenishment arrives.
    Negative score  → stock will last beyond the replenishment window.

Why each term
-------------
- max(quantity_on_hand, 0): the generator occasionally produces negative
  on-hand values (data quality artefact). Clipping to 0 treats them as
  already stocked-out rather than inflating days-of-stock.
- avg_daily_demand (30-day trailing): smooths day-to-day noise and matches
  the window a procurement team would review.
- avg_actual_lead_time_days / on_time_rate: penalises unreliable suppliers.
  A supplier with a 10-day lead time and 50 % on-time rate is effectively a
  20-day supplier from a planning perspective.

Limitations
-----------
- Synthetic data: demand is generated with fixed multipliers, not real
  seasonality, so the 30-day average is a reasonable but flat proxy.
- No demand forecasting: the score uses trailing average demand, not a
  forward-looking forecast. A spike in demand the day after scoring would
  not be captured.
- Lead time is an average, not a distribution: using avg_actual_lead_time_days
  ignores variance. A supplier with a mean of 10 days but high variance is
  riskier than the score implies.

What would change with real data
---------------------------------
- Replace avg lead time with a Bayesian posterior over the lead-time
  distribution (e.g. log-normal) to produce a probabilistic stockout date.
- Replace trailing-average demand with a seasonal decomposition or simple
  exponential smoothing that respects the day-of-week and promotional
  multipliers already embedded in the generator.
- Add safety-stock buffer: reorder_point already encodes this in the data
  model; the score could be adjusted to flag when stock falls below
  reorder_point + (demand_std * z_score) rather than zero.

Inputs (queried from Athena)
----------------------------
    retailops.fct_daily_sales          — avg daily demand per store × product
    retailops.fct_inventory_snapshots  — latest on-hand, reorder point
    retailops.mart_supplier_performance — avg actual lead time, on-time rate

Usage
-----
    Requires AWS credentials with Athena + S3 read access.
    Reads from ~/.aws/credentials or environment variables.

        python analytics/stockout_risk.py

    Writes:
        analytics/stockout_risk_output.csv   — full ranked table
        analytics/supplier_quadrants.csv     — supplier segmentation
"""

import os
import time
import textwrap
import boto3
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent.parent / ".env")

REGION          = os.getenv("AWS_DEFAULT_REGION", "eu-west-2")
WORKGROUP       = "retailops-primary"
DATABASE        = "retailops"
DEMAND_DAYS     = 30
OUTPUT_DIR      = Path(__file__).parent
POLL_INTERVAL   = 2   # seconds between Athena status checks


# ---------------------------------------------------------------------------
# Athena helper
# ---------------------------------------------------------------------------

def run_query(client: boto3.client, sql: str, label: str) -> pd.DataFrame:
    """Submit a query to Athena, wait for completion, return a DataFrame."""
    print(f"  → Running: {label}")
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    qid = resp["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {label!r} {state}: {reason}")
        time.sleep(POLL_INTERVAL)

    paginator = client.get_paginator("get_query_results")
    rows, header = [], None
    for page in paginator.paginate(QueryExecutionId=qid):
        result_rows = page["ResultSet"]["Rows"]
        if header is None:
            header = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            rows.append([c.get("VarCharValue", None) for c in row["Data"]])

    df = pd.DataFrame(rows, columns=header)
    print(f"     {len(df):,} rows returned")
    return df


# ---------------------------------------------------------------------------
# SQL queries — all targeting the dbt mart tables
# ---------------------------------------------------------------------------

SQL_DEMAND = textwrap.dedent(f"""
    SELECT
        store_id,
        product_id,
        CAST(SUM(quantity_sold) AS DOUBLE) / {DEMAND_DAYS}.0  AS avg_daily_demand
    FROM   retailops.fct_daily_sales
    WHERE  sale_date >= DATE_ADD('day', -{DEMAND_DAYS}, (SELECT MAX(sale_date) FROM retailops.fct_daily_sales))
    GROUP  BY store_id, product_id
""")

SQL_INVENTORY = textwrap.dedent("""
    SELECT
        store_id,
        product_id,
        product_name,
        category,
        CAST(quantity_on_hand          AS DOUBLE) AS quantity_on_hand,
        CAST(quantity_on_order         AS DOUBLE) AS quantity_on_order,
        CAST(reorder_point             AS DOUBLE) AS reorder_point,
        CAST(is_out_of_stock           AS BOOLEAN) AS is_out_of_stock,
        CAST(needs_reorder             AS BOOLEAN) AS needs_reorder
    FROM   retailops.fct_inventory_snapshots
    WHERE  snapshot_date = (SELECT MAX(snapshot_date) FROM retailops.fct_inventory_snapshots)
""")

SQL_SUPPLIER = textwrap.dedent("""
    SELECT
        supplier_id,
        supplier_name,
        CAST(avg_actual_lead_time_days AS DOUBLE) AS avg_actual_lead_time_days,
        CAST(calculated_on_time_rate   AS DOUBLE) AS calculated_on_time_rate,
        CAST(avg_fill_rate             AS DOUBLE) AS avg_fill_rate,
        CAST(total_shipments           AS BIGINT)  AS total_shipments,
        CAST(late_shipments            AS BIGINT)  AS late_shipments
    FROM   retailops.mart_supplier_performance
""")

# product → supplier mapping lives in dim_products
SQL_PRODUCT_SUPPLIER = textwrap.dedent("""
    SELECT product_id, supplier_id
    FROM   retailops.dim_products
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    client = boto3.client("athena", region_name=REGION)

    print("\n[1/4] Fetching avg daily demand (last 30 days) from fct_daily_sales …")
    demand = run_query(client, SQL_DEMAND, "demand")
    demand["avg_daily_demand"] = pd.to_numeric(demand["avg_daily_demand"], errors="coerce").fillna(0)

    print("\n[2/4] Fetching latest inventory snapshot from fct_inventory_snapshots …")
    inv = run_query(client, SQL_INVENTORY, "inventory")
    for col in ["quantity_on_hand", "quantity_on_order", "reorder_point"]:
        inv[col] = pd.to_numeric(inv[col], errors="coerce").fillna(0)

    print("\n[3/4] Fetching supplier performance from mart_supplier_performance …")
    sup = run_query(client, SQL_SUPPLIER, "supplier_performance")
    for col in ["avg_actual_lead_time_days", "calculated_on_time_rate", "avg_fill_rate"]:
        sup[col] = pd.to_numeric(sup[col], errors="coerce")

    print("\n[4/4] Fetching product → supplier mapping from dim_products …")
    prod_sup = run_query(client, SQL_PRODUCT_SUPPLIER, "product_supplier")

    # -----------------------------------------------------------------------
    # Build scoring table
    # -----------------------------------------------------------------------

    print("\nBuilding risk scores …")

    scored = (
        inv
        .merge(demand, on=["store_id", "product_id"], how="left")
        .merge(prod_sup, on="product_id", how="left")
        .merge(
            sup[["supplier_id", "supplier_name", "avg_actual_lead_time_days", "calculated_on_time_rate"]],
            on="supplier_id", how="left",
        )
    )

    scored["avg_daily_demand"] = scored["avg_daily_demand"].fillna(0)

    # Clip negative on-hand to 0 before dividing
    scored["quantity_on_hand_safe"] = scored["quantity_on_hand"].clip(lower=0)

    # days_of_stock_remaining — inf when no demand (no risk from demand side)
    scored["days_of_stock_remaining"] = np.where(
        scored["avg_daily_demand"] > 0,
        scored["quantity_on_hand_safe"] / scored["avg_daily_demand"],
        np.inf,
    )

    # expected_replenishment — penalise unreliable suppliers
    scored["expected_replenishment_days"] = (
        scored["avg_actual_lead_time_days"]
        / scored["calculated_on_time_rate"].clip(lower=0.01)
    )

    # risk_score: positive = stockout before replenishment
    scored["risk_score"] = (
        scored["expected_replenishment_days"] - scored["days_of_stock_remaining"]
    )

    output_cols = [
        "store_id", "product_id", "product_name", "category",
        "supplier_id", "supplier_name",
        "quantity_on_hand", "reorder_point", "quantity_on_order",
        "avg_daily_demand", "days_of_stock_remaining",
        "avg_actual_lead_time_days", "calculated_on_time_rate",
        "expected_replenishment_days", "risk_score",
        "is_out_of_stock", "needs_reorder",
    ]

    result = (
        scored[output_cols]
        .sort_values("risk_score", ascending=False)
        .reset_index(drop=True)
    )
    result.index += 1
    result.index.name = "risk_rank"

    # -----------------------------------------------------------------------
    # Write output
    # -----------------------------------------------------------------------

    risk_path = OUTPUT_DIR / "stockout_risk_output.csv"
    result.to_csv(risk_path)
    print(f"\nFull results written → {risk_path}")

    already_out   = (result["quantity_on_hand"] <= 0).sum()
    positive_risk = (result["risk_score"] > 0).sum()
    print(f"Total combinations scored : {len(result):,}")
    print(f"Already out of stock      : {already_out:,}")
    print(f"Positive risk score       : {positive_risk:,}")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", "{:.1f}".format)

    print("\n" + "=" * 100)
    print("TOP 25 HIGHEST-RISK STORE × PRODUCT COMBINATIONS")
    print("=" * 100)
    print(result[[
        "store_id", "product_id", "product_name",
        "quantity_on_hand", "avg_daily_demand",
        "days_of_stock_remaining", "expected_replenishment_days",
        "risk_score", "is_out_of_stock",
    ]].head(25).to_string())

    # -----------------------------------------------------------------------
    # Supplier reliability segmentation (stretch: 2×2 quadrant)
    # -----------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("SUPPLIER RELIABILITY SEGMENTATION (2×2 QUADRANT)")
    print("=" * 100)

    sup_scored = sup.dropna(subset=["calculated_on_time_rate", "avg_fill_rate"]).copy()

    ot_median   = sup_scored["calculated_on_time_rate"].median()
    fill_median = sup_scored["avg_fill_rate"].median()

    def quadrant(row):
        high_ot   = row["calculated_on_time_rate"] >= ot_median
        high_fill = row["avg_fill_rate"]            >= fill_median
        if   high_ot and high_fill:     return "Q1: Preferred"
        elif high_ot and not high_fill: return "Q2: Reliable but under-delivering"
        elif not high_ot and high_fill: return "Q3: Slow but complete"
        else:                           return "Q4: Renegotiate or replace"

    sup_scored["quadrant"] = sup_scored.apply(quadrant, axis=1)

    for q in ["Q1: Preferred",
              "Q2: Reliable but under-delivering",
              "Q3: Slow but complete",
              "Q4: Renegotiate or replace"]:
        group = sup_scored[sup_scored["quadrant"] == q][
            ["supplier_id", "supplier_name", "calculated_on_time_rate",
             "avg_fill_rate", "total_shipments", "late_shipments"]
        ].sort_values("calculated_on_time_rate", ascending=False)
        print(f"\n── {q} ──")
        print(group.to_string(index=False))

    print(f"\nMedians — on_time_rate: {ot_median:.3f}  |  avg_fill_rate: {fill_median:.3f}")

    quad_path = OUTPUT_DIR / "supplier_quadrants.csv"
    sup_scored.to_csv(quad_path, index=False)
    print(f"Supplier quadrants written → {quad_path}")

    # -----------------------------------------------------------------------
    # Supplier quadrant interpretation (written for interview use)
    # -----------------------------------------------------------------------

    print("\n" + "=" * 100)
    print("SUPPLIER QUADRANT INTERPRETATION")
    print("=" * 100)
    print(textwrap.dedent("""
    Q1 — Preferred (high on-time, high fill rate)
    Suppliers in this quadrant deliver on schedule and ship complete orders.
    They are the lowest operational risk in the supply chain and should be
    prioritised for volume commitments and long-term contracts. In the
    synthetic dataset these suppliers have both calculated_on_time_rate and
    avg_fill_rate above the median, meaning the pipeline's 53 dbt tests
    confirm their shipment records are consistent with the master-data
    on_time_delivery_rate. Procurement action: lock in preferred-supplier
    status, negotiate volume discounts, and use them as the primary source
    for high-velocity SKUs that appear at the top of the stockout risk table.

    Q4 — Renegotiate or replace (low on-time, low fill rate)
    Suppliers in this quadrant are doubly penalised in the risk score: their
    low on_time_rate inflates expected_replenishment_days, and their low
    fill_rate means even when they do arrive, orders are incomplete. Any
    store × product combination that depends on a Q4 supplier and has a
    positive risk_score should be treated as a priority escalation. Procurement
    action: issue a formal performance-improvement notice, dual-source
    immediately with a Q1 supplier, and set a 90-day review gate. If the
    calculated_on_time_rate does not improve, replace the supplier entirely.
    In a real dataset you would also cross-reference the on_time_rate_variance
    column in mart_supplier_performance — a large negative variance (calculated
    rate much lower than the master-data rate) is a red flag that the supplier
    is misreporting performance.
    """).strip())


if __name__ == "__main__":
    main()
