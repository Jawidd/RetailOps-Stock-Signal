"""
ml/reorder_recommendations.py
==============================
Reorder recommendation layer.

Combines ML demand forecasts with supplier lead times and a statistical
safety stock formula to produce actionable reorder recommendations.

This is a direct upgrade over analytics/stockout_risk.py:
  - Old: trailing average demand (backward-looking)
    New: ML forecast (forward-looking)
  - Old: ignores demand variance
    New: safety stock incorporates demand std dev
  - Old: outputs a risk score with no action
    New: outputs a specific recommended order quantity

Safety stock formula
--------------------
    safety_stock = z * σ_d * √L

Where:
    z   = service level z-score (1.65 for 95 % service level)
    σ_d = standard deviation of daily demand over trailing 14 days
    L   = expected lead time in days (avg_actual_lead_time_days / on_time_rate)

This is the standard safety stock formula from inventory theory (Silver, Pyke,
Peterson). The √L term accounts for the fact that demand uncertainty compounds
over the lead time — if daily demand std is σ, then over L independent days
the std of total demand is σ√L.

Reorder point
-------------
    reorder_point = forecast_demand_over_lead_time + safety_stock

Where forecast_demand_over_lead_time = sum of daily forecasts for days 1…L.

Recommended order quantity
--------------------------
    recommended_order_qty = max(0, reorder_point - quantity_on_hand - quantity_on_order)

Risk tiers
----------
    Critical : quantity_on_hand < safety_stock
               (already inside the safety buffer — order immediately)
    High     : quantity_on_hand < reorder_point
               (below reorder point — order today)
    Medium   : days_of_stock_remaining < expected_lead_time * 1.5
               (approaching reorder point — monitor closely)
    Low      : everything else

Run:
    python ml/reorder_recommendations.py
"""

import io
import json
import pickle
import numpy as np
import pandas as pd
from datetime import date, datetime
from pathlib import Path

from ml.athena_client import run_query, get_s3_client, BUCKET
from ml.train import (
    FEATURE_COLS, CATEGORICAL_COLS, MODEL_VERSION, MODEL_S3_PREFIX,
    FEATURES_S3_KEY, make_lgb_dataset, N_HORIZONS,
)

SERVICE_LEVEL_Z = 1.65   # 95 % service level
RECS_S3_PREFIX  = "ml/reorder_recommendations"

SQL_LATEST_INVENTORY = """
    SELECT
        store_id,
        product_id,
        CAST(quantity_on_hand_clipped AS DOUBLE) AS quantity_on_hand,
        CAST(quantity_on_order        AS DOUBLE) AS quantity_on_order,
        CAST(reorder_point            AS DOUBLE) AS reorder_point
    FROM retailops.fct_inventory_snapshots
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM retailops.fct_inventory_snapshots)
"""

SQL_SUPPLIER_LEAD = """
    SELECT
        p.product_id,
        p.supplier_id,
        CAST(sp.avg_actual_lead_time_days AS DOUBLE) AS avg_actual_lead_time_days,
        CAST(sp.calculated_on_time_rate   AS DOUBLE) AS calculated_on_time_rate
    FROM retailops.dim_products p
    LEFT JOIN retailops.mart_supplier_performance sp
      ON p.supplier_id = sp.supplier_id
"""


def load_models() -> dict:
    s3  = get_s3_client()
    key = f"{MODEL_S3_PREFIX}/demand_forecast_lgbm_{MODEL_VERSION}.pkl"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pickle.loads(obj["Body"].read())


def load_features() -> pd.DataFrame:
    s3  = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=FEATURES_S3_KEY)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"])
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def assign_risk_tier(row: pd.Series) -> str:
    if row["quantity_on_hand"] < row["safety_stock"]:
        return "Critical"
    if row["quantity_on_hand"] < row["reorder_point_ml"]:
        return "High"
    if (row["days_of_stock_remaining"] <
            row["expected_lead_time_days"] * 1.5):
        return "Medium"
    return "Low"


def generate_recommendations() -> pd.DataFrame:
    print("=" * 70)
    print("REORDER RECOMMENDATIONS")
    print("=" * 70)

    print("\n[1/4] Loading models and features …")
    models   = load_models()
    features = load_features()

    print("\n[2/4] Loading latest inventory and supplier data from Athena …")
    inv = run_query(SQL_LATEST_INVENTORY, "latest inventory")
    for col in ["quantity_on_hand", "quantity_on_order", "reorder_point"]:
        inv[col] = pd.to_numeric(inv[col], errors="coerce").fillna(0)

    sup = run_query(SQL_SUPPLIER_LEAD, "supplier lead times")
    for col in ["avg_actual_lead_time_days", "calculated_on_time_rate"]:
        sup[col] = pd.to_numeric(sup[col], errors="coerce")

    print("\n[3/4] Generating 7-day forecasts …")
    latest_date = features["date"].max()
    latest_rows = features[features["date"] == latest_date].copy()

    # collect per-horizon predictions
    horizon_preds = {}
    for h in range(1, N_HORIZONS + 1):
        X_h, _ = make_lgb_dataset(latest_rows, h)
        if len(X_h) == 0:
            continue
        preds = np.clip(models[h].predict(X_h), 0, None)
        meta  = latest_rows.dropna(subset=["lag_28", "target"]).reset_index(drop=True)
        for i, pred in enumerate(preds):
            if i >= len(meta):
                break
            key = (meta.loc[i, "store_id"], meta.loc[i, "product_id"])
            horizon_preds.setdefault(key, {})[h] = pred

    # demand std dev over trailing 14 days (from features)
    demand_std = (
        features.groupby(["store_id", "product_id"])
                .apply(lambda g: g.sort_values("date").tail(14)["target"].std())
                .reset_index()
                .rename(columns={0: "demand_std_14d"})
    )

    print("\n[4/4] Computing reorder quantities …")
    records = []
    for (store_id, product_id), h_preds in horizon_preds.items():
        inv_row = inv[
            (inv["store_id"] == store_id) &
            (inv["product_id"] == product_id)
        ]
        sup_row = sup[sup["product_id"] == product_id]
        std_row = demand_std[
            (demand_std["store_id"] == store_id) &
            (demand_std["product_id"] == product_id)
        ]

        if inv_row.empty or sup_row.empty:
            continue

        qty_on_hand  = float(inv_row["quantity_on_hand"].iloc[0])
        qty_on_order = float(inv_row["quantity_on_order"].iloc[0])
        lead_days    = float(sup_row["avg_actual_lead_time_days"].iloc[0])
        on_time_rate = float(sup_row["calculated_on_time_rate"].iloc[0])
        supplier_id  = str(sup_row["supplier_id"].iloc[0])
        demand_std_val = float(std_row["demand_std_14d"].iloc[0]) if not std_row.empty else 0.0

        # effective lead time penalises unreliable suppliers
        effective_lead = lead_days / max(on_time_rate, 0.01)
        lead_int = max(1, int(round(effective_lead)))

        # forecast demand over lead time
        forecast_demand_lead = sum(
            h_preds.get(h, h_preds.get(min(h_preds.keys()), 0))
            for h in range(1, lead_int + 1)
        )

        # safety stock: z * σ * √L
        safety_stock = SERVICE_LEVEL_Z * demand_std_val * np.sqrt(effective_lead)

        reorder_point_ml = forecast_demand_lead + safety_stock

        recommended_qty = max(0.0, reorder_point_ml - qty_on_hand - qty_on_order)

        # days of stock remaining using h=1 forecast as daily demand proxy
        daily_demand_forecast = h_preds.get(1, 0.01)
        days_of_stock = qty_on_hand / max(daily_demand_forecast, 0.01)

        records.append({
            "store_id":                  store_id,
            "product_id":                product_id,
            "recommendation_date":       str(date.today()),
            "quantity_on_hand":          round(qty_on_hand, 2),
            "quantity_on_order":         round(qty_on_order, 2),
            "daily_demand_forecast":     round(daily_demand_forecast, 4),
            "days_of_stock_remaining":   round(days_of_stock, 2),
            "supplier_id":               supplier_id,
            "avg_lead_time_days":        round(lead_days, 2),
            "on_time_rate":              round(on_time_rate, 4),
            "expected_lead_time_days":   round(effective_lead, 2),
            "forecast_demand_lead_time": round(forecast_demand_lead, 4),
            "demand_std_14d":            round(demand_std_val, 4),
            "safety_stock":              round(safety_stock, 4),
            "reorder_point_ml":          round(reorder_point_ml, 4),
            "recommended_order_qty":     round(recommended_qty, 4),
        })

    recs = pd.DataFrame(records)
    recs["risk_tier"] = recs.apply(assign_risk_tier, axis=1)
    recs = recs.sort_values(
        ["risk_tier", "days_of_stock_remaining"],
        key=lambda s: s.map({"Critical": 0, "High": 1, "Medium": 2, "Low": 3})
                       if s.name == "risk_tier" else s,
    ).reset_index(drop=True)

    # --- summary ---
    tier_counts = recs["risk_tier"].value_counts()
    print(f"\n  Total recommendations : {len(recs):,}")
    for tier in ["Critical", "High", "Medium", "Low"]:
        print(f"  {tier:10s}: {tier_counts.get(tier, 0):,}")

    print("\n  Top 10 Critical/High recommendations:")
    top = recs[recs["risk_tier"].isin(["Critical", "High"])].head(10)
    print(top[[
        "store_id", "product_id", "risk_tier",
        "quantity_on_hand", "days_of_stock_remaining",
        "reorder_point_ml", "recommended_order_qty",
    ]].to_string(index=False))

    # --- write to S3 ---
    today_str = date.today().isoformat()
    key = f"{RECS_S3_PREFIX}/dt={today_str}/recommendations.parquet"
    buf = io.BytesIO()
    recs.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    get_s3_client().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"\n  Recommendations written → s3://{BUCKET}/{key}")

    return recs


if __name__ == "__main__":
    generate_recommendations()
