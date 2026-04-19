"""
ml/features.py
==============
Feature engineering pipeline for demand forecasting.

Reads from Athena mart tables (retailops database, retailops-primary workgroup).
Produces one row per store × product × date with all features needed for training.
Writes output to s3://retailops-data-lake-{region}/ml/features/features.parquet

Run:
    python ml/features.py

Design decisions
----------------
- Minimum 30 days of sales history required per store-product pair.
  Pairs with fewer observations are excluded — a model trained on sparse
  history will produce unreliable lag features and misleading rolling stats.
- All lag/rolling features are computed in pandas after pulling the full
  sales history from Athena. Computing them in SQL would require complex
  window functions and make the query fragile across Athena versions.
- Stockout frequency is computed from fct_inventory_snapshots, not inferred
  from zero-sales days. Zero sales can mean no demand OR stockout — the
  inventory flag disambiguates this, which matters for demand suppression.
- EWM (exponentially weighted mean) is included alongside simple rolling mean
  because it weights recent observations more heavily. On synthetic data with
  fixed multipliers the difference is small, but on real data with trend or
  regime changes EWM is substantially better.
"""

import io
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats as scipy_stats

from athena_client import run_query, get_s3_client, BUCKET, REGION

MIN_HISTORY_DAYS = 30
FEATURES_S3_KEY  = "ml/features/features.parquet"


# ---------------------------------------------------------------------------
# SQL — pull raw data from Athena mart tables
# ---------------------------------------------------------------------------

SQL_SALES = """
    SELECT
        sale_date,
        store_id,
        product_id,
        region,
        store_type,
        category,
        CAST(total_quantity_sold   AS DOUBLE) AS quantity_sold,
        CAST(avg_unit_price        AS DOUBLE) AS unit_price,
        CAST(total_discount_amount AS DOUBLE) AS discount_amount,
        CAST(total_net_amount      AS DOUBLE) AS net_amount,
        CAST(is_weekend            AS BOOLEAN) AS is_weekend,
        CAST(day_of_week           AS INTEGER) AS day_of_week,
        CAST(sale_month            AS INTEGER) AS month_of_year,
        CAST(sale_day              AS INTEGER) AS day_of_month
    FROM retailops_marts.fct_daily_sales
    ORDER BY store_id, product_id, sale_date
"""

SQL_INVENTORY = """
    SELECT
        snapshot_date,
        store_id,
        product_id,
        CAST(quantity_on_hand_clipped AS DOUBLE)  AS quantity_on_hand,
        CAST(quantity_on_order        AS DOUBLE)  AS quantity_on_order,
        CAST(reorder_point            AS DOUBLE)  AS reorder_point,
        CAST(is_out_of_stock          AS BOOLEAN) AS is_out_of_stock,
        CAST(needs_reorder            AS BOOLEAN) AS needs_reorder
    FROM retailops_marts.fct_inventory_snapshots
    ORDER BY store_id, product_id, snapshot_date
"""

SQL_SUPPLIER = """
    SELECT
        p.product_id,
        p.supplier_id,
        CAST(sp.avg_actual_lead_time_days AS DOUBLE) AS avg_actual_lead_time_days,
        CAST(sp.calculated_on_time_rate   AS DOUBLE) AS calculated_on_time_rate,
        CAST(sp.avg_fill_rate             AS DOUBLE) AS avg_fill_rate
    FROM retailops_marts.dim_products p
    LEFT JOIN retailops_marts.mart_supplier_performance sp
      ON p.supplier_id = sp.supplier_id
"""


# ---------------------------------------------------------------------------
# Feature construction helpers
# ---------------------------------------------------------------------------

def _trend_slope(series: pd.Series) -> float:
    """OLS slope of series values against integer index. Returns 0 on failure."""
    y = series.dropna().values
    if len(y) < 3:
        return 0.0
    x = np.arange(len(y))
    slope, *_ = scipy_stats.linregress(x, y)
    return float(slope)


def build_demand_features(sales: pd.DataFrame) -> pd.DataFrame:
    """
    For each store × product time series, compute lag, rolling, EWM,
    and trend features. Returns a flat DataFrame indexed by
    (store_id, product_id, sale_date).
    """
    records = []

    for (store_id, product_id), grp in sales.groupby(["store_id", "product_id"]):
        grp = grp.sort_values("sale_date").set_index("sale_date")
        qty = grp["quantity_sold"]

        if len(qty) < MIN_HISTORY_DAYS:
            continue

        feat = pd.DataFrame(index=grp.index)
        feat["store_id"]    = store_id
        feat["product_id"]  = product_id

        # --- pass-through context columns ---
        for col in ["region", "store_type", "category", "is_weekend",
                    "day_of_week", "month_of_year", "day_of_month",
                    "unit_price", "discount_amount", "net_amount"]:
            feat[col] = grp[col]

        # --- lag features ---
        for lag in [1, 7, 14, 28]:
            feat[f"lag_{lag}"] = qty.shift(lag)

        # --- rolling statistics ---
        for w in [7, 14, 28]:
            r = qty.shift(1).rolling(w, min_periods=max(1, w // 2))
            feat[f"roll_mean_{w}"] = r.mean()
            feat[f"roll_std_{w}"]  = r.std().fillna(0)
            feat[f"roll_min_{w}"]  = r.min()
            feat[f"roll_max_{w}"]  = r.max()

        # --- exponentially weighted mean ---
        for span in [7, 14]:
            feat[f"ewm_mean_{span}"] = qty.shift(1).ewm(span=span, min_periods=3).mean()

        # --- demand trend: OLS slope over trailing 14 days ---
        feat["demand_trend_14d"] = (
            qty.shift(1)
               .rolling(14, min_periods=5)
               .apply(_trend_slope, raw=False)
        )

        # --- promotion / price features ---
        price_30d_avg = grp["unit_price"].shift(1).rolling(30, min_periods=7).mean()
        feat["has_discount"]     = (grp["discount_amount"] > 0).astype(int)
        feat["discount_pct"]     = (
            grp["discount_amount"]
            / (grp["unit_price"] * qty).replace(0, np.nan)
        ).fillna(0).clip(0, 1)
        feat["price_vs_30d_avg"] = (grp["unit_price"] / price_30d_avg).fillna(1.0)

        # --- calendar: days since month start (pay-cycle proxy) ---
        feat["days_since_period_start"] = feat["day_of_month"] - 1

        # --- target: next-day demand (shifted back by 1) ---
        feat["target"] = qty.shift(-1)

        records.append(feat.reset_index().rename(columns={"sale_date": "date"}))

    return pd.concat(records, ignore_index=True)


def build_inventory_features(inventory: pd.DataFrame,
                              demand_features: pd.DataFrame) -> pd.DataFrame:
    """
    Merge inventory snapshot features onto the demand feature table.
    Computes days_of_stock_remaining and stockout_frequency_14d.
    """
    inv = inventory.copy()
    inv["snapshot_date"] = pd.to_datetime(inv["snapshot_date"])

    # stockout_frequency_14d: fraction of last 14 days with is_out_of_stock
    inv_feat = []
    for (store_id, product_id), grp in inv.groupby(["store_id", "product_id"]):
        grp = grp.sort_values("snapshot_date").set_index("snapshot_date")
        f = pd.DataFrame(index=grp.index)
        f["store_id"]   = store_id
        f["product_id"] = product_id
        f["quantity_on_hand"]  = grp["quantity_on_hand"]
        f["quantity_on_order"] = grp["quantity_on_order"]
        f["reorder_point"]     = grp["reorder_point"]
        f["needs_reorder"]     = grp["needs_reorder"].astype(int)
        f["stockout_freq_14d"] = (
            grp["is_out_of_stock"].astype(float)
                                  .rolling(14, min_periods=1)
                                  .mean()
        )
        inv_feat.append(f.reset_index().rename(columns={"snapshot_date": "date"}))

    inv_df = pd.concat(inv_feat, ignore_index=True)

    # days_of_stock_remaining uses rolling_7d_demand from demand features
    merged = demand_features.merge(
        inv_df[["store_id", "product_id", "date",
                "quantity_on_hand", "quantity_on_order", "reorder_point",
                "needs_reorder", "stockout_freq_14d"]],
        on=["store_id", "product_id", "date"],
        how="left",
    )

    roll7 = merged["roll_mean_7"].clip(lower=0.01)
    merged["days_of_stock_remaining"] = (
        merged["quantity_on_hand"].clip(lower=0) / roll7
    )

    return merged


def build_cross_store_features(demand_features: pd.DataFrame) -> pd.DataFrame:
    """
    region_avg_demand_7d: average demand for this product across all stores
    in the same region over the trailing 7 days.
    Captures regional demand signals that a single-store model misses.
    """
    df = demand_features.copy()
    df["date"] = pd.to_datetime(df["date"])

    # rolling 7-day mean per region × product
    region_daily = (
        df.groupby(["region", "product_id", "date"])["lag_1"]
          .mean()
          .reset_index()
          .rename(columns={"lag_1": "_region_lag1"})
    )
    region_daily = region_daily.sort_values(["region", "product_id", "date"])
    region_daily["region_avg_demand_7d"] = (
        region_daily.groupby(["region", "product_id"])["_region_lag1"]
                    .transform(lambda s: s.rolling(7, min_periods=1).mean())
    )

    df = df.merge(
        region_daily[["region", "product_id", "date", "region_avg_demand_7d"]],
        on=["region", "product_id", "date"],
        how="left",
    )
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_features() -> pd.DataFrame:
    print("=" * 70)
    print("FEATURE ENGINEERING PIPELINE")
    print("=" * 70)

    print("\n[1/4] Loading sales history from Athena …")
    sales_raw = run_query(SQL_SALES, "fct_daily_sales")
    sales_raw["sale_date"] = pd.to_datetime(sales_raw["sale_date"])
    for col in ["quantity_sold", "unit_price", "discount_amount", "net_amount"]:
        sales_raw[col] = pd.to_numeric(sales_raw[col], errors="coerce").fillna(0)
    for col in ["day_of_week", "month_of_year", "day_of_month"]:
        sales_raw[col] = pd.to_numeric(sales_raw[col], errors="coerce").fillna(0).astype(int)
    sales_raw["is_weekend"] = sales_raw["is_weekend"].map(
        {"true": True, "false": False, "True": True, "False": False}
    ).fillna(False)

    print(f"     {sales_raw['store_id'].nunique()} stores, "
          f"{sales_raw['product_id'].nunique()} products, "
          f"{sales_raw['sale_date'].nunique()} days")

    print("\n[2/4] Loading inventory snapshots from Athena …")
    inv_raw = run_query(SQL_INVENTORY, "fct_inventory_snapshots")
    inv_raw["snapshot_date"] = pd.to_datetime(inv_raw["snapshot_date"])
    for col in ["quantity_on_hand", "quantity_on_order", "reorder_point"]:
        inv_raw[col] = pd.to_numeric(inv_raw[col], errors="coerce").fillna(0)
    inv_raw["is_out_of_stock"] = inv_raw["is_out_of_stock"].map(
        {"true": True, "false": False, "True": True, "False": False}
    ).fillna(False)
    inv_raw["needs_reorder"] = inv_raw["needs_reorder"].map(
        {"true": True, "false": False, "True": True, "False": False}
    ).fillna(False)

    print("\n[3/4] Loading supplier features from Athena …")
    sup_raw = run_query(SQL_SUPPLIER, "dim_products + mart_supplier_performance")
    for col in ["avg_actual_lead_time_days", "calculated_on_time_rate", "avg_fill_rate"]:
        sup_raw[col] = pd.to_numeric(sup_raw[col], errors="coerce")

    print("\n[4/4] Building features …")
    demand_feat = build_demand_features(sales_raw)
    print(f"     demand features: {len(demand_feat):,} rows")

    features = build_inventory_features(inv_raw, demand_feat)
    features = build_cross_store_features(features)

    # merge supplier features (static per product)
    features = features.merge(
        sup_raw[["product_id", "supplier_id",
                 "avg_actual_lead_time_days",
                 "calculated_on_time_rate",
                 "avg_fill_rate"]],
        on="product_id", how="left",
    )

    # Drop rows with insufficient lag history (first 28 days of each series).
    # Do NOT drop rows where target is NaN here — the last row of each series
    # (2026-02-10) has target=NaN because there is no 2026-02-11 observation yet.
    # That row is the inference row: it holds the features needed to predict
    # 2026-02-11 onward. Dropping it here would make forecast generation impossible.
    # train.py filters to target.notna() for training; evaluate.py and
    # reorder_recommendations.py use the NaN-target rows for inference.
    features = features.dropna(subset=["lag_28"])

    features["date"] = pd.to_datetime(features["date"])
    features = features.sort_values(["store_id", "product_id", "date"]).reset_index(drop=True)

    n_train = features["target"].notna().sum()
    n_infer = features["target"].isna().sum()
    print(f"\n     Final feature matrix : {len(features):,} rows x {len(features.columns)} columns")
    print(f"     Training rows (target known)  : {n_train:,}")
    print(f"     Inference rows (target=NaN)   : {n_infer:,}  <- last date per series (2026-02-10)")
    print(f"     Date range: {features['date'].min().date()} -> {features['date'].max().date()}")
    print(f"     Store-product pairs: {features.groupby(['store_id','product_id']).ngroups:,}")

    # --- write to S3 ---
    print(f"\n     Writing to s3://{BUCKET}/{FEATURES_S3_KEY} …")
    buf = io.BytesIO()
    features.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    get_s3_client().put_object(Bucket=BUCKET, Key=FEATURES_S3_KEY, Body=buf.getvalue())
    print("     Done.")

    return features


if __name__ == "__main__":
    build_features()
