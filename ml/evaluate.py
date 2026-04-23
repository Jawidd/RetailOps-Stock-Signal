"""
ml/evaluate.py
==============
Model evaluation, error analysis, SHAP, and forecast generation.

Loads the trained LightGBM models from S3, runs walk-forward evaluation
across all four folds, breaks down errors by segment, runs SHAP analysis,
and writes the forecast table to S3 as Parquet.

Run:
    python ml/evaluate.py
"""

import io
import json
import os
import pickle
import warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
import shap
from datetime import datetime, date
from pathlib import Path

from athena_client import get_s3_client, BUCKET
from train import (
    FEATURE_COLS, CATEGORICAL_COLS, N_HORIZONS, MODEL_VERSION,
    MODEL_S3_PREFIX, FEATURES_S3_KEY, FOLDS,
    wape, rmse, bias, fva, make_lgb_dataset,
)

warnings.filterwarnings("ignore")

FORECASTS_S3_PREFIX = "ml/forecasts"
ML_LOCAL_ARTIFACT_DIR = os.getenv("ML_LOCAL_ARTIFACT_DIR", "").strip()
if ML_LOCAL_ARTIFACT_DIR:
    LOCAL_MODEL_PATH = Path(ML_LOCAL_ARTIFACT_DIR) / f"demand_forecast_lgbm_{MODEL_VERSION}.pkl"
    LOCAL_METADATA_PATH = Path(ML_LOCAL_ARTIFACT_DIR) / f"demand_forecast_lgbm_{MODEL_VERSION}_metadata.json"
    LOCAL_FEATURES_PATH = Path(ML_LOCAL_ARTIFACT_DIR) / "ml_features.parquet"
    LOCAL_FORECASTS_DIR = Path(ML_LOCAL_ARTIFACT_DIR) / "ml_forecasts"
    LOCAL_REPORTS_DIR = Path(ML_LOCAL_ARTIFACT_DIR) / "ml_evaluation"
else:
    LOCAL_MODEL_PATH = None
    LOCAL_METADATA_PATH = None
    LOCAL_FEATURES_PATH = None
    LOCAL_FORECASTS_DIR = None
    LOCAL_REPORTS_DIR = None


# ---------------------------------------------------------------------------
# Load artifacts
# ---------------------------------------------------------------------------

def _pipeline_date() -> str:
    """Return PIPELINE_DATE env var if set, otherwise today UTC."""
    return os.environ.get("PIPELINE_DATE", "").strip() or datetime.utcnow().strftime("%Y-%m-%d")


def load_models() -> dict:
    if LOCAL_MODEL_PATH is not None and LOCAL_MODEL_PATH.exists():
        print(f"  Loading model from local file: {LOCAL_MODEL_PATH}")
        with open(LOCAL_MODEL_PATH, "rb") as f:
            return pickle.load(f)

    s3 = get_s3_client()
    key = f"{MODEL_S3_PREFIX}/demand_forecast_lgbm_{MODEL_VERSION}.pkl"
    for attempt in range(3):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            return pickle.loads(obj["Body"].read())
        except Exception as e:
            if attempt == 2:
                raise
            import time
            print(f"  load_models attempt {attempt+1} failed ({e}), retrying...")
            time.sleep(5)


def load_metadata() -> dict:
    if LOCAL_METADATA_PATH is not None and LOCAL_METADATA_PATH.exists():
        print(f"  Loading metadata from local file: {LOCAL_METADATA_PATH}")
        with open(LOCAL_METADATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    s3 = get_s3_client()
    key = f"{MODEL_S3_PREFIX}/demand_forecast_lgbm_{MODEL_VERSION}_metadata.json"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def load_features() -> pd.DataFrame:
    if LOCAL_FEATURES_PATH is not None and LOCAL_FEATURES_PATH.exists():
        print(f"  Loading features from local file: {LOCAL_FEATURES_PATH}")
        df = pd.read_parquet(LOCAL_FEATURES_PATH)
    else:
        s3 = get_s3_client()
        obj = s3.get_object(Bucket=BUCKET, Key=FEATURES_S3_KEY)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"])
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


# ---------------------------------------------------------------------------
# Walk-forward evaluation
# ---------------------------------------------------------------------------

def full_walkforward_eval(models: dict, df: pd.DataFrame,
                          day0: pd.Timestamp) -> pd.DataFrame:
    """
    Evaluate each horizon model on each fold's validation window.
    Returns a DataFrame with one row per (fold, horizon) with all metrics.
    """
    rows = []
    for fold_idx, fold in enumerate(FOLDS, 1):
        train_end = day0 + pd.Timedelta(days=fold["train_end"] - 1)
        val_start = day0 + pd.Timedelta(days=fold["val_start"] - 1)
        val_end   = day0 + pd.Timedelta(days=fold["val_end"]   - 1)

        train_df = df[df["date"] <= train_end]
        val_df   = df[(df["date"] >= val_start) & (df["date"] <= val_end)]

        for h in range(1, N_HORIZONS + 1):
            X_tr, y_tr   = make_lgb_dataset(train_df, h)
            X_val, y_val = make_lgb_dataset(val_df, h)

            if len(X_val) == 0:
                continue

            # retrain on this fold's training window for honest evaluation
            m = lgb.LGBMRegressor(
                **{k: v for k, v in models[h].get_params().items()
                   if k not in ("n_estimators", "random_state", "verbose")},
                n_estimators=500, random_state=42, verbose=-1
            )
            m.fit(X_tr, y_tr)
            preds = np.clip(m.predict(X_val), 0, None)

            rows.append({
                "fold":    fold_idx,
                "horizon": h,
                "wape":    wape(y_val, preds),
                "rmse":    rmse(y_val, preds),
                "bias":    bias(y_val, preds),
                "n_obs":   len(y_val),
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Segment error analysis
# ---------------------------------------------------------------------------

def _best_eval_fold(df: pd.DataFrame, day0: pd.Timestamp) -> dict | None:
    """
    Return the most recent fold whose validation window has labelled data.
    Falls back through folds 4→3→2→1. Returns None if no fold has data.
    """
    df_train = df[df["target"].notna()]
    for fold in reversed(FOLDS):
        val_start = day0 + pd.Timedelta(days=fold["val_start"] - 1)
        val_end   = day0 + pd.Timedelta(days=fold["val_end"]   - 1)
        if len(df_train[(df_train["date"] >= val_start) & (df_train["date"] <= val_end)]) > 0:
            return fold
    return None


def segment_error_analysis(models: dict, df: pd.DataFrame,
                            day0: pd.Timestamp) -> dict:
    """
    Evaluate h=1 model on fold 4 validation window, broken down by:
    - product category
    - store type
    - demand level (high/medium/low)
    - day of week
    - promotion flag
    """
    fold = _best_eval_fold(df, day0)
    if fold is None:
        print("  Skipping segment analysis: no validation data in any fold.")
        return {}
    train_end = day0 + pd.Timedelta(days=fold["train_end"] - 1)
    val_start = day0 + pd.Timedelta(days=fold["val_start"] - 1)
    val_end   = day0 + pd.Timedelta(days=fold["val_end"]   - 1)

    train_df = df[df["date"] <= train_end]
    val_df   = df[(df["date"] >= val_start) & (df["date"] <= val_end)].copy()

    X_tr, y_tr = make_lgb_dataset(train_df, 1)
    X_val, _   = make_lgb_dataset(val_df, 1)

    if len(X_tr) == 0 or len(X_val) == 0:
        print("  Skipping segment analysis: insufficient train or val data.")
        return {}

    m = lgb.LGBMRegressor(
        **{k: v for k, v in models[1].get_params().items()
           if k not in ("n_estimators", "random_state", "verbose")},
        n_estimators=500, random_state=42, verbose=-1
    )
    m.fit(X_tr, y_tr)

    val_df = val_df.dropna(subset=["lag_28", "target"])
    val_df["pred"] = np.clip(m.predict(X_val), 0, None)

    # demand level buckets based on training set median
    demand_median = train_df["target"].median()
    demand_p75    = train_df["target"].quantile(0.75)
    val_df["demand_level"] = pd.cut(
        val_df["target"],
        bins=[-np.inf, demand_median, demand_p75, np.inf],
        labels=["low", "medium", "high"],
    )

    segments = {}
    for seg_col in ["category", "store_type", "demand_level", "day_of_week", "has_discount"]:
        if seg_col not in val_df.columns:
            continue
        seg_results = {}
        for seg_val, grp in val_df.groupby(seg_col):
            a, p = grp["target"].values, grp["pred"].values
            seg_results[str(seg_val)] = {
                "wape": wape(a, p),
                "rmse": rmse(a, p),
                "bias": bias(a, p),
                "n":    len(a),
            }
        segments[seg_col] = seg_results

    return segments


# ---------------------------------------------------------------------------
# SHAP analysis
# ---------------------------------------------------------------------------

def run_shap_analysis(models: dict, df: pd.DataFrame,
                      day0: pd.Timestamp) -> dict:
    fold = _best_eval_fold(df, day0)
    if fold is None:
        print("  Skipping SHAP: no validation data in any fold.")
        return {"global_importance": {}, "worst_pairs_importance": {},
                "top_feature": "n/a", "lag_vs_calendar_ratio": 0.0}

    train_end = day0 + pd.Timedelta(days=fold["train_end"] - 1)
    train_df  = df[df["date"] <= train_end]
    X_tr, y_tr = make_lgb_dataset(train_df, 1)

    if len(X_tr) == 0:
        print("  Skipping SHAP: insufficient training data.")
        return {"global_importance": {}, "worst_pairs_importance": {},
                "top_feature": "n/a", "lag_vs_calendar_ratio": 0.0}

    m = lgb.LGBMRegressor(
        **{k: v for k, v in models[1].get_params().items()
           if k not in ("n_estimators", "random_state", "verbose")},
        n_estimators=500, random_state=42, verbose=-1
    )
    m.fit(X_tr, y_tr)

    # Sample from training data — must use the same X that was passed to fit()
    # so categorical levels are consistent with what the model saw.
    sample_idx = X_tr.sample(min(500, len(X_tr)), random_state=42).index
    sample = X_tr.loc[sample_idx]

    try:
        explainer   = shap.TreeExplainer(m)
        shap_values = explainer.shap_values(sample[FEATURE_COLS])
    except Exception as e:
        print(f"  SHAP failed ({e}), skipping.")
        return {"global_importance": {}, "worst_pairs_importance": {},
                "top_feature": "n/a", "lag_vs_calendar_ratio": 0.0}

    mean_abs_shap = pd.Series(
        np.abs(shap_values).mean(axis=0),
        index=FEATURE_COLS,
    ).sort_values(ascending=False)

    worst_idx    = np.argsort(np.abs(shap_values).sum(axis=1))[-50:]
    worst_sample = sample.iloc[worst_idx][FEATURE_COLS]
    worst_shap   = explainer.shap_values(worst_sample)
    worst_importance = pd.Series(
        np.abs(worst_shap).mean(axis=0),
        index=FEATURE_COLS,
    ).sort_values(ascending=False)

    lag_sum = mean_abs_shap[[c for c in FEATURE_COLS if c.startswith("lag_")]].sum()
    cal_sum = mean_abs_shap[[c for c in FEATURE_COLS if c in
                              ["day_of_week", "month_of_year", "is_weekend",
                               "days_since_period_start"]]].sum()

    return {
        "global_importance":      mean_abs_shap.head(20).to_dict(),
        "worst_pairs_importance": worst_importance.head(5).to_dict(),
        "top_feature":            mean_abs_shap.index[0],
        "lag_vs_calendar_ratio":  float(lag_sum / cal_sum) if cal_sum > 0 else 0.0,
    }


# ---------------------------------------------------------------------------
# Generate forecasts for the latest date
# ---------------------------------------------------------------------------

def generate_forecasts(models: dict, df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate h=1..7 forecasts from the inference rows: the last date per
    store x product series where target=NaN (2026-02-10 in the current dataset).
    These rows have all lag/rolling features computed from observed history
    but no label, which is exactly the real-world forecasting situation.

    h=1 forecast -> predicted demand for 2026-02-11
    h=2 forecast -> predicted demand for 2026-02-12
    ...and so on.
    """
    # Use NaN-target rows as the inference slice.
    # Fall back to the last labelled date if no NaN-target rows exist
    # (e.g. if features.py was run with the old drop-NaN behaviour).
    infer_rows = df[df["target"].isna()].copy()
    if infer_rows.empty:
        latest_date = df["date"].max()
        infer_rows = df[df["date"] == latest_date].copy()
        print(f"  Warning: no NaN-target rows found, falling back to date={latest_date.date()}")

    inference_date = infer_rows["date"].max()
    print(f"  Inference date (features from): {inference_date.date()}")
    print(f"  Forecasting dates: {(inference_date + pd.Timedelta(days=1)).date()} "
          f"to {(inference_date + pd.Timedelta(days=N_HORIZONS)).date()}")

    # For inference we only need the feature columns, not a valid target.
    # Drop rows missing any feature (lag_28 is the strictest requirement).
    infer_clean = infer_rows.dropna(subset=FEATURE_COLS).copy()
    for col in CATEGORICAL_COLS:
        infer_clean[col] = infer_clean[col].astype("category")

    records = []
    for h in range(1, N_HORIZONS + 1):
        X_infer = infer_clean[FEATURE_COLS + CATEGORICAL_COLS].copy()
        if len(X_infer) == 0:
            continue
        preds = np.clip(models[h].predict(X_infer), 0, None)
        forecast_date = inference_date + pd.Timedelta(days=h)
        for i, pred in enumerate(preds):
            records.append({
                "store_id":                infer_clean.iloc[i]["store_id"],
                "product_id":              infer_clean.iloc[i]["product_id"],
                "forecast_generated_date": str(inference_date.date()),
                "horizon_day":             h,
                "forecast_date":           str(forecast_date.date()),
                "predicted_quantity":      round(float(pred), 4),
                "model_version":           MODEL_VERSION,
            })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def evaluate():
    print("=" * 70)
    print("DEMAND FORECASTING — EVALUATION")
    print("=" * 70)

    print("\nLoading artifacts …")
    models   = load_models()
    metadata = load_metadata()
    df       = load_features()
    day0     = df["date"].min()

    # --- walk-forward metrics ---
    print("\n[1/4] Walk-forward evaluation across all folds …")
    wf_results = full_walkforward_eval(models, df, day0)
    print("\n  Walk-forward results (mean across folds):")
    summary = wf_results.groupby("horizon")[["wape", "rmse", "bias"]].mean()
    print(summary.round(4).to_string())
    overall_wape = wf_results["wape"].mean()
    print(f"\n  Overall mean WAPE: {overall_wape:.4f}")

    baseline_wape = metadata["baseline_metrics"]["seasonal_naive"]["wape"]
    if baseline_wape and not np.isnan(float(baseline_wape)):
        print(f"  Seasonal naive WAPE: {float(baseline_wape):.4f}")
        print(f"  FVA (model / seasonal_naive): {fva(overall_wape, float(baseline_wape)):.3f}")
    else:
        print("  Seasonal naive WAPE: n/a (fold outside data range)")
        baseline_wape = None

    # --- segment error analysis ---
    print("\n[2/4] Segment error analysis …")
    segments = segment_error_analysis(models, df, day0)
    for seg_name, seg_data in segments.items():
        print(f"\n  By {seg_name}:")
        for seg_val, m in sorted(seg_data.items(),
                                  key=lambda x: x[1]["wape"], reverse=True):
            print(f"    {str(seg_val):20s}  WAPE={m['wape']:.4f}  "
                  f"RMSE={m['rmse']:.3f}  Bias={m['bias']:.3f}  n={m['n']}")

    # --- SHAP ---
    print("\n[3/4] SHAP analysis …")
    shap_results = run_shap_analysis(models, df, day0)
    print(f"\n  Top feature (global): {shap_results['top_feature']}")
    print(f"  Lag vs calendar SHAP ratio: {shap_results['lag_vs_calendar_ratio']:.2f}")
    print("  Global top-10 features:")
    for feat, val in list(shap_results["global_importance"].items())[:10]:
        print(f"    {feat:30s}  {val:.4f}")
    print("  Top-5 features on worst-predicted pairs:")
    for feat, val in shap_results["worst_pairs_importance"].items():
        print(f"    {feat:30s}  {val:.4f}")

    # --- generate and save forecasts ---
    print("\n[4/4] Generating forecasts for latest date ...")
    forecasts = generate_forecasts(models, df)
    print(f"  {len(forecasts):,} forecast rows generated")

    pipeline_date = _pipeline_date()
    forecast_key = f"{FORECASTS_S3_PREFIX}/dt={pipeline_date}/forecasts.parquet"
    if LOCAL_FORECASTS_DIR is not None:
        LOCAL_FORECASTS_DIR.mkdir(parents=True, exist_ok=True)
        local_forecast_path = LOCAL_FORECASTS_DIR / f"forecasts_{pipeline_date}.parquet"
        forecasts.to_parquet(local_forecast_path, index=False, engine="pyarrow")
        print(f"  Forecasts written -> {local_forecast_path}")
    else:
        buf = io.BytesIO()
        forecasts.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        get_s3_client().put_object(Bucket=BUCKET, Key=forecast_key, Body=buf.getvalue())
        print(f"  Forecasts written -> s3://{BUCKET}/{forecast_key}")

    # --- save evaluation report ---
    report = {
        "evaluated_at":       datetime.utcnow().isoformat(),
        "pipeline_date":      pipeline_date,
        "model_version":      MODEL_VERSION,
        "overall_wape":       overall_wape,
        "fva":                fva(overall_wape, float(baseline_wape)) if baseline_wape else None,
        "walkforward_detail": wf_results.to_dict(orient="records"),
        "segment_analysis":   segments,
        "shap_summary":       shap_results,
    }
    report_key = f"ml/evaluation/eval_{pipeline_date}.json"
    if LOCAL_REPORTS_DIR is not None:
        LOCAL_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        local_report_path = LOCAL_REPORTS_DIR / f"eval_{pipeline_date}.json"
        with open(local_report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"  Evaluation report -> {local_report_path}")
    else:
        get_s3_client().put_object(
            Bucket=BUCKET, Key=report_key,
            Body=json.dumps(report, indent=2, default=str).encode()
        )
        print(f"  Evaluation report -> s3://{BUCKET}/{report_key}")

    return report


if __name__ == "__main__":
    evaluate()
