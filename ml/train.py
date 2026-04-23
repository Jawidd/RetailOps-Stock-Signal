"""
ml/train.py
===========
Demand forecasting model training.

Walk-forward cross-validation → Optuna hyperparameter tuning → final LightGBM
model trained on all available data → artifacts saved to S3.

Run:
    python ml/train.py

Walk-forward validation design
-------------------------------
We use expanding-window walk-forward CV, not a single train/test split.
A random split on time-series data leaks future information into training
(a row from day 150 in the training set contains lag features computed from
day 149, which may be in the "test" set). Walk-forward CV respects the
temporal ordering of the data and produces metrics that reflect real
deployment conditions.

Fold structure (days relative to the first date in the dataset):
    Fold 1: train 1–90,   validate 91–97
    Fold 2: train 1–110,  validate 111–117
    Fold 3: train 1–130,  validate 131–137
    Fold 4: train 1–150,  validate 151–157

Metric: WAPE (weighted absolute percentage error)
    WAPE = sum(|actual - forecast|) / sum(actual)
WAPE is preferred over MAPE because it weights errors by volume — a 10-unit
error on a 10-unit SKU is catastrophic; the same error on a 1,000-unit SKU
is negligible. MAPE treats both identically and is dominated by low-volume
SKUs where percentage errors are large by construction.

Model: LightGBM direct multi-step
    One model per horizon day (h=1 … 7). Each model predicts demand h days
    ahead using features computed at the time of forecasting. This avoids
    error accumulation from recursive forecasting and allows each horizon to
    learn different feature relationships (e.g. lag_7 is more informative
    for h=7 than for h=1).
"""

import io
import json
import os
import pickle
import warnings
import numpy as np
import pandas as pd
import lightgbm as lgb
import optuna
import boto3
from datetime import datetime
from pathlib import Path

from athena_client import get_s3_client, BUCKET

warnings.filterwarnings("ignore", category=UserWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)

FEATURES_S3_KEY   = "ml/features/features.parquet"
MODEL_VERSION     = "v1"
MODEL_S3_PREFIX   = "ml/models"
N_HORIZONS        = 7
N_OPTUNA_TRIALS   = 50

ML_LOCAL_ARTIFACT_DIR = os.getenv("ML_LOCAL_ARTIFACT_DIR", "").strip()
if ML_LOCAL_ARTIFACT_DIR:
    LOCAL_FEATURES_PATH = Path(ML_LOCAL_ARTIFACT_DIR) / "ml_features.parquet"
    LOCAL_MODEL_PATH = Path(ML_LOCAL_ARTIFACT_DIR) / f"demand_forecast_lgbm_{MODEL_VERSION}.pkl"
    LOCAL_METADATA_PATH = Path(ML_LOCAL_ARTIFACT_DIR) / f"demand_forecast_lgbm_{MODEL_VERSION}_metadata.json"
else:
    LOCAL_FEATURES_PATH = None
    LOCAL_MODEL_PATH = None
    LOCAL_METADATA_PATH = None

FEATURE_COLS = [
    "lag_1", "lag_7", "lag_14", "lag_28",
    "roll_mean_7", "roll_std_7", "roll_min_7", "roll_max_7",
    "roll_mean_14", "roll_std_14", "roll_min_14", "roll_max_14",
    "roll_mean_28", "roll_std_28", "roll_min_28", "roll_max_28",
    "ewm_mean_7", "ewm_mean_14",
    "demand_trend_14d",
    "has_discount", "discount_pct", "price_vs_30d_avg",
    "days_since_period_start", "day_of_week", "month_of_year",
    "is_weekend",
    "quantity_on_hand", "quantity_on_order", "reorder_point",
    "needs_reorder", "stockout_freq_14d", "days_of_stock_remaining",
    "avg_actual_lead_time_days", "calculated_on_time_rate", "avg_fill_rate",
    "region_avg_demand_7d",
]

CATEGORICAL_COLS = ["store_id", "product_id", "region", "store_type", "category"]


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def wape(actual: np.ndarray, forecast: np.ndarray) -> float:
    denom = np.sum(np.abs(actual))
    return float(np.sum(np.abs(actual - forecast)) / denom) if denom > 0 else np.nan


def rmse(actual: np.ndarray, forecast: np.ndarray) -> float:
    return float(np.sqrt(np.mean((actual - forecast) ** 2)))


def bias(actual: np.ndarray, forecast: np.ndarray) -> float:
    """Mean signed error. Positive = over-forecast, negative = under-forecast."""
    return float(np.mean(forecast - actual))


def fva(model_wape: float, baseline_wape: float) -> float:
    """Forecast Value Added. < 1.0 means model beats seasonal naive."""
    return model_wape / baseline_wape if baseline_wape > 0 else np.nan


# ---------------------------------------------------------------------------
# Baseline forecasts
# ---------------------------------------------------------------------------

def naive_forecast(series: pd.Series) -> float:
    """Last observed value."""
    return float(series.iloc[-1]) if len(series) > 0 else 0.0


def seasonal_naive_forecast(series: pd.Series, horizon: int = 1) -> float:
    """Same day last week (lag 7)."""
    idx = -(7 - horizon + 1)
    return float(series.iloc[idx]) if len(series) >= abs(idx) else float(series.mean())


def rolling_mean_forecast(series: pd.Series, window: int = 7) -> float:
    return float(series.tail(window).mean())


def evaluate_baselines(df: pd.DataFrame,
                       train_end_day: int,
                       val_start_day: int,
                       val_end_day: int,
                       day0: pd.Timestamp) -> dict:
    """
    Evaluate all three baselines on a single validation window.
    Returns dict of {baseline_name: {wape, rmse, bias}}.
    """
    train_end = day0 + pd.Timedelta(days=train_end_day - 1)
    val_start = day0 + pd.Timedelta(days=val_start_day - 1)
    val_end   = day0 + pd.Timedelta(days=val_end_day - 1)

    train = df[df["date"] <= train_end]
    val   = df[(df["date"] >= val_start) & (df["date"] <= val_end)]

    results = {}
    for name, forecast_fn in [
        ("naive",          lambda s: naive_forecast(s)),
        ("seasonal_naive", lambda s: seasonal_naive_forecast(s)),
        ("rolling_mean",   lambda s: rolling_mean_forecast(s)),
    ]:
        preds, actuals = [], []
        for (store_id, product_id), val_grp in val.groupby(["store_id", "product_id"]):
            hist = train[
                (train["store_id"] == store_id) &
                (train["product_id"] == product_id)
            ]["target"]
            if len(hist) == 0:
                continue
            pred = forecast_fn(hist)
            for actual in val_grp["target"].values:
                preds.append(pred)
                actuals.append(actual)

        a, p = np.array(actuals), np.array(preds)
        results[name] = {"wape": wape(a, p), "rmse": rmse(a, p), "bias": bias(a, p)}

    return results


# ---------------------------------------------------------------------------
# Walk-forward CV folds
# ---------------------------------------------------------------------------

# Walk-forward CV folds anchored to the actual data interval 2025-07-01 -> 2026-02-10.
# Total labelled days = 224 (2025-07-01 to 2026-02-09, since the target for day N
# is demand on day N+1, so the last labelled row is 2026-02-09 with target=2026-02-10).
#
# Fold structure (day offsets from day0 = 2025-07-01, 1-indexed):
#   Fold 1: train days   1-120  (2025-07-01 -> 2025-10-28)  val days 121-127
#   Fold 2: train days   1-148  (2025-07-01 -> 2025-11-25)  val days 149-155
#   Fold 3: train days   1-176  (2025-07-01 -> 2025-12-23)  val days 177-183
#   Fold 4: train days   1-210  (2025-07-01 -> 2026-01-26)  val days 211-217
#                                                            (2026-01-27 -> 2026-02-02)
# Fold 4 is the most important: its validation window is the 7 days immediately
# before the final 7 days of labelled data, so it best reflects the model's
# performance on the prediction task (forecasting Feb 2026).
FOLDS = [
    {"train_end": 120, "val_start": 121, "val_end": 127},
    {"train_end": 148, "val_start": 149, "val_end": 155},
    {"train_end": 176, "val_start": 177, "val_end": 183},
    {"train_end": 210, "val_start": 211, "val_end": 217},
]


def make_lgb_dataset(df: pd.DataFrame, horizon: int) -> tuple:
    """
    Shift target by `horizon` days to create h-step-ahead labels.
    Only uses rows where the original target is known (not NaN) — inference
    rows (last date per series, target=NaN) are excluded here and handled
    separately in evaluate.py and reorder_recommendations.py.
    Returns (X, y) with NaN rows dropped.
    """
    df = df.copy()
    # Restrict to labelled rows only before shifting
    df = df[df["target"].notna()]
    df["target_h"] = df.groupby(["store_id", "product_id"])["target"].shift(-(horizon - 1))
    df = df.dropna(subset=["target_h"] + FEATURE_COLS)

    X = df[FEATURE_COLS + CATEGORICAL_COLS].copy()
    for col in CATEGORICAL_COLS:
        X[col] = X[col].astype("category")
    y = df["target_h"].values
    return X, y


def cv_wape_for_params(params: dict, df: pd.DataFrame, day0: pd.Timestamp) -> float:
    """
    Run walk-forward CV with given LightGBM params.
    Returns mean WAPE across all folds and horizons.
    """
    fold_wapes = []

    for fold in FOLDS:
        train_end = day0 + pd.Timedelta(days=fold["train_end"] - 1)
        val_start = day0 + pd.Timedelta(days=fold["val_start"] - 1)
        val_end   = day0 + pd.Timedelta(days=fold["val_end"]   - 1)

        train_df = df[df["date"] <= train_end]
        val_df   = df[(df["date"] >= val_start) & (df["date"] <= val_end)]

        if len(val_df) == 0:
            continue

        for h in range(1, N_HORIZONS + 1):
            X_tr, y_tr = make_lgb_dataset(train_df, h)
            X_val, y_val = make_lgb_dataset(val_df, h)

            if len(X_tr) == 0 or len(X_val) == 0:
                continue

            model = lgb.LGBMRegressor(**params, n_estimators=300, random_state=42, verbose=-1)
            model.fit(
                X_tr, y_tr,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(30, verbose=False),
                           lgb.log_evaluation(-1)],
            )
            preds = np.clip(model.predict(X_val), 0, None)
            fold_wapes.append(wape(y_val, preds))

    return float(np.mean(fold_wapes)) if fold_wapes else 1.0


# ---------------------------------------------------------------------------
# Optuna objective
# ---------------------------------------------------------------------------

def make_objective(df: pd.DataFrame, day0: pd.Timestamp):
    def objective(trial: optuna.Trial) -> float:
        params = {
            "num_leaves":        trial.suggest_int("num_leaves", 20, 150),
            "learning_rate":     trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 50),
            "feature_fraction":  trial.suggest_float("feature_fraction", 0.5, 1.0),
            "bagging_fraction":  trial.suggest_float("bagging_fraction", 0.5, 1.0),
            "bagging_freq":      1,
            "reg_alpha":         trial.suggest_float("reg_alpha", 1e-4, 1.0, log=True),
            "reg_lambda":        trial.suggest_float("reg_lambda", 1e-4, 1.0, log=True),
            "objective":         "regression_l1",
        }
        return cv_wape_for_params(params, df, day0)
    return objective


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_features() -> pd.DataFrame:
    if LOCAL_FEATURES_PATH is not None and LOCAL_FEATURES_PATH.exists():
        print(f"  Loading features from local file: {LOCAL_FEATURES_PATH}")
        df = pd.read_parquet(LOCAL_FEATURES_PATH)
    else:
        print("  Loading features from S3 …")
        s3 = get_s3_client()
        obj = s3.get_object(Bucket=BUCKET, Key=FEATURES_S3_KEY)
        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    df["date"] = pd.to_datetime(df["date"])
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def train():
    print("=" * 70)
    print("DEMAND FORECASTING — TRAINING")
    print("=" * 70)

    df = load_features()
    # Separate training rows (target known) from inference rows (target=NaN).
    # Training and CV only ever see labelled rows.
    df_train = df[df["target"].notna()].copy()
    day0 = df_train["date"].min()
    print(f"  Training data  : {day0.date()} -> {df_train['date'].max().date()}")
    print(f"  Inference date : {df[df['target'].isna()]['date'].max().date()} (features ready, no label yet)")
    print(f"  Labelled rows  : {len(df_train):,}")
    print(f"  Pairs          : {df_train.groupby(['store_id','product_id']).ngroups:,}")

    # --- baseline evaluation on fold 4 (most recent, closest to prediction boundary) ---
    print("\n[1/3] Evaluating baselines ...")
    fold4 = FOLDS[-1]
    baseline_metrics = evaluate_baselines(
        df_train, fold4["train_end"], fold4["val_start"], fold4["val_end"], day0
    )
    for name, m in baseline_metrics.items():
        print(f"  {name:20s}  WAPE={m['wape']:.4f}  RMSE={m['rmse']:.3f}  Bias={m['bias']:.3f}")

    # --- Optuna hyperparameter search ---
    print(f"\n[2/3] Optuna search ({N_OPTUNA_TRIALS} trials) ...")
    study = optuna.create_study(direction="minimize",
                                sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(make_objective(df_train, day0), n_trials=N_OPTUNA_TRIALS, show_progress_bar=False)

    best_params = study.best_params
    best_params.update({"objective": "regression_l1", "bagging_freq": 1})
    print(f"  Best WAPE  : {study.best_value:.4f}")
    print(f"  Best params: {best_params}")

    # --- train final models on ALL labelled data (up to 2026-02-09) ---
    print("\n[3/3] Training final models on full labelled dataset ...")
    models = {}
    cv_metrics = {}

    for h in range(1, N_HORIZONS + 1):
        X_all, y_all = make_lgb_dataset(df_train, h)
        model = lgb.LGBMRegressor(
            **best_params, n_estimators=500, random_state=42, verbose=-1
        )
        model.fit(X_all, y_all)
        models[h] = model

        # evaluate on fold 4 for reporting
        fold4_train_end = day0 + pd.Timedelta(days=fold4["train_end"] - 1)
        fold4_val_start = day0 + pd.Timedelta(days=fold4["val_start"] - 1)
        fold4_val_end   = day0 + pd.Timedelta(days=fold4["val_end"]   - 1)

        X_tr, y_tr   = make_lgb_dataset(df_train[df_train["date"] <= fold4_train_end], h)
        X_val, y_val = make_lgb_dataset(
            df_train[(df_train["date"] >= fold4_val_start) & (df_train["date"] <= fold4_val_end)], h
        )

        if len(X_val) > 0:
            eval_model = lgb.LGBMRegressor(
                **best_params, n_estimators=500, random_state=42, verbose=-1
            )
            eval_model.fit(X_tr, y_tr)
            preds = np.clip(eval_model.predict(X_val), 0, None)
            cv_metrics[f"h{h}"] = {
                "wape": wape(y_val, preds),
                "rmse": rmse(y_val, preds),
                "bias": bias(y_val, preds),
                "fva":  fva(wape(y_val, preds),
                            baseline_metrics["seasonal_naive"]["wape"]),
            }
            print(f"  h={h}  WAPE={cv_metrics[f'h{h}']['wape']:.4f}"
                  f"  RMSE={cv_metrics[f'h{h}']['rmse']:.3f}"
                  f"  FVA={cv_metrics[f'h{h}']['fva']:.3f}")

    metadata = {
        "model_version":   MODEL_VERSION,
        "trained_at":      datetime.utcnow().isoformat(),
        "n_horizons":      N_HORIZONS,
        "feature_cols":    FEATURE_COLS,
        "categorical_cols": CATEGORICAL_COLS,
        "best_params":     best_params,
        "optuna_best_wape": study.best_value,
        "cv_metrics":      cv_metrics,
        "baseline_metrics": baseline_metrics,
        "training_date_range": {
            "min": str(df_train["date"].min().date()),
            "max": str(df_train["date"].max().date()),
        },
        "inference_from": str(df[df["target"].isna()]["date"].max().date()),
        "n_rows":  len(df_train),
        "n_pairs": df_train.groupby(["store_id", "product_id"]).ngroups,
    }

    if LOCAL_MODEL_PATH is not None:
        Path(ML_LOCAL_ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
        print(f"\n  Saving local model -> {LOCAL_MODEL_PATH}")
        with open(LOCAL_MODEL_PATH, "wb") as f:
            pickle.dump(models, f)
        with open(LOCAL_METADATA_PATH, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        print(f"\n  Model saved    -> {LOCAL_MODEL_PATH}")
        print(f"  Metadata saved -> {LOCAL_METADATA_PATH}")
    else:
        s3 = get_s3_client()
        model_key = f"{MODEL_S3_PREFIX}/demand_forecast_lgbm_{MODEL_VERSION}.pkl"
        buf = io.BytesIO()
        pickle.dump(models, buf)
        buf.seek(0)
        s3.put_object(Bucket=BUCKET, Key=model_key, Body=buf.getvalue())

        meta_key = f"{MODEL_S3_PREFIX}/demand_forecast_lgbm_{MODEL_VERSION}_metadata.json"
        s3.put_object(
            Bucket=BUCKET, Key=meta_key,
            Body=json.dumps(metadata, indent=2).encode()
        )
        print(f"\n  Model saved    → s3://{BUCKET}/{model_key}")
        print(f"  Metadata saved → s3://{BUCKET}/{meta_key}")

    print("\nTraining complete.")
    return models, metadata


if __name__ == "__main__":
    train()
