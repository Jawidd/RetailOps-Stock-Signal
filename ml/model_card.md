# Model Card — RetailOps Demand Forecasting

## Problem Statement

**What is being forecast:** Daily unit demand per store × product combination.

**Grain:** One forecast row per `(store_id, product_id, forecast_date, horizon_day)`.

**Horizon:** 7 days ahead (h=1 … h=7). A separate LightGBM model is trained for each horizon day (direct multi-step strategy).

**Business use:** Forecasts feed the reorder recommendation layer (`ml/reorder_recommendations.py`), which computes safety stock and recommended order quantities. This replaces the deterministic trailing-average score in `analytics/stockout_risk.py` with a forward-looking, variance-aware signal.

---

## Training Data

| Property | Value |
|---|---|
| Source tables | `retailops.fct_daily_sales`, `retailops.fct_inventory_snapshots`, `retailops.mart_supplier_performance`, `retailops.dim_products` |
| Date range | ~180 days of synthetic daily data |
| Stores | 20 |
| Active products | ~160 |
| Store-product pairs | ~4,000 |
| Rows after feature engineering | ~500,000+ (varies by history cutoff) |
| Minimum history required | 30 days per store-product pair |

**Sparse history handling:** Store-product combinations with fewer than 30 days of sales history are excluded from training and inference. Below 30 days, lag features (especially `lag_28`) are undefined, and rolling statistics are unreliable. These pairs fall back to the deterministic stockout risk score.

**Zero-sales days:** Days with no sales transactions are not present in `fct_daily_sales` (the table only contains days with at least one sale). Missing dates are not imputed — the lag and rolling features naturally handle gaps because they are computed on the observed time series.

---

## Features

### Demand features (from `fct_daily_sales`)

| Feature | Description |
|---|---|
| `lag_1`, `lag_7`, `lag_14`, `lag_28` | Demand 1/7/14/28 days ago |
| `roll_mean_7/14/28` | Rolling mean over 7/14/28-day window (shifted by 1 to avoid leakage) |
| `roll_std_7/14/28` | Rolling std — captures demand volatility |
| `roll_min_7/14/28`, `roll_max_7/14/28` | Rolling range — captures demand spikes |
| `ewm_mean_7`, `ewm_mean_14` | Exponentially weighted mean (span=7/14) — weights recent observations more heavily than simple rolling mean |
| `demand_trend_14d` | OLS slope of demand over trailing 14 days — sign indicates whether demand is growing or declining |

### Promotion / price features (from `fct_daily_sales`)

| Feature | Description |
|---|---|
| `has_discount` | Binary flag: discount_amount > 0 |
| `discount_pct` | discount_amount / (unit_price × quantity_sold) — normalised discount depth |
| `price_vs_30d_avg` | Today's unit_price / 30-day rolling average — detects price changes |

### Calendar features

| Feature | Description |
|---|---|
| `day_of_week` | 0–6 (Monday=0) |
| `is_weekend` | Binary |
| `month_of_year` | 1–12 |
| `day_of_month` | 1–31 |
| `days_since_period_start` | day_of_month − 1 (proxy for pay-cycle effects) |

### Inventory features (from `fct_inventory_snapshots`)

| Feature | Description |
|---|---|
| `quantity_on_hand` | Current stock level (clipped to 0) |
| `quantity_on_order` | Open purchase orders |
| `reorder_point` | Configured reorder threshold |
| `needs_reorder` | Binary flag from dbt model |
| `stockout_freq_14d` | Fraction of last 14 days where `is_out_of_stock = true` — captures demand suppression from stockouts |
| `days_of_stock_remaining` | quantity_on_hand / roll_mean_7 — forward-looking stock coverage |

### Supplier features (from `mart_supplier_performance` via `dim_products`)

| Feature | Description |
|---|---|
| `avg_actual_lead_time_days` | Mean actual lead time from shipment records |
| `calculated_on_time_rate` | On-time rate derived from actual shipments (not master data) |
| `avg_fill_rate` | Mean fill rate per supplier |

### Cross-store features

| Feature | Description |
|---|---|
| `region_avg_demand_7d` | Average demand for this product across all stores in the same region over 7 days — captures regional demand signals |

---

## Model Architecture

**Algorithm:** LightGBM gradient boosted trees (`LGBMRegressor`)

**Strategy:** Direct multi-step — one model per horizon day (h=1 … 7). Each model predicts demand h days ahead using features computed at the time of forecasting. This avoids error accumulation from recursive forecasting and allows each horizon to learn different feature relationships.

**Objective:** `regression_l1` (MAE loss) — more robust to outliers than MSE, appropriate for demand data with occasional spikes.

**Hyperparameter tuning:** Optuna with TPE sampler, 50 trials, minimising mean WAPE across all walk-forward folds.

Tuned parameters:

| Parameter | Search range |
|---|---|
| `num_leaves` | 20–150 |
| `learning_rate` | 0.01–0.2 (log scale) |
| `min_child_samples` | 5–50 |
| `feature_fraction` | 0.5–1.0 |
| `bagging_fraction` | 0.5–1.0 |
| `reg_alpha`, `reg_lambda` | 1e-4–1.0 (log scale) |

---

## Validation Strategy

**Walk-forward cross-validation with expanding windows.**

A random train/test split is not used. On time-series data, a random split leaks future information into training: a row from day 150 in the training set contains lag features computed from day 149, which may be in the "test" set. This produces optimistic metrics that collapse in production.

Walk-forward CV respects temporal ordering and simulates real deployment conditions — the model is always evaluated on data it has never seen, in the order it would arrive.

| Fold | Train | Validate |
|---|---|---|
| 1 | Days 1–90 | Days 91–97 |
| 2 | Days 1–110 | Days 111–117 |
| 3 | Days 1–130 | Days 131–137 |
| 4 | Days 1–150 | Days 151–157 |

Each validation window is 7 days (one full forecast horizon). The final model is trained on all available data using the best hyperparameters found during tuning.

---

## Evaluation Metrics

| Metric | Formula | Why it matters |
|---|---|---|
| **WAPE** | Σ\|actual−forecast\| / Σactual | Primary metric. Weights errors by volume — a 10-unit error on a 10-unit SKU is catastrophic; the same error on a 1,000-unit SKU is acceptable. MAPE treats both identically and is dominated by low-volume SKUs. |
| **RMSE** | √(mean((actual−forecast)²)) | Penalises large errors quadratically. Useful for catching catastrophic misforecasts that WAPE might dilute. |
| **Bias** | mean(forecast − actual) | Signed error. Positive = systematic over-forecast (leads to excess inventory). Negative = systematic under-forecast (leads to stockouts). A model with good WAPE but large bias is dangerous. |
| **FVA** | WAPE(model) / WAPE(seasonal_naive) | Forecast Value Added. If FVA > 1.0, the model is worse than a naive baseline and should not be deployed. This is the single most important deployment gate. |

---

## Results

*Results are populated after running `python ml/train.py` and `python ml/evaluate.py`. The table below shows the expected structure.*

### Baseline comparison (Fold 4 validation window)

| Model | WAPE | RMSE | Bias | FVA |
|---|---|---|---|---|
| Naive (last value) | — | — | — | — |
| Seasonal naive (same day last week) | — | — | — | 1.00 (reference) |
| 7-day rolling mean | — | — | — | — |
| **LightGBM h=1** | — | — | — | — |
| **LightGBM h=7** | — | — | — | — |

### Walk-forward results by horizon

| Horizon | Fold 1 WAPE | Fold 2 WAPE | Fold 3 WAPE | Fold 4 WAPE | Mean WAPE |
|---|---|---|---|---|---|
| h=1 | — | — | — | — | — |
| h=7 | — | — | — | — | — |

---

## Known Limitations

**Synthetic demand structure.** The data generator uses fixed demand multipliers per store type and product category. There is no real seasonality, no promotional lift, and no external demand signals (weather, events, competitor pricing). The model will learn the day-of-week pattern and discount effects that are baked in, but it cannot generalise to real-world demand complexity.

**No demand forecasting for sparse SKUs.** Store-product combinations with fewer than 30 days of history are excluded. In a real deployment, these would need a separate cold-start strategy (e.g. category-level priors or a global model).

**Lead time is a point estimate, not a distribution.** The reorder recommendation uses `avg_actual_lead_time_days`, which ignores lead time variance. A supplier with a mean of 10 days but high variance is riskier than the formula implies. A Bayesian posterior over the lead-time distribution would produce a more accurate safety stock.

**No external signals.** The model has no access to promotional calendars, weather, macroeconomic indicators, or competitor data. On real retail data, these are often the most important features.

**Static supplier features.** Supplier performance metrics are computed once from the full shipment history. In production, these should be computed on a rolling window to detect supplier degradation.

---

## What Would Change With Real Data

- Replace `avg_actual_lead_time_days` with a Bayesian posterior over the lead-time distribution (log-normal is a good prior for lead times) to produce a probabilistic safety stock with confidence intervals.
- Replace the 7-day rolling mean baseline with a seasonal decomposition (STL) or simple exponential smoothing that respects day-of-week and promotional patterns.
- Add a Temporal Fusion Transformer (TFT) as a secondary model. TFT is architecturally better suited to this problem than LightGBM because it handles variable-length history natively, learns attention weights over time steps (interpretable temporal patterns), and separates static covariates (store type, product category) from time-varying inputs. On synthetic data with fixed multipliers, LightGBM will likely win on metrics because the signal is simple and tabular. On real data with complex temporal dependencies, TFT's inductive biases become an advantage.
- Incorporate `reorder_point` from the dbt model directly into the risk tier logic — flag combinations where `days_of_stock_remaining < expected_lead_time` **and** `quantity_on_hand < reorder_point` as the highest-priority tier.

---

## Deployment

1. `python ml/features.py` — reads Athena, writes `s3://.../ml/features/features.parquet`
2. `python ml/train.py` — trains models, writes `s3://.../ml/models/demand_forecast_lgbm_v1.pkl`
3. `python ml/evaluate.py` — evaluates, writes forecasts to `s3://.../ml/forecasts/dt=YYYY-MM-DD/`
4. `python ml/reorder_recommendations.py` — combines forecasts + inventory + supplier data, writes `s3://.../ml/reorder_recommendations/dt=YYYY-MM-DD/`

In production, steps 1–4 run daily after the dbt pipeline completes, triggered by Step Functions.

---

## Monitoring

| Metric | Alert threshold | Action |
|---|---|---|
| Daily WAPE (h=1) | > 2× baseline WAPE | Retrain model; check for data pipeline issues |
| Bias drift | \|bias\| > 0.5 units/day sustained 7 days | Investigate demand shift; consider retraining |
| Feature distribution shift | KS statistic > 0.2 on lag_7 or roll_mean_7 | Check upstream data pipeline; may indicate demand regime change |
| FVA > 1.0 | Any single day | Fall back to seasonal naive; do not use ML forecasts for reorder decisions |
| Missing store-product pairs | > 5% drop vs previous day | Check fct_daily_sales freshness; may indicate pipeline failure |
