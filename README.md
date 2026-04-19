# RetailOps Stock Signal

## Summary

A serverless retail analytics platform on AWS that generates daily synthetic retail data, transforms it through a dbt pipeline, scores stockout risk across every store and product, and forecasts future demand using a machine learning model — all fully automated, infrastructure-as-code, and running for roughly $10/month.

---

## Technical Summary

A serverless retail analytics platform on AWS, fully automated and deployed as infrastructure-as-code across 9 CloudFormation stacks. A containerized Lambda function generates daily synthetic retail data (sales, inventory, shipments) and uploads it to a partitioned S3 data lake. AWS Step Functions orchestrates the end-to-end pipeline: Lambda → ECS Fargate (dbt) → Athena validation → SNS notification. dbt transforms raw CSV data into Parquet-backed dimensional models (Kimball-style) in Athena, with 53 automated data quality tests enforcing schema contracts, referential integrity, and business rules. A LightGBM demand forecasting model trained with walk-forward cross-validation and Optuna hyperparameter tuning generates 7-day-ahead predictions per store × product, feeding a statistical reorder recommendation engine. CloudWatch monitors pipeline health with a failure alarm wired to SNS email alerts.

**Stack:** AWS Lambda · ECS Fargate · Step Functions · S3 · Glue Data Catalog · Athena · dbt-athena · CloudFormation · CloudWatch · SNS · ECR · LightGBM · Optuna · SHAP

---

## Architecture Overview

![RetailOps Daily Pipeline Architecture](_docs/diagrams/Diagram1.svg)

```
EventBridge (06:00 UTC daily)
  └─► Step Functions
        ├─► Lambda          — generates ~55K rows of synthetic retail data for the day
        ├─► ECS Fargate     — runs dbt: staging views → Parquet mart tables → 53 tests
        ├─► Athena          — validates freshness: row count over trailing 7 days
        └─► SNS             — emails success or failure with execution context

ML layer (runs after dbt succeeds):
  python ml/features.py               — reads Athena → feature matrix → S3 Parquet
  python ml/train.py                  — walk-forward CV + Optuna → LightGBM → S3
  python ml/evaluate.py               — metrics + SHAP + forecasts → S3
  python ml/reorder_recommendations.py — forecasts + safety stock → recommendations → S3
```

### Service Rationale

| Service | Role | Why |
|---|---|---|
| **Lambda** | Daily data generation | Stateless, event-driven, no infrastructure to manage |
| **S3** | Data lake (raw + curated + ml zones) | Durable, cheap, native to Athena/Glue |
| **Glue Data Catalog** | Schema registry | Partition projection eliminates crawler runs; Athena reads it natively |
| **Athena** | Query engine + validation | Serverless, pay-per-query, no cluster to maintain |
| **ECS Fargate** | dbt execution environment | Exceeds Lambda's 15-min timeout; full stdout/stderr in CloudWatch |
| **Step Functions** | Pipeline orchestration | Native `.sync` integrations with Lambda, ECS, and Athena; visual audit trail |
| **CloudFormation** | Infrastructure provisioning | All resources version-controlled and reproducible |
| **CloudWatch** | Logs + metrics + alarms | Centralized observability without additional tooling |
| **SNS** | Alerting | Decoupled notification layer; email subscription on failure topic |

---

## Data Flow

```
1. GENERATE   Lambda reads dimension CSVs from S3 (products, stores, suppliers)
              and generates daily fact data:
                - raw/sales/dt=YYYY-MM-DD/sales.csv         (~50K rows)
                - raw/inventory/dt=YYYY-MM-DD/inventory.csv (~4K rows)
                - raw/shipments/dt=YYYY-MM-DD/shipments.csv (~200 rows)

2. CATALOG    Glue Data Catalog exposes all raw tables to Athena via
              partition projection (no crawlers, instant partition discovery)

3. TRANSFORM  ECS Fargate pulls the dbt project from S3 (metadata/dbt/dbt_athena.zip),
              builds profiles.yml at runtime from environment variables, then runs:
                dbt run   → staging views (type casting, null handling, deduplication)
                          → mart Parquet tables written to curated/ zone
                dbt test  → 53 tests across all models

4. VALIDATE   Step Functions invokes Athena directly to confirm the latest partition
              is present and row counts are non-zero over the trailing 7 days

5. NOTIFY     SNS publishes success or failure with the execution date and error context

6. SCORE      analytics/stockout_risk.py queries Athena mart tables and produces a
              ranked stockout risk table for every store × product combination

7. FORECAST   ml/ pipeline reads Athena mart tables, engineers features, trains a
              LightGBM model with walk-forward CV, generates 7-day demand forecasts,
              and produces statistical reorder recommendations with safety stock
```

### S3 Zone Layout

```
s3://retailops-data-lake-eu-west-2/
├── raw/
│   ├── products/products.csv
│   ├── stores/stores.csv
│   ├── suppliers/suppliers.csv
│   ├── sales/dt=YYYY-MM-DD/sales.csv
│   ├── inventory/dt=YYYY-MM-DD/inventory.csv
│   └── shipments/dt=YYYY-MM-DD/shipments.csv
├── curated/                          ← dbt mart output (Parquet/Snappy)
│   ├── dim_date/
│   ├── dim_products/
│   ├── dim_stores/
│   ├── fct_daily_sales/
│   ├── fct_inventory_snapshots/
│   └── mart_supplier_performance/
├── ml/                               ← ML pipeline output
│   ├── features/features.parquet
│   ├── models/demand_forecast_lgbm_v1.pkl
│   ├── models/demand_forecast_lgbm_v1_metadata.json
│   ├── forecasts/dt=YYYY-MM-DD/forecasts.parquet
│   ├── reorder_recommendations/dt=YYYY-MM-DD/recommendations.parquet
│   └── evaluation/eval_YYYY-MM-DD.json
└── metadata/
    └── dbt/dbt_athena.zip            ← dbt project bundle for ECS
```

---

## dbt Models

### Staging (materialized as views)

| Model | Key Transformations |
|---|---|
| `stg_products` | Type casting, null handling on `supplier_id` |
| `stg_stores` | Attribute cleaning, region normalization |
| `stg_suppliers` | Lead time and on-time rate parsing |
| `stg_sales` | Numeric casting, discount null coalescing |
| `stg_inventory` | Negative quantity handling |
| `stg_shipments` | Deduplication of 29 duplicate shipment IDs, date casting |

### Marts (materialized as Parquet tables in `curated/`)

| Model | Grain | Description |
|---|---|---|
| `dim_date` | Day | Calendar dimension with fiscal attributes, weekend flags, day-of-week names |
| `dim_products` | Product | Product attributes denormalized with supplier details |
| `dim_stores` | Store | Store master with geography and format |
| `fct_daily_sales` | Store × Product × Day | Quantity, pricing, discounts, profit |
| `fct_inventory_snapshots` | Store × Product × Day | On-hand, on-order, reorder points, stockout flags |
| `mart_supplier_performance` | Supplier | On-time rate, fill rate, average lead time, late shipment count |

### Data Quality Tests (53 total)

- Primary key uniqueness on all dimensions and fact grain combinations
- Foreign key relationships validated across all fact-to-dimension joins
- Business rule enforcement: quantities ≥ 0, prices > 0, rates between 0 and 1
- Source-level NOT NULL constraints on all primary and foreign keys
- Data coverage test in interval: (2025-07-01 → 2026-02-10)

---

## Analytical Layer — Stockout Risk Scoring

`analytics/stockout_risk.py` queries the Athena mart tables and produces a ranked table of stockout risk for every store × product combination, plus a supplier reliability segmentation.

```bash
python analytics/stockout_risk.py
```

**Formula:**
```
days_of_stock_remaining  = max(quantity_on_hand, 0) / avg_daily_demand
expected_replenishment   = avg_actual_lead_time_days / calculated_on_time_rate
risk_score               = expected_replenishment - days_of_stock_remaining
```

A positive score means stockout is expected before the next shipment arrives. The supplier segmentation classifies all suppliers into a 2×2 quadrant (on-time rate vs fill rate) with written procurement actions for each quadrant.

Outputs: `analytics/stockout_risk_output.csv`, `analytics/supplier_quadrants.csv`

---

## ML Layer — Demand Forecasting & Reorder Intelligence

The ML layer is a direct upgrade over the deterministic stockout risk score. Where the risk score looks backward (trailing 30-day average demand), the ML layer looks forward (7-day-ahead forecast). Where the risk score ignores demand variance, the ML layer incorporates it via a statistical safety stock formula.

### What it does

1. **Feature engineering** (`ml/features.py`) — reads all six Athena mart tables and builds a feature matrix with lag features, rolling statistics, exponentially weighted means, demand trend, inventory state, promotion flags, calendar features, supplier reliability, and cross-store regional demand signals.

2. **Model training** (`ml/train.py`) — trains a LightGBM model for each of the 7 forecast horizons using walk-forward cross-validation and Optuna hyperparameter tuning. Evaluates against three baselines (naive, seasonal naive, 7-day rolling mean) and reports Forecast Value Added (FVA).

3. **Evaluation** (`ml/evaluate.py`) — full walk-forward evaluation across all folds, error breakdown by product category / store type / demand level / day of week / promotion flag, SHAP feature importance analysis, and forecast generation for the latest date.

4. **Reorder recommendations** (`ml/reorder_recommendations.py`) — combines ML forecasts with supplier lead times and a statistical safety stock formula to produce a specific recommended order quantity and risk tier (Critical / High / Medium / Low) for every store × product combination.

### Run order

```bash
python ml/features.py
python ml/train.py
python ml/evaluate.py
python ml/reorder_recommendations.py
```

### Key design decisions

**Walk-forward cross-validation, not a random split.** A random split on time-series data leaks future information into training. Walk-forward CV respects temporal ordering and produces metrics that reflect real deployment conditions.

**Direct multi-step forecasting.** One LightGBM model per horizon day (h=1…7). This avoids error accumulation from recursive forecasting and allows each horizon to learn different feature relationships.

**WAPE as primary metric, not RMSE.** Weighted Absolute Percentage Error weights errors by volume — a 10-unit error on a 10-unit SKU is catastrophic; the same error on a 1,000-unit SKU is acceptable. RMSE treats both identically.

**Forecast Value Added (FVA) as deployment gate.** FVA = WAPE(model) / WAPE(seasonal_naive). If FVA ≥ 1.0, the model is worse than a naive baseline and is not deployed.

**Safety stock formula with demand variance:**
```
safety_stock             = z × σ_demand × √(lead_time_days)
reorder_point            = forecast_demand_over_lead_time + safety_stock
recommended_order_qty    = max(0, reorder_point − quantity_on_hand − quantity_on_order)
```
Where z = 1.65 (95% service level). The √L term accounts for demand uncertainty compounding over the lead time.

### Outputs written to S3

| File | Contents |
|---|---|
| `ml/features/features.parquet` | Full feature matrix (~500K rows) |
| `ml/models/demand_forecast_lgbm_v1.pkl` | Trained models for h=1…7 |
| `ml/models/demand_forecast_lgbm_v1_metadata.json` | Hyperparameters, metrics, feature list |
| `ml/forecasts/dt=YYYY-MM-DD/forecasts.parquet` | 7-day-ahead predictions per store × product |
| `ml/reorder_recommendations/dt=YYYY-MM-DD/recommendations.parquet` | Order quantities + risk tiers |
| `ml/evaluation/eval_YYYY-MM-DD.json` | Full evaluation report including SHAP summary |

### Model card

Full documentation — training data, feature descriptions, validation strategy, results table, error analysis findings, limitations, deployment instructions, and monitoring metrics — is in `ml/model_card.md`.

---

## Repository Structure

```
infrastructure/
├── cfn/                        # 9 CloudFormation stacks (deploy in order)
│   ├── retops-s3datalake.yaml
│   ├── retops-athena.yaml
│   ├── retops-iam.yaml
│   ├── retops-ecr-data-generator.yaml
│   ├── retops-ecr-ml.yaml
│   ├── retops-lambda-data-generator.yaml
│   ├── retops-ecs-dbt.yaml
│   ├── retops-step-functions.yaml
│   └── retops-cloudwatch.yaml
├── docker/dbt_athena/          # Dockerfile for dbt-athena container image
├── lambda_functions/data_generator/
└── deploy-all-cfn-stacks.sh

dbt_athena/retailops_athena/models/
├── staging/                    # 6 staging views + source tests
└── marts/                      # 5 mart models + quality tests

analytics/
├── stockout_risk.py            # Deterministic stockout risk score (Athena → CSV)
├── stockout_risk_output.csv
└── supplier_quadrants.csv

ml/
├── athena_client.py            # Shared Athena query helper
├── features.py                 # Feature engineering pipeline
├── train.py                    # Walk-forward CV + Optuna + LightGBM training
├── evaluate.py                 # Evaluation + SHAP + forecast generation
├── reorder_recommendations.py  # Safety stock + reorder quantities
├── model_card.md               # Full model documentation
└── notebooks/
    ├── 01_eda.ipynb
    ├── 03_baseline_models.ipynb
    └── 04_model_development.ipynb

scripts/
├── 02_upload_raw_data_to_s3.py
├── 03_upload_dbt_project_to_s3.sh
└── show_schema_tables.py

data/synthetic/                 # Local reference copies of dimension CSVs
```

---

## Deployment

### Prerequisites

- AWS CLI configured with CloudFormation, S3, ECR, Lambda, ECS, Step Functions, and IAM permissions
- Docker (for building and pushing container images)
- Python 3.11+ with dependencies from `requirements.txt`
- Region: `eu-west-2`

### Infrastructure

```bash
cd infrastructure
./deploy-all-cfn-stacks.sh
```

### Data pipeline

```bash
python scripts/02_upload_raw_data_to_s3.py
./scripts/03_upload_dbt_project_to_s3.sh
cd infrastructure/lambda_functions/data_generator && ./push_image.sh
cd infrastructure/docker/dbt_athena && ./push_image_ecr_dbt_athena.sh
```

### ML pipeline

```bash
pip install -r requirements.txt
python ml/features.py
python ml/train.py
python ml/evaluate.py
python ml/reorder_recommendations.py
```

### Run the full pipeline manually

```bash
aws stepfunctions start-execution \
  --state-machine-arn <StateMachineArn> \
  --input '{"time": "2025-01-18T06:00:00Z"}'
```

---

## Monitoring and Failure Handling

All ECS container output streams to CloudWatch Logs at `/ecs/retailops/dev/dbt` with 14-day retention. A CloudWatch alarm on `ExecutionsFailed ≥ 1` publishes to the SNS failure topic within 5 minutes. Every Step Functions task state has a `Catch` block on `States.ALL` that routes to `NotifyFailure` before terminating — no silent failures.

ML model health is tracked via WAPE drift, bias drift, and Forecast Value Added. If FVA rises above 1.0 on any scoring run, the model falls back to seasonal naive forecasts.

---

## Design Decisions

**Step Functions over Airflow** — native `.sync` integrations, visual audit trail, ~$0.50/month vs operational overhead of a managed Airflow cluster.

**Athena over Redshift** — serverless, pay-per-query, ~$2/month vs ~$180/month for the smallest Redshift cluster. Partition projection keeps query performance fast without manual partition management.

**ECS Fargate for dbt** — Lambda's 15-minute limit is insufficient for a full dbt run + test cycle. Fargate provides unconstrained execution with full CloudWatch log streaming.

**LightGBM over neural models** — handles mixed feature types natively, fast enough to retrain daily on ~4,000 store-product combinations, feature importance is interpretable without SHAP overhead, and outperforms neural approaches on tabular data at this scale.

**Direct multi-step over recursive forecasting** — one model per horizon avoids error accumulation and lets each horizon learn its own feature relationships.

**Walk-forward CV over random split** — the only defensible validation strategy for time-series data. A random split leaks future information into training.

---

## Cost (approximate monthly)

| Service | Cost |
|---|---|
| ECS Fargate | ~$5.00 |
| Athena | ~$2.00 |
| CloudWatch | ~$1.00 |
| Lambda | ~$0.50 |
| Step Functions | ~$0.50 |
| S3 | ~$0.50 |
| SNS | ~$0.01 |
| **Total** | **~$9.50** |
