# RetailOps Data Platform


## Summary

This platform automatically generates and processes retail business data every day — tracking sales, stock levels, and supplier shipments across stores. It cleans and organizes that data into structured reports that answer questions like: which products are selling, which suppliers are reliable, and where inventory is running low. The entire system runs on AWS, starts itself on a schedule, sends an email if anything goes wrong, and costs roughly $10/month to operate.



---

## Technical Summary

A serverless retail analytics platform on AWS, fully automated and deployed as infrastructure-as-code across 9 CloudFormation stacks. A containerized Lambda function generates daily synthetic retail data (sales, inventory, shipments) and uploads it to a partitioned S3 data lake. AWS Step Functions orchestrates the end-to-end pipeline: Lambda → ECS Fargate (dbt) → Athena validation → SNS notification. dbt transforms raw CSV data into Parquet-backed dimensional models (Kimball-style) in Athena, with 53 automated data quality tests enforcing schema contracts, referential integrity, and business rules. CloudWatch monitors pipeline health with a failure alarm wired to SNS email alerts.

**Stack:** AWS Lambda · ECS Fargate · Step Functions · S3 · Glue Data Catalog · Athena · dbt-athena · CloudFormation · CloudWatch · SNS · ECR

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
```

### Service Rationale

| Service | Role | Why |
|---|---|---|
| **Lambda** | Daily data generation | Stateless, event-driven, no infrastructure to manage |
| **S3** | Data lake (raw + curated zones) | Durable, cheap, native to Athena/Glue |
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

---

## Key Features

- **End-to-end orchestration** via Step Functions with per-state error catching and SNS failure routing
- **Infrastructure as code** — all AWS resources defined across 9 CloudFormation stacks with cross-stack exports
- **Containerized dbt execution** on ECS Fargate with runtime profile injection (no credentials in image)
- **Partition projection** on all fact tables — date-filtered Athena queries resolve instantly without crawlers
- **Idempotent data uploads** — upload script checks S3 object existence before writing; pipeline is safe to re-run
- **Least-privilege IAM** — separate roles for pipeline, ECS task execution, ECS task, Step Functions, and EventBridge
- **Lifecycle policies** — raw data transitions to S3 Standard-IA after 30 days; staged after 90 days
- **Observability** — CloudWatch dashboard tracking Step Functions execution metrics and dbt error log queries
- **Failure alerting** — CloudWatch alarm on `ExecutionsFailed ≥ 1` publishes to SNS failure topic within 5 minutes

---

## Repository Structure

```
infrastructure/
├── cfn/                        # 9 CloudFormation stacks (deploy in order)
│   ├── retops-s3datalake.yaml          # S3 data lake with encryption + lifecycle
│   ├── retops-athena.yaml              # Glue catalog, tables, Athena workgroup
│   ├── retops-iam.yaml                 # Pipeline IAM role (least privilege)
│   ├── retops-ecr-data-generator.yaml  # ECR repo for Lambda image
│   ├── retops-ecr-ml.yaml              # ECR repo for ML image
│   ├── retops-lambda-data-generator.yaml # Lambda function definition
│   ├── retops-ecs-dbt.yaml             # ECS cluster + Fargate task definition
│   ├── retops-step-functions.yaml      # State machine + EventBridge schedule + SNS
│   └── retops-cloudwatch.yaml          # Dashboard + failure alarm
├── docker/
│   └── dbt_athena/             # Dockerfile for dbt-athena container image
├── lambda_functions/
│   └── data_generator/         # Lambda handler (app.py) + data generator (generator.py)
└── deploy-all-cfn-stacks.sh    # Ordered stack deployment script

dbt_athena/
└── retailops_athena/
    └── models/
        ├── staging/            # 6 staging views + source tests
        └── marts/              # 5 mart models + quality tests

scripts/
├── 02_upload_raw_data_to_s3.py         # Idempotent dimension + fact upload to S3
├── 03_upload_dbt_project_to_s3.sh      # Zips and uploads dbt project to S3 for ECS
└── show_schema_tables.py               # Utility: list Athena tables and schemas

analytics/
├── stockout_risk.py            # Stockout risk scoring model (queries Athena mart tables)
├── stockout_risk_output.csv    # Generated: ranked store × product risk table
└── supplier_quadrants.csv      # Generated: supplier 2×2 reliability segmentation

data/synthetic/                 # Local reference copies of dimension CSVs
experiments/                    # Exploratory work (Favorita dataset, R pipeline, Postgres dbt)
output/                         # Screenshots and query results from validation runs
_docs/diagrams/                 # Architecture diagrams
```

---

## Deployment

### Prerequisites

- AWS CLI configured with an IAM user that has CloudFormation, S3, ECR, Lambda, ECS, Step Functions, and IAM permissions
- Docker (for building and pushing container images)
- Region: `eu-west-2` (configurable in `deploy-all-cfn-stacks.sh`)

### 1. Deploy all infrastructure stacks

```bash
cd infrastructure
./deploy-all-cfn-stacks.sh
```

Stacks are deployed in dependency order. Cross-stack exports are used to wire resources together — no manual ARN substitution required.

### 2. Upload raw dimension data to S3

```bash
cd scripts
python 02_upload_raw_data_to_s3.py
```

Uploads `products.csv`, `stores.csv`, `suppliers.csv` to the raw zone. Skips existing objects.

### 3. Upload the dbt project bundle

```bash
cd scripts
./03_upload_dbt_project_to_s3.sh
```

Zips `dbt_athena/` and uploads to `s3://<data-lake-bucket>/metadata/dbt/dbt_athena.zip`. ECS pulls this at runtime.

### 4. Build and push the Lambda container image

```bash
cd infrastructure/lambda_functions/data_generator
./push_image.sh
```

Builds with `--platform linux/amd64` for Lambda compatibility and pushes to ECR.

### 5. Build and push the dbt container image

```bash
cd infrastructure/docker/dbt_athena
./push_image_ecr_dbt_athena.sh
```

---

## Running the Pipeline

### Automatic

EventBridge triggers the state machine daily at **06:00 UTC**. The schedule is currently set to `DISABLED` in the CloudFormation template to avoid unintended charges — set `State: ENABLED` in `retops-step-functions.yaml` and redeploy to activate.

### Manual execution

```bash
aws stepfunctions start-execution \
  --state-machine-arn <StateMachineArn> \
  --input '{"time": "2025-01-18T06:00:00Z"}'
```

The `time` field is parsed by the `ExtractDate` state to derive the target date. The pipeline then generates data for that date, runs dbt, validates with Athena, and sends an SNS notification.

### Expected outputs

| Stage | Output |
|---|---|
| Lambda | 3 CSV files written to `raw/` partitioned by date; returns row counts |
| dbt run | 11 models materialized (6 views + 5 Parquet tables) |
| dbt test | 53 tests executed; failures surface in CloudWatch logs and fail the ECS task |
| Athena validation | Query result written to `s3://<athena-results-bucket>/quality-checks/` |
| SNS | Email to subscribed address with date and status |

---

## Monitoring and Failure Handling

### Logging

All ECS container output (dbt stdout/stderr) streams to CloudWatch Logs at `/ecs/retailops/dev/dbt` with 14-day retention. The CloudWatch dashboard includes a log insights widget that surfaces any line matching `ERROR` or `FAIL` from the most recent dbt runs.

### Alerting

Two SNS topics handle pipeline outcomes:

- `retailops-pipeline-success` — published on clean execution
- `retailops-pipeline-failure` — published on any caught error, with the error message and state included in the notification body

A CloudWatch alarm on `AWS/States ExecutionsFailed ≥ 1` (5-minute period) provides an independent failure signal that also publishes to the failure topic.

### Error handling in Step Functions

Every task state (`GenerateData`, `RunDbt`, `AthenaValidation`) has a `Catch` block on `States.ALL` that routes to `NotifyFailure`, which publishes the error context to SNS before transitioning to a terminal `Fail` state. This ensures no silent failures — every execution ends with an observable outcome.

---

## Design Decisions

**Step Functions over Airflow**
Step Functions provides native `.sync` integrations with Lambda, ECS, and Athena — no polling logic to write. The visual execution graph gives an immediate audit trail. At this scale, it costs ~$0.50/month vs. the operational overhead of running and maintaining an Airflow cluster.

**Athena over a data warehouse**
Athena is serverless and billed per query scanned. For a daily batch workload with moderate query volume, this costs ~$2/month vs. ~$180/month for the smallest Redshift cluster. Partition projection on date-partitioned fact tables keeps query performance fast without manual partition management.

**ECS Fargate for dbt**
Lambda's 15-minute execution limit is insufficient for a full dbt run + test cycle across 11 models. ECS Fargate provides an unconstrained execution environment with full CloudWatch log streaming, configurable CPU/memory (1 vCPU / 2 GB), and no persistent infrastructure to manage between runs.

**Partition projection over Glue crawlers**
Partition projection is configured directly in the Glue table definition with a date range and format template. New partitions are immediately queryable without running a crawler, eliminating scheduling complexity and crawler costs.

**Synthetic data**
The platform requires sales, inventory, and shipment data with consistent foreign keys across all three domains. No public dataset provides this combination. A Python generator produces realistic correlated data with controllable volume and date ranges, enabling deterministic testing of the full pipeline.

---

## Analytical Layer — Stockout Risk Scoring

### Business Question

Which store × product combinations are most likely to stock out before the next replenishment arrives, given current inventory levels, recent demand velocity, and supplier lead time?

### Deliverable

`analytics/stockout_risk.py` — queries the three Athena mart tables built by this pipeline and produces a ranked table of stockout risk, plus a supplier reliability segmentation.

```bash
pip install boto3 pandas numpy python-dotenv
python analytics/stockout_risk.py
```

Outputs:
- `analytics/stockout_risk_output.csv` — every store × product combination ranked by risk score
- `analytics/supplier_quadrants.csv` — supplier segmentation by on-time rate vs fill rate

### Formula

```
days_of_stock_remaining  = max(quantity_on_hand, 0) / avg_daily_demand
expected_replenishment   = avg_actual_lead_time_days / calculated_on_time_rate
risk_score               = expected_replenishment - days_of_stock_remaining
```

A **positive score** means the stock is expected to run out before the next shipment arrives. A **negative score** means current stock will outlast the replenishment window.

**Why each term:**
- `max(quantity_on_hand, 0)` — the generator occasionally produces negative on-hand values (a data quality artefact). Clipping to zero treats them as already stocked-out rather than inflating days-of-stock.
- `avg_daily_demand` over 30 days — smooths day-to-day noise and matches the window a procurement team would review.
- `avg_actual_lead_time_days / calculated_on_time_rate` — penalises unreliable suppliers. A supplier with a 10-day lead time and 50 % on-time rate is effectively a 20-day supplier from a planning perspective. This uses `calculated_on_time_rate` from `mart_supplier_performance`, which is derived from actual shipment records, not the master-data field.

### Inputs (Athena mart tables)

| Table | What it provides |
|---|---|
| `retailops.fct_daily_sales` | `avg_daily_demand` — sum of `quantity_sold` over trailing 30 days, divided by 30 |
| `retailops.fct_inventory_snapshots` | `quantity_on_hand`, `reorder_point`, `quantity_on_order` at the latest snapshot date |
| `retailops.mart_supplier_performance` | `avg_actual_lead_time_days`, `calculated_on_time_rate`, `avg_fill_rate` per supplier |
| `retailops.dim_products` | `supplier_id` — joins products to their supplier |

### Limitations

- **Synthetic data**: demand is generated with fixed multipliers, not real seasonality, so the 30-day trailing average is a reasonable but flat proxy.
- **No demand forecasting**: the score uses trailing average demand, not a forward-looking forecast. A demand spike the day after scoring would not be captured.
- **Lead time is an average, not a distribution**: using `avg_actual_lead_time_days` ignores variance. A supplier with a mean of 10 days but high variance is riskier than the score implies.
- **No safety-stock term**: `reorder_point` already encodes a safety buffer in the data model, but the risk score does not explicitly incorporate it — it scores against zero stock, not against the reorder threshold.

### What would change with real data

- Replace average lead time with a Bayesian posterior over the lead-time distribution (e.g. log-normal) to produce a probabilistic stockout date with confidence intervals.
- Replace trailing-average demand with seasonal decomposition or simple exponential smoothing that respects day-of-week and promotional patterns.
- Incorporate `reorder_point` directly into the score: flag combinations where `days_of_stock_remaining < expected_replenishment_days` **and** `quantity_on_hand < reorder_point` as the highest-priority tier.

### Supplier Reliability Segmentation (stretch)

The script also segments all suppliers into a 2×2 quadrant using `calculated_on_time_rate` vs `avg_fill_rate` from `mart_supplier_performance`, split at the median of each dimension.

| Quadrant | On-time rate | Fill rate | Procurement action |
|---|---|---|---|
| Q1: Preferred | High | High | Lock in volume commitments; use for high-velocity SKUs |
| Q2: Reliable but under-delivering | High | Low | Investigate partial shipment root cause; increase order quantities |
| Q3: Slow but complete | Low | High | Add buffer stock; use for low-velocity SKUs with long lead tolerance |
| Q4: Renegotiate or replace | Low | Low | Dual-source immediately; issue performance-improvement notice |

**Q1 suppliers** deliver on schedule and ship complete orders. They are the lowest operational risk in the supply chain and should be prioritised for volume commitments and long-term contracts. In this dataset, their `calculated_on_time_rate` (derived from actual shipment records) is consistent with the master-data `on_time_delivery_rate`, which the 53 dbt tests validate. Procurement action: lock in preferred-supplier status, negotiate volume discounts, and assign them as the primary source for any store × product combination appearing in the top 25 of the stockout risk table.

**Q4 suppliers** are doubly penalised in the risk score: their low on-time rate inflates `expected_replenishment_days`, and their low fill rate means even when they do arrive, orders are incomplete. Any store × product combination that depends on a Q4 supplier and carries a positive `risk_score` should be treated as a priority escalation. Procurement action: dual-source immediately with a Q1 supplier, issue a formal performance-improvement notice, and set a 90-day review gate. A large negative `on_time_rate_variance` in `mart_supplier_performance` (calculated rate much lower than the master-data rate) is an additional red flag that the supplier may be misreporting performance in their own systems.

---

## Future Improvements

- **CI/CD pipeline** — GitHub Actions workflow to lint CloudFormation (cfn-lint), run `dbt compile`, and deploy on merge to main
- **Data quality thresholds** — Athena validation state currently checks row presence; extend to assert minimum row counts and flag anomalous drops
- **dbt source freshness** — add `freshness` blocks to source definitions so stale partitions surface as dbt warnings before mart models run
- **Secrets Manager** — move any remaining environment-level config to AWS Secrets Manager with automatic rotation
- **Parquet partition pruning** — partition mart tables by date to enable predicate pushdown on time-range queries in the curated zone

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
