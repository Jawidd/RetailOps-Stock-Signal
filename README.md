# RetailOps Data Platform

A retail analytics platform on AWS that I built to learn data engineering. Started with real data to understand the problem, then moved to synthetic data I could control. Built the whole thing: infrastructure, pipelines, transformations, orchestration.

**Stack**: S3, Glue, Athena, Lambda, ECS Fargate, Step Functions, dbt, CloudFormation

## Why This Project

I wanted to build something real - not just follow a tutorial. Something that shows I can design infrastructure, write good SQL, automate pipelines, and think about what actually matters in production.

## What I Did

### Week 1: Understanding the Domain (Favorita Dataset)

Started with the Kaggle Favorita dataset (grocery sales from Ecuador). Used it to understand retail analytics requirements and prove I could handle real messy data.

**DuckDB phase**:

* Loaded CSVs into DuckDB (fast for analytical queries)
* Built cleaning scripts, ran quality checks
* Created fact tables and tested them
* Found DuckDB doesn't integrate well with Metabase

**dbt (Postgres) phase**:

* Set up Postgres in Docker
* Built 6 staging models and a daily sales mart
* Connected to Metabase, created business dashboards
* Validated dimensional modeling approach

![Week 1 Metabase dashboard](output/week1-dbt-metabase/Metabase_Mart_daily_sales_Dash.png)

**R pipeline phase**:

* Data availability and data quality checks
* Statistical profiling with `pg_stats`
* Data cleaning + tests (keys, not-null, relationships, ranges)
* Built daily sales mart with R aggregations
* Generated an HTML report with visualizations

All this work is preserved in `experiments/favorita-r-pipeline/` with full R scripts and outputs.

**Why I moved to synthetic data**:
The Favorita dataset has sales and products, but no inventory tracking, no suppliers, no shipments. Couldn't build a complete retail ops platform without supply chain data. So I wrote a Python generator to create realistic data with all the operational pieces I needed.

### Week 2: AWS Data Lake

Built the foundation on AWS. Everything as code with CloudFormation.

**S3 data lake**:

* Bucket with versioning and encryption (AES256)
* Lifecycle policies (raw transitions to IA after 30 days, staged after 90 days)
* Three zones: `raw/`, `curated/`, `metadata/`
* Public access blocked, objects encrypted

**Glue catalog + Athena**:

* Glue database pointing to S3
* 3 dimensions + 3 facts partitioned by `dt=YYYY-MM-DD`
* Partition projection enabled (range 2024-07-01 to NOW)
* Athena workgroup with enforced output location + SSE_S3

**Data upload**:

* Idempotent upload script with existence checks
* Dimensions overwrite, facts partition by date and skip existing partitions
* S3 object metadata (timestamps, row counts, table type)

Verified everything worked by querying in Athena:

![Athena query results](output/week2-Athena-GLUE-S3/query_row_count_all_tabkes.png)

### Week 3: dbt Transformation Layer

Migrated dbt from Postgres to Athena. Built proper dimensional models.

**Setup**:

* Installed `dbt-athena-community==1.8.2`
* Profiles use IAM role auth

**Staging + marts**:

* 6 staging models (casts, null handling, dedup shipments)
* 6 mart models (dims + facts + supplier performance)
* Tests for keys, grain, relationships, and business rules

### Week 4: Orchestration with Step Functions

Automated the entire pipeline end-to-end.

* EventBridge schedule → Step Functions
* Lambda generates daily partitions
* ECS runs dbt (run + test)
* Athena validation query
* SNS notifications

## Current State

**What works**:

* Daily pipeline running successfully
* Data queryable in Athena
* Email alerts when things break

**What's next**:

* Metabase dashboards for business users
* Demand forecasting model

## Setup

Deploy infrastructure:

```bash
cd infrastructure
./deploy-all-cfn-stacks.sh
```

Upload dbt project:

```bash
cd scripts
./03_upload_dbt_project_to_s3.sh
```

Build and push Lambda image:

```bash
cd lambda_functions/data_generator
./push_image.sh
```

Trigger manually:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"date":"2025-01-18"}'
```

## Repository Structure

```
infrastructure/cfn/
dbt_athena/models/
lambda_functions/
scripts/
experiments/
  └── favorita-r-pipeline/
```

Built this to learn by doing. Made mistakes, fixed them, learned what matters.
