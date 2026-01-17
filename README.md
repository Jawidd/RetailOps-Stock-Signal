# RetailOps Data Platform

A retail analytics platform on AWS that I built to learn data engineering. Started with real data to understand the problem, then moved to synthetic data I could control. Built the whole thing: infrastructure, pipelines, transformations, orchestration.

**Stack**: S3, Glue, Athena, Lambda, ECS Fargate, Step Functions, dbt, CloudFormation

## Why This Project

I wanted to build something real - not just follow a tutorial. Something that shows I can design infrastructure, write good SQL, automate pipelines, and think about what actually matters in production.

## What I Did

### Week 1: Understanding the Domain (Favorita Dataset)

Started with the Kaggle Favorita dataset (grocery sales from Ecuador). Used it to understand retail analytics requirements and prove I could handle real messy data.

**DuckDB phase**:

* Loaded CSVs into DuckDB (fast for analytics)
* Built cleaning scripts, ran quality checks
* Created fact tables and tested them

**dbt (Postgres) phase**:

* Set up Postgres in Docker
* Built 6 staging models and a daily sales mart
* Connected to Metabase, created dashboards

![Week 1 Metabase dashboard](output/week1-dbt-metabase/Metabase_Mart_daily_sales_Dash.png)

**R pipeline phase**:

* Data availability and quality checks
* Statistical profiling with `pg_stats`
* Data cleaning across all tables
* Data quality tests (uniqueness, NOT NULL, relationships, ranges)
* Generated analysis and an HTML report

All this work is preserved in `experiments/favorita-r-pipeline/` with full R scripts and outputs.

**Why I moved to synthetic data**:
The Favorita dataset has sales and products, but no inventory tracking, no suppliers, no shipments. Couldn't build a complete retail ops platform without supply chain data. So I wrote a Python generator to create realistic data with all the operational pieces I needed.

### Week 2: AWS Data Lake

Built the foundation on AWS. Everything as code with CloudFormation.

**S3 data lake**:

* Bucket with versioning and encryption (AES256)
* Lifecycle policies for cost control
* Zones: `raw/`, `curated/`, `metadata/`
* Public access blocked

**Glue catalog + Athena**:

* Glue database pointing to S3
* 3 dimensions + 3 partitioned facts
* Partition projection enabled (no crawlers)
* Dedicated Athena workgroup with encrypted results and metrics

**Data upload**:

* Idempotent upload script
* Dimensions overwrite, facts partition by date and skip existing partitions

Verified everything worked by querying in Athena:

![Athena query results](output/week2-Athena-GLUE-S3/query_row_count_all_tabkes.png)

### Week 3: dbt Transformation Layer

Migrated dbt from Postgres to Athena and built dimensional models.

* Installed `dbt-athena-community==1.8.2`
* 6 staging models (casts, null handling, dedup shipments)
* Marts: `dim_date`, `dim_products`, `dim_stores`, `fct_daily_sales`, `fct_inventory_snapshots`, `mart_supplier_performance`
* Tests for keys, grain, relationships, and business rules

### Week 4: Orchestration with Step Functions

Automated the pipeline end-to-end.

* Lambda generates daily raw partitions
* ECS runs dbt (run + test)
* Athena validation query
* SNS notifications
* EventBridge schedule (daily)

## Current State

**What works**:

* Daily pipeline running successfully
* Data queryable in Athena
* Email alerts when things break

**What's next**:

* Dashboards for business users
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

Trigger manually:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"date":"2025-01-18"}'
```
