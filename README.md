# RetailOps Data Platform

A retail analytics platform on AWS that I built to learn data engineering. Started with real data to understand the problem, then moved to synthetic data I could control. Built the whole thing: infrastructure, pipelines, transformations, orchestration.

**Stack**: S3, Glue, Athena, Lambda, ECS Fargate, Step Functions, dbt, CloudFormation

## Why This Project

I wanted to build something real - not just follow a tutorial. Something that shows I can design infrastructure, write good SQL, automate pipelines, and think about what matters in production.

## What I Did

### Week 1: Understanding the Domain (Favorita Dataset)

Started with the Kaggle Favorita dataset (grocery sales from Ecuador). Used it to understand retail analytics requirements and prove I could handle real messy data.

**DuckDB phase**:

* Loaded CSVs into DuckDB
* Built cleaning scripts, ran quality checks
* Created fact tables and tested them

**dbt (Postgres) phase**:

* Set up Postgres in Docker
* Built staging models and a daily sales mart
* Connected to Metabase, created dashboards

**Why I moved to synthetic data**:
The Favorita dataset has sales and products, but no inventory tracking, no suppliers, no shipments. So I wrote a Python generator to create realistic data with the operational pieces I needed.

### Week 2: AWS Data Lake

Built the foundation on AWS. Everything as code with CloudFormation.

* S3 data lake bucket (encryption, versioning, lifecycle)
* Glue catalog + Athena workgroup
* IAM roles for least-privilege access
* Idempotent upload script for dimensions + partitioned facts

### Week 3: dbt Transformation Layer

Migrated dbt from Postgres to Athena and built dimensional models.

* Staging models (type casting, null handling)
* Marts (dims + facts + supplier performance)
* Tests for keys, grain, relationships, and business rules

### Week 4: Orchestration with Step Functions

Automated the pipeline end-to-end:

* EventBridge schedule → Step Functions
* Lambda generates daily partitions
* ECS runs dbt (run + test)
* Athena validation query
* SNS notifications

## Current State

* Daily pipeline running successfully
* Data queryable in Athena
* Email alerts on failure

## Setup

Deploy infrastructure:

```bash
cd infrastructure
./deploy-all-cfn-stacks.sh
```

Trigger manually:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"date":"2025-01-18"}'
```
