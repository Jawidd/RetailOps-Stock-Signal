# RetailOps Data Platform

## Overview

* Retail analytics platform on AWS I built to learn data engineering.
* Started with real data, then moved to synthetic data I could control.
* Built infrastructure, pipelines, transformations, and orchestration.

**Stack**: S3, Glue, Athena, Lambda, ECS Fargate, Step Functions, dbt, CloudFormation

## Why This Project

* Build something real (not a tutorial).
* Show end-to-end data engineering skills.

## What I Did (by week)

### Week 1: Understanding the Domain (Favorita Dataset)

* DuckDB phase
* Postgres + dbt phase
* R pipeline phase
* Why I moved to synthetic data

### Week 2: AWS Data Lake

* S3 data lake
* Glue catalog + Athena workgroup
* IAM roles
* Data upload script
* Validation in Athena

### Week 3: dbt Transformation Layer

* dbt-athena setup
* staging models + tests
* marts + tests + analysis queries

### Week 4: Orchestration with Step Functions

* Lambda data generator (container image)
* ECS dbt runner
* Step Functions pipeline + schedule
* Monitoring (CloudWatch + alarm)

## Current State

* What works
* What’s next

## Design Decisions

* Why Athena / ECS / Step Functions / partition projection / synthetic data

## Cost

* Rough monthly estimate

## Setup

* Deploy infra
* Upload dbt project
* Build/push Lambda image
* Manual trigger

## Repo Structure

* infrastructure
* dbt_athena
* lambda_functions
* scripts
* experiments
