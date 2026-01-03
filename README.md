# RetailOps Companion

A simple portfolio project where I build a small retail analytics pipeline:
**generate data → load to a database → create clean analytics tables → show a dashboard**.
Later, I will add **forecasting + reorder recommendations**.

---

## Current status
**Not started yet.**  
This README will be updated as I complete each step.

---

## Goal (why I’m building this)
Retail teams need answers like:
- What are sales trends by store/product?
- Where are stockouts happening?
- What should we reorder?

This project will show I can build an end-to-end data workflow (and later add ML).

---

## Planned milestones
### Week 1 (MVP)
- [X] Create a repo structure

- [X] Setup dataset
    - select https://www.kaggle.com/competitions/favorita-grocery-sales-forecasting/data and download(kaggle competitions download -c favorita-grocery-sales-forecasting)
    - Note: It seems this dataset does not include some data which is needed for this project so those data need to be generated. ((Dataset dont have inventory, purchase orders, suppliers and lead times data.))

- [X] add Docker, Docker compose and docker requirements

- [X] Convert data to perquet
    - unzip data (7z to csv)
    - create script convert_to_parquet.py
    - store parquet files in data/bronze/favorita/

- [X]  Build a local warehouse (DuckDB).
    - create script setup_warehouse_duckdb.py
    - Create a schema for raw data and tables(raw.train, raw.items, raw.stores, raw.transactions, raw.oil, raw.holidays_events, raw.test)(each table comes from a Parquet file in data/bronze/favorita/)

- [X] Data Cleaning
    - add sql scripts for data cleaning in warehouse/sql/ for each dataset
    - Add&Execute run_cleaing scripts to create the clean schema and its tables
    - script to display the clean schema tables

- [X] Testing
    - create a test sql query for fact. daily sales of stores
    - create a script to run testSqlQuery for fact . daily sales of stores

- [X] Analysis
    - create analysis script analyze_store_daily_sales.py to analyze fact.store_daily_sales table
    - run the analysis script 
    - <!-- and save the output -->

- [X] PostgreSQL Setup
    - load csvs into postgres
    - create dbt project (dbt initiate)
    - create dbt stage sql queries
    - run stage_sql_queries (dbt run) to create stage tables
    - create dbt daily_sales mart, run all models again
    - connect to metabase



 - [] Dashboard
    - Metabase: set up locally with Docker compose
    - connect to DuckDB warehouse
    - create a simple dashboard page with few charts (screenshots)
    - save screenshots in the repo

- [ ] Build 1 clean analytics table (daily store KPIs)
- [ ] Create 1 simple dashboard page (screenshots)
- [ ] Add a few basic data quality checks

### Next (after MVP)
- [ ] Add dbt models + tests
- [ ] Add orchestration + alerts
- [ ] Add forecasting + reorder recommendations
- [ ] Deploy parts on AWS

---

## Tech (will be confirmed as I build)
- Python
- Postgres (Docker)
- SQL (and later dbt)
- Dashboard tool (Metabase or Power BI)

---

## Repo structure 
data/
src/
warehouse/
dashboards/

---

## How to run
!!
---
