# RetailOps Companion

A production-grade retail analytics data platform on AWS, demonstrating modern data engineering practices: data lake architecture, dimensional modeling with dbt, automated orchestration, and ML-ready feature engineering.

---

## Current status
- Documenting Week 2: Data Lake Foundation + Athena Setup

### Notes
- it can be holiday for stores from particular cities or states. a column should be added.


## Goal (why I’m building this)
Retail teams need answers like:
- What are sales trends by store/product?
- Where are stockouts happening?
- What should we reorder?

This project will show I can build an end-to-end data workflow (and later add ML).

---


## Planned milestones / TODO List





###  Week 1: Local analytics pipeline done in repo
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

- [X] DBT setup
    - create dbt project (dbt initiate)
    - create dbt stage sql queries
    - run stage_sql_queries (dbt run) to create stage tables
    - create dbt daily_sales mart, run all models again
    - connect to metabase
    - create dbt analysis for daily_sales mart

- [X] Metabase Dashboards
    - create dashboard from daily_sales analysis
    - save dashboards and charts as pdf and png in /dbt/snapshots
    

### Week 1 (Using R)


1. [X] Docker Setup
    - Create  R environment
    - Install necessary packages (archive, DBI, RPostgres, DataExplorer, etc.)
    - Configure connection to existing PostgreSQL

2. [X] Data availability check
    - connect to database and check each file exists in raw schema
    - check row count for each table and save results to outpu/

3. [X] Data quality check
    - check row number
    - add&join columndata and pg_stat 
    - check pk_duplicate
    - check pk_nullabe

4. [X] Data cleaning 
    - write cleaing functions for holiday_events, items, stores,
     train, test, oil, transactions
    - write alternate method for train_clean function as
      doing cleaning in R is expensive, do the cleaning in postgres
    - Note : - train is sampled(0.05) in R_stag_train 

5. [X] TEST Stage(clean) DATA
    - UNIQUENESS TESTs
    - NOt_null tests
    - Referntial tests
    - Range tests

6. [X] make reports and plots



### Week 2 synthetic data + AWS data lake 
- [X] Create S3 data lake with encryption, versioning and public access block using cloudformation

- [x] create IAM  
    - Data pipeline Role                           
    - Data pipeline Policy                         

- [X] DAta GEneration & Data ingestion to s3
    - generating synthetic data    
    - data upload as dimensions and facts to s3

- [X] GLUE 
    - create Glue database, Glue dimensions and Glue facts
    - create s3 bucket for Athena output
    - create workgroup to force output dir and encryption
    - add iam policies for accessing and writing to s3 output bucket
    - add iam policy to preform queries                      


### Week 3: Cloud dbt / orchestration / ML

- [X] dbt Project Setup
    - add athena adapter to requierments
    - Create new dbt project for Athena
    
- [X] add stq sql query for all raw tables  
    - add tests for raw tables / save test results to output
    - add tests for stg tables
   
- [X] debug,run,test dbt
    - dbt debug / dbt deps / dbt parse 
    - dbt run --select staging         <!-- SUCCESS -->
    - dbt test --select source:raw_retailops 2>&1 | tee raw_source_tests_$(date +%Y%m%d_%H%M%S).txt 
    - dbt test --select staging        <!-- SUCCESS -->
    - deduplicate duplicated shipments_ids ((add deduplication in stg_shipments))

- [] add marts and dims
    - add dim_date and dim_producs







