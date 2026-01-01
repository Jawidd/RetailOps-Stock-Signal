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

- [ ] Generate or download a dataset (sales + inventory):
    - I selected https://www.kaggle.com/competitions/favorita-grocery-sales-forecasting/data and downloaded(kaggle competitions download -c favorita-grocery-sales-forecasting)
    - It seems this dataset does not include some data which is needed for this project which I will generate those data.
    - The missing data are inventory, purchase orders, suppliers and lead times(stock delivery time from the point an item was ordered to when arrived in stock and is ready to sell).

- [ ] Load data into a local database (Postgres)
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
