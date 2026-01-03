{{
  config(
    materialized='table',
    tags=['analysis', 'metabase']
  )
}}

with summary as (
  select 
    count(distinct saledate) as total_days,
    count(distinct store_nbr) as total_stores,
    sum(total_units_sold) as total_unit_sales_amount,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
)

select 'summary' as analysis_type, 
       total_days,
       total_stores,
       total_unit_sales_amount,
       avg_daily_units_sales
from summary
