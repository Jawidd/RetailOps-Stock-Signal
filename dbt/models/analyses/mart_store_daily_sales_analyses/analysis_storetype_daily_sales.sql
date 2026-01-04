{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}

-- 06 Sales by Store Type
with store_type_sales as (
  select
    store_type,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by store_type
)



select 'store_type_sales' as analysis_type, * from store_type_sales
