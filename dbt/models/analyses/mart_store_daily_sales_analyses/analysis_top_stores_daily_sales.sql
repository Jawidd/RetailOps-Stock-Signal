{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}

-- 02 top stores by unit sales
with top_stores as (
  select 
    store_nbr,
    city,
    state,
    store_type,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by store_nbr, city, state, store_type
  order by total_units_sold desc limit 10
)


select 'top_stores' as analysis_type, * from top_stores
