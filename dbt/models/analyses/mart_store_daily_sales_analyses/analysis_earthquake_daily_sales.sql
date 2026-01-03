{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}


-- 08 avg sales on earthquake days vs non earthquake days
 earthquake_days_non_earthquake_days_sales as (
  select
    case when earthquake_days_flg = 1 then 'earthquake_days' else 'non_earthquake_days' end as earthquake_days_type,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by earthquake_days_type
)




select 'earthquake_days_non_earthquake_days_sales' as analysis_type, * from earthquake_days_non_earthquake_days_sales

