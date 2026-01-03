{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}

--  01 summary
with summary as (
  select 
    count(distinct saledate) as total_days,
    count(distinct store_nbr) as total_stores,
    count(distinct city) as total_cities,
    count(distinct state) as total_states,
    count(distinct store_type) as total_store_types,
    count(distinct store_cluster) as total_store_clusters,
    sum(total_units_sold) as total_unit_sales_amount,
    sum(returned_units) as total_returned_units_amount,
    sum(sold_units) as total_sold_units_amount,
    count(distinct saledate) filter (where is_wage_day) as total_wage_days,
    count(distinct saledate) filter (where is_wage_day and is_earthquake_period) as total_earthquake_wage_days,
    count(distinct saledate) filter (where is_wage_day and is_holiday) as total_holiday_wage_days,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
)


select 'summary' as analysis_type, * from summary
