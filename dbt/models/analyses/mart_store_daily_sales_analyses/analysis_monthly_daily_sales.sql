{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}



-- 04 Monthly unit Sales Trend 
 with monthly_sales_trend as (
  select
    extract(month from saledate) as month_number,
    to_char(saledate, 'Mon') as month_name,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by
    extract(month from saledate),
    to_char(saledate, 'Mon')
)



select 'monthly_sales_trend' as analysis_type, * from monthly_sales_trend
