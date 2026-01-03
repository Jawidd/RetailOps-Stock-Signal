{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}



-- 03 weekend vs weekday sales
 with weekend_weekday_summary as(
  select 
    case when extract(dow from saledate) in (0,6) then 'weekend' else 'weekday' end as day_type,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by day_type

)


-- -- 04 Monthly unit Sales Trend 
--  monthly_sales_trend as (
--   select
--     date_trunc('month', saledate) as month,
--     sum(total_units_sold) as total_units_sold,
--     avg(total_units_sold) as avg_daily_units_sales
--   from {{ ref('mart_store_daily_sales') }}
--   group by month
--   order by month
-- ),

-- -- 05 holiday vs non-holiday sales
--  holiday_non_holiday_sales as (
--   select
--   case when holiday_flg = 1 then 'holiday' else 'non-holiday' end as holiday_type,
--     sum(total_units_sold) as total_units_sold,
--     avg(total_units_sold) as avg_daily_units_sales
--   from {{ ref('mart_store_daily_sales') }}
--   group by holiday_type
-- ),

-- -- 06 sales by store type
--  store_type_sales as (
--   select
--     store_type,
--     sum(total_units_sold) as total_units_sold,
--     avg(total_units_sold) as avg_daily_units_sales
--   from {{ ref('mart_store_daily_sales') }}
--   group by store_type
--   order by total_units_sold desc
-- ),

-- -- 07 avg sales on Wagedays vs non wage days
--  wage_days_non_wage_days_sales as (
--   select
--     case when wage_days_flg = 1 then 'wage_days' else 'non_wage_days' end as wage_days_type,
--     sum(total_units_sold) as total_units_sold,
--     avg(total_units_sold) as avg_daily_units_sales
--   from {{ ref('mart_store_daily_sales') }}
--   group by wage_days_type
-- ),

-- -- 08 avg sales on earthquake days vs non earthquake days
--  earthquake_days_non_earthquake_days_sales as (
--   select
--     case when earthquake_days_flg = 1 then 'earthquake_days' else 'non_earthquake_days' end as earthquake_days_type,
--     sum(total_units_sold) as total_units_sold,
--     avg(total_units_sold) as avg_daily_units_sales
--   from {{ ref('mart_store_daily_sales') }}
--   group by earthquake_days_type
-- ),


-- -- 09 store with most sale returns
--  store_with_most_sale_returns as (
--   select
--     store_nbr,
--     city,
--     state,
--     store_type,
--     sum(returned_units) as returned_units,
--     avg(returned_units) as avg_returned_units
--   from {{ ref('mart_store_daily_sales') }}
--   where total_units_sold < 0
--   group by store_nbr, city, state, store_type
--   order by total_units_sold asc
--   limit 10
-- )





select 'weekend_weekday_summary' as analysis_type, * from weekend_weekday_summary
-- union all
-- select 'monthly_sales_trend' as analysis_type, * from monthly_sales_trend
-- union all
-- select 'holiday_non_holiday_sales' as analysis_type, * from holiday_non_holiday_sales
-- union all
-- select 'store_type_sales' as analysis_type, * from store_type_sales
-- union all
-- select 'wage_days_non_wage_days_sales' as analysis_type, * from wage_days_non_wage_days_sales
-- union all
-- select 'earthquake_days_non_earthquake_days_sales' as analysis_type, * from earthquake_days_non_earthquake_days_sales
-- union all
-- select 'store_with_most_sale_returns' as analysis_type, * from store_with_most_sale_returns



