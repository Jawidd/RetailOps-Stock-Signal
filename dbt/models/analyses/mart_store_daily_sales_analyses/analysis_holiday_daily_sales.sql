{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}



-- 05 holiday vs non-holiday sales
 holiday_non_holiday_sales as (
  select
  case when holiday_flg = 1 then 'holiday' else 'non-holiday' end as holiday_type,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by holiday_type
)

select 'holiday_non_holiday_sales' as analysis_type, * from holiday_non_holiday_sales



