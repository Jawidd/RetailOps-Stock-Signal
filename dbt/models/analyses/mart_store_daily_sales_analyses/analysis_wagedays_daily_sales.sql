{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}

-- 07 avg sales on Wagedays vs non wage days
 wage_days_non_wage_days_sales as (
  select
    case when wage_days_flg = 1 then 'wage_days' else 'non_wage_days' end as wage_days_type,
    sum(total_units_sold) as total_units_sold,
    avg(total_units_sold) as avg_daily_units_sales
  from {{ ref('mart_store_daily_sales') }}
  group by wage_days_type
)

select 'wage_days_non_wage_days_sales' as analysis_type, * from wage_days_non_wage_days_sales



