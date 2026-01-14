{{
    config(materialized='table',tags=['marts', 'dimension'])
}}


with bounds as (
  select
    least(
      (select min(sale_date)     from {{ ref('stg_sales') }}),
      (select min(snapshot_date) from {{ ref('stg_inventory') }}),
      (select min(order_date)    from {{ ref('stg_shipments') }})
    ) as min_date,
    greatest(
      (select max(sale_date)     from {{ ref('stg_sales') }}),
      (select max(snapshot_date) from {{ ref('stg_inventory') }}),
      (select max(order_date)    from {{ ref('stg_shipments') }})
    ) as max_date
),
date_spine as (
  select cast(d as date) as date_day
  from bounds
  cross join unnest(sequence(min_date, max_date, interval '1' day)) as t(d)
)
select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day,
  extract(day_of_week from date_day) as day_of_week,
  extract(quarter from date_day) as quarter,
  extract(week from date_day) as week_of_year,
  format_datetime(cast(date_day as timestamp), 'EEEE') as day_name,
  format_datetime(cast(date_day as timestamp), 'MMMM') as month_name,
  (extract(day_of_week from date_day) in (6, 7)) as is_weekend,
  case
    when extract(month from date_day) in (12, 1, 2) then 'Winter'
    when extract(month from date_day) in (3, 4, 5) then 'Spring'
    when extract(month from date_day) in (6, 7, 8) then 'Summer'
    else 'Fall'
  end as season,
  case when extract(month from date_day) >= 4 then extract(year from date_day)
       else extract(year from date_day) - 1 end as fiscal_year,
  case when extract(month from date_day) >= 4 then extract(month from date_day) - 3
       else extract(month from date_day) + 9 end as fiscal_month
from date_spine;
