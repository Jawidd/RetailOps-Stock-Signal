{{
  config(
    materialized='view',
    tags=['analysis', 'metabase']
  )
}}



-- 09 store with most returns
 store_with_most_sale_returns as (
  select
    store_nbr,
    city,
    state,
    store_type,
    sum(returned_units) as returned_units,
    avg(returned_units) as avg_returned_units
  from {{ ref('mart_store_daily_sales') }}
  where total_units_sold < 0
  group by store_nbr, city, state, store_type
  order by total_units_sold asc
  limit 10
)


select 'store_with_most_sale_returns' as analysis_type, * from store_with_most_sale_returns



