with date_store_combos as (
    select
        d.date_day,
        s.store_id
    from {{ ref('dim_date') }} d
    cross join {{ ref('stg_stores') }} s
),

actual_sales as (
    select distinct
        sale_date,
        store_id
    from {{ ref('fct_daily_sales') }}
)

select
    c.date_day,
    c.store_id
from date_store_combos c
left join actual_sales a
  on c.date_day = a.sale_date
 and c.store_id = a.store_id
where a.store_id is null
