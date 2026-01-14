{{
    config(materialized='view',tags=['staging', 'fact'])
}}


with source as(
        select * from {{ source('raw_retailops','raw_sales') }}
),
cleaned as(
    select
        sale_id,
        cast(sale_date as date) as sale_date,
        store_id,
        product_id,

        try_cast(nullif(trim(quantity_sold), '') as integer) as quantity_sold,
        try_cast(nullif(trim(unit_price), '') as decimal(10,2)) as unit_price,
        try_cast(nullif(trim(discount_amount), '') as decimal(10,2)) as discount_amount,
        try_cast(nullif(trim(total_amount), '') as decimal(10,2)) as total_amount,
        
        coalesce(
        try_cast(nullif(trim(discount_amount), '') as decimal(10,2)),
        0
        ) /
        nullif(
        coalesce(try_cast(nullif(trim(total_amount), '') as decimal(10,2)), 0)
        + coalesce(try_cast(nullif(trim(discount_amount), '') as decimal(10,2)), 0),
        0
        ) as discount_rate,

        extract(year from cast(sale_date as date)) as sale_year,
        extract(month from cast(sale_date as date)) as sale_month,
        extract(day from cast(sale_date as date)) as sale_day,
        extract(day_of_week from cast(sale_date as date)) as day_of_week,
        format_datetime(cast(sale_date as timestamp), 'EEEE') as day_name,
        case
            when extract(day_of_week from cast(sale_date as date)) in (6, 7) then true
            else false
        end as is_weekend,
        localtimestamp as dbt_loaded_at

    from source
    where sale_date is not null
      and store_id is not null
      and product_id is not null
      and try_cast(quantity_sold as integer) > 0

)
select * from cleaned