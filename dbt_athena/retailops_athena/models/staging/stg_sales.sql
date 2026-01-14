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


        cast(quantity_sold as int) as quantity_sold,
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(discount_amount as decimal(10,2)) as discount_amount,
        cast(total_amount as decimal(10,2)) as total_amount,

        cast(discount_amount as decimal(10,2)) / 
            (cast(total_amount as decimal(10,2)) + cast(discount_amount as decimal(10,2))) as discount_rate,

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