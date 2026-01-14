{{
    config(materialized='view',tags=['staging', 'fact'])
}}


with source as(
        select * from {{ source('raw_retailops','raw_inventory') }}
),

cleaned as (
    select

        cast(snapshot_date as date) as snapshot_date,
        store_id,
        product_id,

        try_cast(nullif(trim(quantity_on_hand), '') as integer) as quantity_on_hand,
        try_cast(nullif(trim(quantity_on_order), '') as integer) as quantity_on_order,
        try_cast(nullif(trim(reorder_point), '') as integer) as reorder_point,
        try_cast(nullif(nullif(trim(last_restock_date), ''), '.') as date) as last_restock_date,
        

        coalesce(try_cast(nullif(trim(quantity_on_hand), '') as integer), 0)
          + coalesce(try_cast(nullif(trim(quantity_on_order), '') as integer), 0) as total_available_quantity,
        

        date_diff(
          'day',
          try_cast(nullif(nullif(trim(last_restock_date), ''), '.') as date),
          try_cast(nullif(nullif(trim(snapshot_date), ''), '.') as date)
        ) as days_since_restock,

        case when coalesce(try_cast(nullif(trim(quantity_on_hand), '') as integer), 0) = 0 then true else false end as is_out_of_stock,
        case when coalesce(try_cast(nullif(trim(quantity_on_hand), '') as integer), 0)
                  <= coalesce(try_cast(nullif(trim(reorder_point), '') as integer), 0)
             then true else false end as needs_reorder,
        
        case when date_diff(
                  'day',
                  try_cast(nullif(nullif(trim(last_restock_date), ''), '.') as date),
                  try_cast(nullif(nullif(trim(snapshot_date), ''), '.') as date)
             ) > 60 then true else false end as is_slow_moving,
        dt as partition_date,     
        localtimestamp as dbt_loaded_at

    from source
    where snapshot_date is not null
      and store_id is not null
      and product_id is not null
)

select * from cleaned