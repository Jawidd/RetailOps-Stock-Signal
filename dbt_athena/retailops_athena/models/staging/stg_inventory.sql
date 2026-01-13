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

        cast(quantity_on_hand as int) as quantity_on_hand,
        cast(quantity_on_order as int) as quantity_on_order,
        cast(reorder_point as int) as reorder_point,
        cast(last_restock_date as date) as last_restock_date,
        

        cast(quantity_on_hand as int) + cast(quantity_on_order as int) as total_available_quantity,
        

        date_diff('day', cast(last_restock_date as date), cast(snapshot_date as date)) as days_since_restock,
        

        case
            when cast(quantity_on_hand as int) = 0 then true
            else false
        end as is_out_of_stock,
        
        case
            when cast(quantity_on_hand as int) <= cast(reorder_point as int) then true
            else false
        end as needs_reorder,
        
        case
            when date_diff('day', cast(last_restock_date as date), cast(snapshot_date as date)) > 60 then true
            else false
        end as is_slow_moving,
        localtimestamp as dbt_loaded_at

    from source
    where snapshot_date is not null
      and store_id is not null
      and product_id is not null
)

select * from cleaned