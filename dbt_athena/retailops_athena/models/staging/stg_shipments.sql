{{
    config(materialized='view',tags=['staging', 'fact'])
}}


with source as(
        select * from {{ source('raw_retailops','raw_shipments') }}
),

cleaned as (
    select
        shipment_id,
        cast(order_date as date) as order_date,
        cast(expected_date as date) as expected_date,
        cast(received_date as date) as received_date,
        store_id,
        product_id,
        supplier_id,
        
        cast(quantity_ordered as int) as quantity_ordered,
        cast(quantity_received as int) as quantity_received,
        

        date_diff('day', cast(order_date as date), cast(received_date as date)) as actual_lead_time_days,
        date_diff('day', cast(order_date as date), cast(expected_date as date)) as expected_lead_time_days,
        date_diff('day', cast(expected_date as date), cast(received_date as date)) as days_late,
        
        cast(quantity_received as int) - cast(quantity_ordered as int) as quantity_variance,
        
        cast(quantity_received as decimal(10,2)) /
            nullif(cast(quantity_ordered as decimal(10,2)), cast(0 as decimal(10,2)))
            as fill_rate,
        

        cast(
        case
            when lower(cast(is_late as varchar)) in ('true', 't', '1', 'yes') then true
            else false
        end as boolean
        ) as is_late,
        
        case
            when cast(received_date as date) > cast(expected_date as date) then true
            else false
        end as is_delayed,
        
        case
            when cast(quantity_received as int) < cast(quantity_ordered as int) then true
            else false
        end as is_partial_shipment,
        localtimestamp as dbt_loaded_at
        
        


    from source
    where shipment_id is not null
      and order_date is not null
),

deduped as (
    select *
    from (
        select
            c.*,
            row_number() over (
                partition by shipment_id
                order by
                    received_date desc nulls last,
                    expected_date desc nulls last,
                    order_date desc nulls last,
                    quantity_received desc nulls last
            ) as rn
        from cleaned c
    ) x
    where rn = 1
)

select
    -- return all columns but rn
    shipment_id,
    order_date,
    expected_date,
    received_date,
    store_id,
    product_id,
    supplier_id,
    quantity_ordered,
    quantity_received,
    actual_lead_time_days,
    expected_lead_time_days,
    days_late,
    quantity_variance,
    fill_rate,
    is_late,
    is_delayed,
    is_partial_shipment,
    dbt_loaded_at
from deduped