{{
    config(materialized='view',tags=['staging', 'dimension'])
}}

with source as(
    select * from {{ source('raw_retailops','raw_suppliers') }}
    ),

cleaned as(
    select
        supplier_id,
        supplier_name,
        country,
        cast(lead_time_days as int) as lead_time_days,
        cast(on_time_rate as decimal(5,3)) as on_time_delivery_rate,
        case
            when cast(lead_time_days as int) <= 7 then 'Fast'
            when cast(lead_time_days as int) <= 14 then 'Standard'
            else 'Slow'
        end as lead_time_category,
        case
            when cast(on_time_rate as decimal(5,3)) >= 0.95 then 'Excellent'
            when cast(on_time_rate as decimal(5,3)) >= 0.85 then 'Good'
            when cast(on_time_rate as decimal(5,3)) >= 0.75 then 'Fair'
            else 'Poor'
        end as on_time_performance_tier,


    from source
)

select * from cleaned