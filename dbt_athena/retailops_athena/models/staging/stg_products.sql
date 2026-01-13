{{
    config(
        materialized= 'view',
        tags=['staging','dimension']
    )
}}

with source as (
    select * from {{source('raw_retailops','raw_products')}}
),

cleaned as (
    select
        product_id,
        product_name,
        category,
        cast(unit_cost as decimal(10,2)) as unit_cost,
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(unit_price as decimal(10,2)) - cast(unit_cost as decimal(10,2)) as unit_gross_margin,
        (cast(unit_price as decimal(10,2)) - cast(unit_cost as decimal(10,2)))
            / nullif(cast(unit_price as decimal(10,2)), 0) as unit_margin_pct,

        supplier_id,

        cast(
            case 
                when lower(is_active) in ('true','t','1','yes') then true
                else false
            end as boolean   
            ) as is_active,

    
    from source
)
select * from cleaned