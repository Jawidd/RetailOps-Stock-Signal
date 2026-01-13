{{
    config(materialized='view',tags=['staging', 'dimension'])
}}

with source as( 
   select * from {{ source('raw_retailops','raw_stores') }} 
   ),

cleaned as (
    select
        store_id,
        store_name,
        region,
        store_type,
        cast(sq_footage as int) as square_footage,
        cast(opened_date as date) as opened_date,
        case
            when store_type = 'Superstore' then 'Large'
            when store_type = 'Standard' then 'Medium'
            when store_type = 'Express' then 'Small'
            else 'Unknown'
        end as store_size_category,
        date_diff('year', cast(opened_date as date), current_date) as store_age_years,
        current_timestamp as dbt_loaded_at
    from source
) 

select * from cleaned