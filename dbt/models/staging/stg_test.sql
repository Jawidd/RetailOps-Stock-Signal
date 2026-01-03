
-- COLUMNS= id,date,store_nbr,item_nbr,onpromotion
-- column to predict: unit_sales


SELECT
    cast(id as bigint) as id,
    cast(date as date) as date,
    cast(store_nbr as int) as store_nbr,
    cast(item_nbr as int) as item_nbr,


    coalesce(   cast(onpromotion as BOOLEAN)    ,FALSE) as onpromotion,
    
    extract(year from cast(date as date)) as year,
    extract(month from cast(date as date)) as month,
    extract(day from cast(date as date)) as day,
    extract(dow from cast(date as date)) as day_of_week,

    case 
        when cast(date as date) between '2016-04-16' and '2016-04-30' then true
        else false 
    end as is_earthquake_period,

    case 
        when EXTRACT(DAY from cast(date as date)) = 15 then true
        WHEN CAST(date AS DATE) = (DATE_TRUNC('month', CAST(date AS DATE)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN TRUE
        else false 
    end as is_wage_day

FROM {{ source('raw', 'test') }}

where 
    date is not null
    and store_nbr is not null
    and item_nbr is not null

