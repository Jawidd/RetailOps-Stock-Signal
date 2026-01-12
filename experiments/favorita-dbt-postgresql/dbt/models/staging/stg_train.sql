-- train_COLUMNS= id,date,store_nbr,item_nbr,unit_sales,onpromotion



SELECT
    cast(id as bigint) as id,
    cast(date as date) as date,
    cast(store_nbr as int) as store_nbr,
    cast(item_nbr as int) as item_nbr,
    cast(unit_sales as double precision) as unit_sales,

    case 
        when cast(unit_sales as double precision) <0 then true 
        else false 
    end as is_return,

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

FROM {{ source('raw', 'train') }}

where 
    date is not null
    and store_nbr is not null
    and unit_sales is not null
    and item_nbr is not null

