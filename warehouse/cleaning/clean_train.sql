-- Clean(fix types, handles nulls, filters bad data) and standardize train data 
-- COLUMNS= id,date,store_nbr,item_nbr,unit_sales,onpromotion



CREATE OR REPLACE TABLE cleaning.clean_train AS
SELECT
    cast(id as INT) as id,
    cast(date as DATE) as date,
    cast(store_nbr as INT) as store_nbr,
    cast(item_nbr as INT) as item_nbr,
    cast(unit_sales as FLOAT(10,2)) as unit_sales,

    -- Handle unit_sales: keep negatives (they're returns), but flag them
    case when unit_sales < 0 then TRUE else FALSE end as is_return,

    -- Handle nulls
    coalesce(onpromotion, 'FALSE') as onpromotion,

    -- Add derived fields useful for analysis
    extract(year from date) as sale_year,
    extract(month from date) as sale_month,
    extract(day from date) as sale_day,
    dayofweek(date) as day_of_week,

-- handle wages days (15th and last day of each month)
    case 
        when day(date) = 15 or day(date) = day(last_day(date)) 
        then TRUE 
        else FALSE 
    end as is_wage_day,

-- handle earthquake days (April 16-30, 2016)
    ,case 
        when date between '2016-04-16' and '2016-04-30' 
        then TRUE 
        else FALSE 
    end as is_earthquake_period


from raw.train
    where date is not null
    and store_nbr is not null
    and item_nbr is not null;