-- Clean(fix types, handles nulls, filters bad data) and standardize train data 
-- COLUMNS= id,date,store_nbr,item_nbr,onpromotion
-- column to predict: unit_sales


SELECT
    id,
    cast(date as DATE) as date,
    store_nbr,
    item_nbr,

    -- Handle onpromotion nulls
    coalesce(onpromotion, FALSE) as onpromotion,

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

-- handle earthquake days (April 16-30, 2016) which will allways be null in test set
    case 
        when date between '2016-04-16' and '2016-04-30' 
        then TRUE 
        else FALSE 
    end as is_earthquake_period


from raw.test
    where date is not null
    and store_nbr is not null
    and item_nbr is not null;