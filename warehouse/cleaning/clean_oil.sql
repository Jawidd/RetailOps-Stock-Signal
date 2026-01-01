-- Clean(fix types, handles nulls, filters bad data) and standardize data 
-- COLUMNS= date,dcoilwtico

SELECT
    CAST(date AS DATE) as date,
    CAST(dcoilwtico AS FLOAT(10,4)) as oil_price

-- compare with last oil price and return difference
    ,dcoilwtico - lag(dcoilwtico) over (order by date) as oil_price_change

from raw.oil
where date is not null
and where dcoilwtico is not null;