-- COLUMNS= date,dcoilwtico

SELECT
    cast(date as date) as date,
    cast(dcoilwtico as double precision) as oil_price,
    --  oil price change compared to last price
    cast(dcoilwtico as double precision)
    - lag(cast(dcoilwtico as double precision), 1) over (order by cast(date as date))
    as oil_price_change


FROM {{ source('raw', 'oil') }}

where 
    date is not null
    and dcoilwtico is not null
