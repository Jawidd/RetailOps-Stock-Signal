-- COLUMNS= date,dcoilwtico

SELECT
    cast(date as date) as date,
    dcoilwtico as oil_price

FROM {{ source('raw', 'oil') }}

where 
    date is not null