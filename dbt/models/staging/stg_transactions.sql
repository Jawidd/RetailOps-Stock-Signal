-- COLUMNS= date,store_nbr,transactions




SELECT
    cast(date as date) as date,
    store_nbr,
    transactions

FROM {{ source('raw', 'transactions') }}

where
    date is not null
    and store_nbr is not null
    and transactions is not null