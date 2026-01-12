-- COLUMNS= date,store_nbr,transactions




SELECT
    cast(date as date) as date,
    cast(store_nbr as int) as store_nbr,
    cast(transactions as int) as transactions

FROM {{ source('raw', 'transactions') }}

where
    date is not null
    and store_nbr is not null
    and transactions is not null