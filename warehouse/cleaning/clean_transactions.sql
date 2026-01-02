-- Clean(fix types, handles nulls, filters bad data) and standardize data 
-- COLUMNS= date,store_nbr,transactions

select 
    date,
    store_nbr,
    transactions as transactions_count

from raw.transactions
WHERE date IS NOT NULL
  AND store_nbr IS NOT NULL
  AND transactions >= 0 