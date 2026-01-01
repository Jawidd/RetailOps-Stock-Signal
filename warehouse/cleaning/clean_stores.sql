-- Clean(fix types, handles nulls, filters bad data) and standardize train data 
-- COLUMNS= store_nbr,city,state,type,cluster

SELECT
    store_nbr,
    ciry,
    state,
    type as store_type,
    cluster as store_cluster

from raw.stores
where store_nbr is not null;