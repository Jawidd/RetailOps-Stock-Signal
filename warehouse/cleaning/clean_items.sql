-- Clean(fix types, handles nulls, filters bad data) and standardize data 
-- COLUMNS= item_nbr,family,class,perishable

SELECT
    store_nbr,
    ciry,
    state,
    type as store_type,
    cluster as store_cluster

from raw.stores
where store_nbr is not null;