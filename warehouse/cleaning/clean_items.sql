-- Clean(fix types, handles nulls, filters bad data) and standardize data 
-- COLUMNS= item_nbr,family,class,perishable

select
    item_nbr,
    family,
    class,
    perishable
from
    raw.items
where
    family is not null
    and class is not null
    and perishable is not null
    and perishable in (0,1)
