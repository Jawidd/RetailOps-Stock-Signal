-- items_COLUMNS= item_nbr,family,class,perishable

SELECT
    item_nbr,
    family,
    class,
    perishable

FROM {{source('raw','items')}}

where item_nbr is not null

    

