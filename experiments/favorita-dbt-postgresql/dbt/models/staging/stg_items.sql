-- items_COLUMNS= item_nbr,family,class,perishable

SELECT
    cast(item_nbr as int) as item_nbr,
    family,
    class,
    perishable

FROM {{source('raw','items')}}

where item_nbr is not null

    

