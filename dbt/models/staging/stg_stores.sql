select
    cast(store_nbr as int) as store_nbr,
    city,
    state,
    "type" as store_type,
    cluster as store_cluster
from {{ source('raw', 'stores') }}

where store_nbr is not null
