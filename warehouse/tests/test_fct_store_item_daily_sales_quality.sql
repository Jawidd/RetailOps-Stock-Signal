-- Data Quality Tests for fct_store_item_daily_sales
-- Each test return 0/0rows if test passes


select 'Test 1: No null dates' as test_name, count(*) as failed_rows
    from fact.fct_store_item_daily_sales
    where sales_date is null
    having count(*) > 0
union all 


select 'Test 2: duplicate date-store ' as test_name, count(*) as failed_rows
from (
    select sales_date, store_nbr, count(*) as n
    from fact.fct_store_item_daily_sales
    group by sales_date, store_nbr
    having count(*) > 1
    ) d
    having count(*) > 0
union all


select 'Test 3: invalid store numbers' as test_name, count(*) as failed_rows
    from fact.fct_store_item_daily_sales fct
    left join clean.clean_stores str
    on fct.store_nbr = str.store_nbr
    where str.store_nbr is null
    having count(*) > 0
union all


select 'Test 4: null unit sales' as test_name, count(*) as failed_rows
    from fact.fct_store_item_daily_sales
    where total_unit_sales is NULL
    having count(*) > 0
union all


select 'Test 5: no null numeric' as test_name, count(*) as failed_rows
    from fact.fct_store_item_daily_sales
    where sold_units is NULL
        or returned_units is NULL
        or unique_items_sold is NULL
        or promo_units is NULL
        or nonpromo_units is NULL
        or items_on_promo is NULL
        or transactions_count is NULL
    having count(*) > 0
union all


select 'Test 6: metrics must be non-negative' as test_name, count(*) as failed_rows
    from fact.fct_store_item_daily_sales
    where transactions_count < 0
        or sold_units < 0
        or unique_items_sold < 0
        or items_on_promo < 0
    having count(*) > 0 ;

