-- Aggregates sales by store and date with context

with daily_sales as(
    Select
        date,
        store_nbr,

        count(distinct item_nbr) as unique_items_sold,
        count(*) as total_transactions,
        sum(unit_sales) as total_unit_sales,
        sum(case when is_return then unit_sales else 0 end)as returned_units,
        sum(case when not is_return then unit_sales else 0 end)as sold_units,

        sum(case when onpromotion then unit_sales else 0 end)as promo_units,
        sum(case when not onpromotion then unit_sales else 0 end)as nonpromo_units,
        count(case when onpromotion then 1  end) as items_on_promo,

        max(sale_year) as year,
        max(sale_month) as month,
        max(sale_day) as day,
        max(day_of_week) as day_of_week,
        max(is_wage_day) as is_wage_day,
        max(is_earthquake_period) as is_earthquake_period

    from clean.clean_train
    group by date, store_nbr
),

store_transactions as (
    Select
        date as transaction_date,
        store_nbr,
        transactions_count
    from clean.clean_transactions
),

holiday_flags as (
    select 
        holiday_date,
        is_actual_holiday
        )

select 
    ds.date as sales_date,
    ds.store_nbr,
    -- Store context
    st.city,
    st.state,
    st.store_type,
    st.store_cluster,
    -- Sales metrics
    ds.unique_items_sold,
    ds.total_transactions,
    ds.total_unit_sales,
    ds.sold_units,
    ds.returned_units,
    -- Promotion metrics
    ds.promo_units,
    ds.nonpromo_units,
    ds.items_on_promo,
    round(ds.promo_units::decimal / nullif(ds.total_unit_sales,0),4) as promo_unit_ratio,
    -- Transactions
    coalesce(tr.transactions_count,0) as transactions_count,
    -- Date context
    ds.year,
    ds.month,
    ds.day,
    ds.day_of_week,
    ds.is_wage_day,
    ds.is_earthquake_period,
    DAYNAME(ds.date) AS day_name,
    -- Holiday flag
    coalesce(hf.is_actual_holiday, FALSE) as is_holiday

from daily_sales ds
    left join clean.clean_stores st on ds.store_nbr = st.store_nbr
    left join clean.clean_transactions tr on ds.date = tr.date and ds.store_nbr = tr.store_nbr
    left join clean.clean_holidays hf on ds.date = hf.holiday_date
ORDER BY ds.date, ds.store_nbr ;