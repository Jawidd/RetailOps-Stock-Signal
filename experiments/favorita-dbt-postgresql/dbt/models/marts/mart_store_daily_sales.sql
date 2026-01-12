
with daily_sales as (
    select

        date,
        store_nbr,
        count(distinct item_nbr) as unique_item_types_sold,
        count(*) as line_items, 
        sum(unit_sales) as total_units_sold,
        -- will show retuned units as positive numbers
        sum(case when is_return then abs(unit_sales) else 0 end) as returned_units,
        sum(case when is_return then 0 else unit_sales end) as sold_units,
        sum(case when onpromotion then unit_sales else 0 end) as promo_units,
        sum(case when onpromotion then 0 else unit_sales end) as nonpromo_units,
        count(*) filter (where onpromotion) as items_on_promo,
        count(distinct item_nbr) filter (where onpromotion) as distinct_items_on_promo,
        max(year) as year,
        max(month)as month,
        max(day) as day,
        max(day_of_week) as day_of_week,
        bool_or(is_wage_day) as is_wage_day,
        bool_or(is_earthquake_period) as is_earthquake_period

    
    from {{ ref('stg_train') }}
    group by date, store_nbr
),



store_transactions as (
    select
        date,
        store_nbr,
        sum(transactions) as transactions_count
    from  {{ ref('stg_transactions') }}
    group by date, store_nbr

),


holiday_flags as (
    select
        holiday_date,
        bool_or(is_actual_holiday) as is_actual_holiday
    from  {{ ref('stg_holidays') }}
    where is_actual_holiday = true
    and holiday_locale in ('National', 'Regional')
    group by 1
)




select
    ds.date as saledate,
    ds.store_nbr,
    -- Store context
    st.city,
    st.state,
    st.store_type,
    st.store_cluster,
    -- Sales metrics
    ds.unique_item_types_sold,
    ds.line_items,
    ds.total_units_sold,
    ds.returned_units,
    ds.sold_units,
    -- Transactions
    coalesce(tr.transactions_count,0) as transactions_count,
    -- Promotion metrics
    ds.promo_units,
    ds.nonpromo_units,
    ds.items_on_promo,
    ds.year,
    ds.month,
    ds.day,
    ds.day_of_week,
    ds.is_wage_day,
    ds.is_earthquake_period,
    -- to_char(ds.date, 'Day') as day_name,
    to_char(ds.date, 'FMDay') as day_name,

    coalesce(hf.is_actual_holiday, FALSE) as is_holiday



from daily_sales ds

    left join {{ref ('stg_stores')}} st
    on ds.store_nbr = st.store_nbr

   left join store_transactions tr
   on ds.date = tr.date and ds.store_nbr = tr.store_nbr

   left join holiday_flags hf
   on ds.date = hf.holiday_date

order by ds.date, ds.store_nbr

