{{
    config(materialized='table',tags=['marts', 'fact'])
}}


with sales as (
    select * from {{ ref('stg_sales') }}
),
products as (
    select * from {{ ref('dim_products') }}
),
stores as (
    select * from {{ ref('stg_stores') }}
),

daily as (
    select
        -- grain: store + product + day
        sale_date,
        store_id,
        product_id,

        sale_year,
        sale_month,
        sale_day,
        day_of_week,
        day_name,
        is_weekend,

        count(distinct sale_id) as transaction_count,
        sum(quantity_sold) as total_quantity_sold,

        sum(total_amount + discount_amount) as total_gross_amount,
        sum(discount_amount) as total_discount_amount,
        sum(total_amount) as total_net_amount,

        avg(unit_price) as avg_unit_price,
        avg(discount_rate) as avg_discount_rate,

        min(unit_price) as min_unit_price,
        max(unit_price) as max_unit_price

    from sales
    group by
        sale_date, store_id, product_id,
        sale_year, sale_month, sale_day,
        day_of_week, day_name, is_weekend
),

enriched as (
    select
        d.*,

        st.store_name,
        st.region,
        st.store_type,
        st.store_size_category,

        p.product_name,
        p.category,
        p.unit_cost,
        p.supplier_name,

        (d.total_quantity_sold * p.unit_cost) as total_cost,
        (d.total_net_amount - (d.total_quantity_sold * p.unit_cost)) as total_profit,
        (d.total_net_amount - (d.total_quantity_sold * p.unit_cost)) / nullif(d.total_net_amount, 0) as profit_margin

    from daily d
    left join stores st
      on d.store_id = st.store_id
    left join products p
      on d.product_id = p.product_id
)

select * from enriched;
