{{
    config(materialized='table',tags=['marts', 'fact'])
}}

with inv as (
    select * from {{ ref('stg_inventory') }}
),
products as (
    select * from {{ ref('dim_products') }}
),
stores as (
    select * from {{ ref('stg_stores') }}
),

enriched as (
    select
        inv.snapshot_date,
        inv.store_id,
        inv.product_id,

        s.store_name,
        s.region,
        s.store_type,

        p.product_name,
        p.category,
        p.unit_cost,
        p.unit_price,

        inv.quantity_on_hand,
        greatest(inv.quantity_on_hand, 0) as quantity_on_hand_clipped, -- protects value calcs
        inv.quantity_on_order,
        inv.total_available_quantity,
        inv.reorder_point,
        inv.days_since_restock,

        -- inventory valuation (both raw and clipped)
        (inv.quantity_on_hand * p.unit_cost) as inventory_value_at_cost,
        (greatest(inv.quantity_on_hand, 0) * p.unit_cost) as inventory_value_at_cost_clipped,

        inv.is_out_of_stock,
        inv.needs_reorder,
        inv.is_slow_moving,

        inv.dbt_loaded_at
    from inv
    left join stores s
      on inv.store_id = s.store_id
    left join products p
      on inv.product_id = p.product_id
)

select * from enriched;
