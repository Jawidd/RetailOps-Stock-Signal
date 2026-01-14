{{
    config(materialized='table',tags=['marts', 'dimension'])
}}


select
    p.product_id,
    p.product_name,
    p.category,

    p.unit_cost,
    p.unit_price,
    p.unit_gross_margin,
    p.unit_margin_pct,

    p.is_active,

    p.supplier_id,
    s.supplier_name,
    s.lead_time_category,
    s.on_time_delivery_rate,
    s.on_time_performance_tier,
    s.country as supplier_country,
    p.dbt_loaded_at
    
from {{ ref('stg_products') }} p
left join {{ ref('stg_suppliers') }} s
  on p.supplier_id = s.supplier_id
