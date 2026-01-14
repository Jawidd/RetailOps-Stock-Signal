{{
    config(materialized='table',tags=['marts', 'mart'])
}}


with sh as (
    select * from {{ ref('stg_shipments') }}
),
sup as (
    select * from {{ ref('stg_suppliers') }}
),

metrics as (
    select
        supplier_id,

        count(distinct shipment_id) as total_shipments,
        count(distinct shipment_id) filter (where is_late) as late_shipments,
        count(distinct shipment_id) filter (where is_partial_shipment) as partial_shipments,
        count(distinct shipment_id) filter (where is_delayed) as delayed_shipments,

        avg(actual_lead_time_days) as avg_actual_lead_time_days,
        avg(expected_lead_time_days) as avg_expected_lead_time_days,
        avg(days_late) as avg_days_late,
        avg(fill_rate) as avg_fill_rate,

        -- calculated on-time rate from actual shipment records
        (count(*) filter (where not is_late)) / nullif(count(*), 0) as calculated_on_time_rate,

        sum(quantity_ordered) as total_quantity_ordered,
        sum(quantity_received) as total_quantity_received,
        sum(quantity_variance) as total_quantity_variance
    from sh
    group by supplier_id
),

final as (
    select
        sup.supplier_id,
        sup.supplier_name,
        sup.country,
        sup.lead_time_category,
        sup.on_time_performance_tier,

        m.total_shipments,
        m.late_shipments,
        m.partial_shipments,
        m.delayed_shipments,

        m.avg_actual_lead_time_days,
        m.avg_expected_lead_time_days,
        m.avg_days_late,
        m.avg_fill_rate,

        m.calculated_on_time_rate,

        m.total_quantity_ordered,
        m.total_quantity_received,
        m.total_quantity_variance,

        -- compare “master data” on_time_delivery_rate vs calculated
        sup.on_time_delivery_rate as master_on_time_rate,
        (m.calculated_on_time_rate - sup.on_time_delivery_rate) as on_time_rate_variance
    from sup
    left join metrics m
      on sup.supplier_id = m.supplier_id
)

select * from final;
