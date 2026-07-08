{{ config(materialized='table', schema='gold') }}

with clicks as (
    select
        user_id,
        count(*) as total_clicks
    from {{ ref('stg_clickstream') }}
    group by user_id
),

orders as (
    select
        user_id,
        count(*) as total_orders,
        sum(amount) as total_revenue
    from {{ ref('stg_orders') }}
    group by user_id
)

select
    coalesce(clicks.user_id, orders.user_id) as user_id,
    coalesce(clicks.total_clicks, 0) as total_clicks,
    coalesce(orders.total_orders, 0) as total_orders,
    coalesce(orders.total_revenue, 0.0) as total_revenue,
    case
        when coalesce(clicks.total_clicks, 0) > 0
        then coalesce(orders.total_orders, 0) / clicks.total_clicks
        else 0.0
    end as conversion_rate
from clicks
full outer join orders
    on clicks.user_id = orders.user_id