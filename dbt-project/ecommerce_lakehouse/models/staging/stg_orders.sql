{{ config(materialized='table') }}

with windowed as (
    select
        order_id,
        first_value(user_id, true) over (
            partition by order_id order by event_time
            rows between unbounded preceding and unbounded following
        ) as user_id,
        last_value(status, true) over (
            partition by order_id order by event_time
            rows between unbounded preceding and unbounded following
        ) as current_status,
        first_value(amount, true) over (
            partition by order_id order by event_time
            rows between unbounded preceding and unbounded following
        ) as amount,
        first_value(items, true) over (
            partition by order_id order by event_time
            rows between unbounded preceding and unbounded following
        ) as items,
        first_value(event_time) over (
            partition by order_id order by event_time
            rows between unbounded preceding and unbounded following
        ) as first_event_time,
        last_value(event_time) over (
            partition by order_id order by event_time
            rows between unbounded preceding and unbounded following
        ) as last_event_time,
        row_number() over (partition by order_id order by event_time) as rn
    from {{ source('bronze', 'order_events') }}
)

select
    order_id,
    user_id,
    current_status,
    amount,
    items,
    first_event_time,
    last_event_time
from windowed
where rn = 1