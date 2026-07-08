{{ config(materialized='table') }}

with deduped as (
    select
        *,
        row_number() over (
            partition by user_id, session_id, event_type, page, timestamp
            order by event_time
        ) as rn
    from {{ source('bronze', 'clickstream_events') }}
)

select
    user_id,
    session_id,
    event_type,
    page,
    timestamp,
    event_time
from deduped
where rn = 1