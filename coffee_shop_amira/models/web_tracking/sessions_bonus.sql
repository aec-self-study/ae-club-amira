{{ config(materialized='view') }}

with sessions as (
    select
        blended_user_id,
        session_number,
        timestamp,
        COUNT(page) as pages_visited,
        (case when page = 'order-confirmation' then 'true' else 'false' end) AS is_purchase,
from {{ref('sessions')}}
group by 1, 2, 3, 5
),

with_session_times as (
    select *,
        MIN(timestamp) over (partition by blended_user_id, session_number) as session_start_time,
        MAX(timestamp) over (partition by blended_user_id, session_number) as session_end_time,
from sessions   
),

with_difference_in_seconds as (
    select *,
    TIMESTAMP_DIFF(session_end_time, session_start_time, second) AS diff_duration,
from with_session_times
)

select
    blended_user_id,
    session_number,
    session_start_time,
    session_end_time,
    FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_SECONDS(diff_duration)) AS session_duration,
    pages_visited,
    is_purchase
from with_difference_in_seconds
