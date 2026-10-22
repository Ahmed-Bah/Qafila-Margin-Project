-- Grain: one row per order (order_id).
-- created_at is UTC text; order_date is the calendar date in Riyadh (UTC+3, no daylight saving).
select
    order_id,
    store_id,
    status,
    cast(try_cast(created_at as timestamp) + interval 3 hour as date) as order_date
from {{ source('raw', 'orders') }}
