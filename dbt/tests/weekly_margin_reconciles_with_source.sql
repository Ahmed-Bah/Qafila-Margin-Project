-- Independent recount straight from the raw app tables: the margin table must cover every eligible order line once.
-- Catches a join that drops lines (missing product/store) or multiplies them (two cost versions for one date).
with expected as (
    select
        count(*) as lines,
        round(sum(round(i.quantity * i.unit_price - coalesce(i.discount, 0), 2)), 2) as revenue
    from {{ source('raw', 'order_items') }} as i
    inner join {{ source('raw', 'orders') }} as o on o.order_id = i.order_id
    where o.status = 'delivered'
      and i.quantity > 0
      and cast(try_cast(o.created_at as timestamp) + interval 3 hour as date) >= date '{{ var("margin_start_date") }}'
),

actual as (
    select sum(lines) as lines, round(sum(revenue_sar), 2) as revenue
    from {{ ref('weekly_margin') }}
)

select expected.lines as expected_lines, actual.lines as actual_lines,
       expected.revenue as expected_revenue, actual.revenue as actual_revenue
from expected cross join actual
where expected.lines <> actual.lines or abs(expected.revenue - actual.revenue) > 0.05
