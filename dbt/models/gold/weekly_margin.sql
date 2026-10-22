-- Grain: one row per iso_week x city x category that has at least one counted order line.
--
-- A line counts when its order is delivered, quantity > 0 and the Riyadh order date is on/after the first supplier week
-- (rule 7). Each line is costed with the SCD2 version valid on its order date (rule 9):
-- valid_from <= order_date < valid_to. A line with no such version is "uncosted": it still counts in lines and
-- revenue_sar, but not in costed_revenue_sar / cost_sar / margin_sar.
with lines as (
    select
        o.order_date,
        st.city,
        p.category,
        p.sku_clean as sku,
        i.quantity,
        round(i.quantity * i.unit_price - i.discount, 2) as revenue
    from {{ ref('stg_order_items') }} as i
    inner join {{ ref('stg_orders') }} as o on o.order_id = i.order_id
    left join {{ ref('stg_products') }} as p on p.product_id = i.product_id
    left join {{ ref('stg_stores') }} as st on st.store_id = o.store_id
    where o.status = 'delivered'
      and i.quantity > 0
      and o.order_date >= date '{{ var("margin_start_date") }}'
),

costed as (
    select
        l.*,
        h.unit_cost_sar,
        l.quantity * h.unit_cost_sar as line_cost
    from lines as l
    left join {{ ref('product_cost_history') }} as h
        on h.sku = l.sku
       and l.order_date >= h.valid_from
       and (h.valid_to is null or l.order_date < h.valid_to)
)

select
    cast(weekofyear(order_date) as integer) as iso_week,
    city,
    category,
    cast(count(*) as integer) as lines,
    round(sum(revenue), 2) as revenue_sar,
    coalesce(round(sum(revenue) filter (where unit_cost_sar is not null), 2), 0.0) as costed_revenue_sar,
    coalesce(round(sum(line_cost), 2), 0.0) as cost_sar,
    round(
        coalesce(round(sum(revenue) filter (where unit_cost_sar is not null), 2), 0.0)
        - coalesce(round(sum(line_cost), 2), 0.0), 2) as margin_sar,
    cast(count(*) filter (where unit_cost_sar is null) as integer) as uncosted_lines
from costed
group by 1, 2, 3
