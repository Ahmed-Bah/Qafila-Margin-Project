-- Two neighbouring versions of a SKU must never carry the same cost (rule 6), and every cost must be positive.
with ordered as (
    select
        sku, valid_from, unit_cost_sar,
        lag(unit_cost_sar) over (partition by sku order by valid_from) as previous_cost
    from {{ ref('product_cost_history') }}
)
select * from ordered
where unit_cost_sar = previous_cost or unit_cost_sar <= 0
