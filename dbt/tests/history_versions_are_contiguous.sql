-- No gaps and no overlaps: each version ends exactly where the next one starts (valid_to is exclusive).
with ordered as (
    select
        sku, valid_from, valid_to,
        lead(valid_from) over (partition by sku order by valid_from) as next_valid_from
    from {{ ref('product_cost_history') }}
)
select * from ordered
where (next_valid_from is not null and valid_to is distinct from next_valid_from)
   or (valid_to is not null and valid_to <= valid_from)
