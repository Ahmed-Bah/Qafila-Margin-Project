-- Grain: one row per supplier price row (source_file, row_num): the same rows as stg_supplier_prices.
-- Adds the verdict: reject_reason is NULL for a usable row, otherwise why it is rejected.
-- Order of checks (rule 3): unknown SKU first, then a cost that is not a positive number.
select
    s.source_file,
    s.row_num,
    s.sku_clean as sku,
    s.valid_from,
    s.raw_cost,
    case
        when p.sku_clean is null then 'unknown_sku'
        when s.cost_parsed is null or not isfinite(s.cost_parsed) or s.cost_parsed <= 0 then 'invalid_cost'
    end as reject_reason,
    cast(round(s.cost_parsed, 2) as decimal(18, 2)) as cost
from {{ ref('stg_supplier_prices') }} as s
left join {{ ref('stg_products') }} as p on p.sku_clean = s.sku_clean
