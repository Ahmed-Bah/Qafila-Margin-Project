-- Grain: one row per SKU per cost version (SCD2).
--
-- 1. Only usable rows count; quarantined rows are ignored (rule 3).
-- 2. Same (sku, valid_from) in several files: the row from the file whose name sorts last wins (rule 5),
--    so prices_2026-W39_fix.csv replaces prices_2026-W39.csv.
-- 3. A product missing from a weekly file simply has no new row, so its version stays open (rule 4).
-- 4. A new version starts only when the cost differs from the previous one (rule 6): repeated costs are dropped.
-- 5. valid_to is the next version's valid_from (exclusive), empty for the current version.
with usable as (
    select sku, valid_from, cost, source_file, row_num
    from {{ ref('supplier_prices_validated') }}
    where reject_reason is null
),

latest_per_day as (
    select sku, valid_from, cost
    from usable
    qualify row_number() over (partition by sku, valid_from order by source_file desc, row_num desc) = 1
),

with_previous as (
    select *, lag(cost) over (partition by sku order by valid_from) as previous_cost
    from latest_per_day
),

versions as (
    select sku, valid_from, cost
    from with_previous
    where previous_cost is null or cost <> previous_cost
)

select
    sku,
    cast(cost as double) as unit_cost_sar,
    valid_from,
    lead(valid_from) over (partition by sku order by valid_from) as valid_to,
    lead(valid_from) over (partition by sku order by valid_from) is null as is_current
from versions
