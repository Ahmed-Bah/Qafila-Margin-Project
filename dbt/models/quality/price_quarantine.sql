-- Grain: one row per rejected supplier price row.
-- A row stays here even after a later file supplies a good price for the same SKU and date:
-- it was still a bad row in its own file.
select
    source_file,
    sku,
    valid_from,
    raw_cost,
    reject_reason as reason
from {{ ref('supplier_prices_validated') }}
where reject_reason is not null
