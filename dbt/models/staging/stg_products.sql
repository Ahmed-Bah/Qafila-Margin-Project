-- Grain: one row per product (product_id).
-- sku_clean is the SKU with surrounding whitespace removed: the key used to match supplier prices.
select
    product_id,
    regexp_replace(sku, '^\s+|\s+$', '', 'g') as sku_clean,
    category,
    unit_price
from {{ source('raw', 'products') }}
