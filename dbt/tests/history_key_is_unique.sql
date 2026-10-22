-- Grain of product_cost_history: one row per (sku, valid_from).
select sku, valid_from, count(*) as n
from {{ ref('product_cost_history') }}
group by sku, valid_from
having count(*) > 1
