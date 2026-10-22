-- Every SKU must have exactly one open (current) version, and it must be the one with no valid_to.
select sku
from {{ ref('product_cost_history') }}
group by sku
having count(*) filter (where is_current) <> 1
    or count(*) filter (where valid_to is null) <> 1
    or bool_or(is_current <> (valid_to is null))
