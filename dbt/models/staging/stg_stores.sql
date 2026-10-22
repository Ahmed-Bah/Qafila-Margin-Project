-- Grain: one row per store (store_id).
select store_id, city
from {{ source('raw', 'stores') }}
