-- Validation must neither drop nor duplicate supplier rows: rows in = rows out.
with counts as (
    select
        (select count(*) from {{ source('raw', 'supplier_prices') }}) as raw_rows,
        (select count(*) from {{ ref('supplier_prices_validated') }}) as validated_rows,
        (select count(*) from {{ ref('supplier_prices_validated') }} where reject_reason is not null)
            as quarantined_by_model,
        (select count(*) from {{ ref('price_quarantine') }}) as quarantine_rows
)
select * from counts
where raw_rows <> validated_rows or quarantined_by_model <> quarantine_rows
