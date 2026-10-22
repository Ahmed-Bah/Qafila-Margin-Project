-- Grain: one row per supplier price row, in every file (source_file, row_num).
-- Nothing is dropped here: bad rows are classified further down, not removed.
--
-- The cost is `unit_cost`, or `cost_sar` in files that renamed the column; each file has only one of them.
-- raw_cost keeps the text exactly as it arrived (needed for the quarantine table).
-- cost_parsed turns Arabic-Indic digits and decimal commas into a number; NULL when it is not a number.
with parsed as (
    select
        source_file,
        row_num,
        regexp_replace(sku, '^\s+|\s+$', '', 'g') as sku_clean,
        try_cast(valid_from as date) as valid_from,
        coalesce(nullif(unit_cost, ''), nullif(cost_sar, '')) as raw_cost
    from {{ source('raw', 'supplier_prices') }}
),

normalised as (
    select
        *,
        -- Arabic-Indic (٠-٩) and Persian (۰-۹) digits -> 0-9, then ٫ (Arabic decimal separator) and , -> .
        replace(replace(translate(translate(trim(raw_cost), '٠١٢٣٤٥٦٧٨٩', '0123456789'), '۰۱۲۳۴۵۶۷۸۹', '0123456789'), '٫', '.'), ',', '.') as cost_text
    from parsed
)

select
    source_file,
    row_num,
    sku_clean,
    valid_from,
    raw_cost,
    try_cast(cost_text as double) as cost_parsed
from normalised
