# Qafila gross margin

Submitted by: Ahmad Bahwerith - academy: @SDAIAAcademy

This pipeline calculates weekly gross margin by city and product category. It combines order data with supplier price history so each order line uses the cost that applied on its order date. It also keeps rejected supplier rows and preserves earlier results in Iceberg snapshots.

The task and output contract are in [BRIEF.md](BRIEF.md).

## Run the project

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
```

The project uses Python, DuckDB, dbt and PyIceberg. Dependencies are defined in `pyproject.toml` and locked in `uv.lock`.

## Design

`ingest()` loads the four SQLite app tables and all supplier price files into DuckDB's `raw` schema. It replaces the raw tables in one transaction, so loading the same files again does not duplicate rows. Supplier values land as text, with `source_file` and `row_num` retained for tracing and tie-breaking.

dbt builds the staging views, validates supplier prices, creates the cost history and calculates margin. `publish()` then writes the three output tables to Iceberg.

| Model | Layer | One row per | Materialization |
|---|---|---|---|
| `stg_orders` | staging | Order, with its Riyadh `order_date` | view |
| `stg_order_items` | staging | Order line (`order_id`, `line_no`) | view |
| `stg_products` | staging | Product, with trimmed `sku_clean` | view |
| `stg_stores` | staging | Store | view |
| `stg_supplier_prices` | staging | Supplier row (`source_file`, `row_num`), including rows with invalid costs | view |
| `supplier_prices_validated` | silver | Supplier row, with `reject_reason` | table |
| `product_cost_history` | silver | SKU and cost version (SCD2) | table |
| `weekly_margin` | gold | `iso_week`, `city` and `category` with counted lines | table |
| `price_quarantine` | quality | Rejected supplier row | table |

### Supplier cost history

SKU matching uses trimmed values on both sides. A row with an unknown SKU goes to quarantine as `unknown_sku`. Otherwise, an unreadable, non-finite or non-positive cost is rejected as `invalid_cost`. Rejected rows remain in quarantine even if a later file supplies a usable replacement.

For each SKU and `valid_from`, the usable row from the file whose name sorts last wins. This lets `prices_2026-W39_fix.csv` replace prices from `prices_2026-W39.csv`. Within the same file, `row_num` breaks a tie.

A new SCD2 version starts only when the rounded cost changes. Repeated costs are skipped, and a product missing from a weekly file keeps its previous cost. Each version ends at the next version's `valid_from`, exclusive. The current version has an empty `valid_to`.

The history is rebuilt from all available supplier files on every run. A correction therefore follows the same rules as any other input file.

### Margin calculation

An order line counts when the order is delivered, quantity is positive and its Riyadh date is on or after `2026-08-31`. `created_at` is converted from UTC to UTC+3 before filtering or assigning the ISO week. City comes from the store and category from the product.

Line revenue is rounded to two decimals after subtracting the discount. The cost join uses the trimmed SKU and `valid_from <= order_date AND (valid_to IS NULL OR order_date < valid_to)`. Non-overlapping cost versions give each line at most one match.

A line without a matching cost still contributes to `lines` and `revenue_sar`, and is counted in `uncosted_lines`. It contributes nothing to `costed_revenue_sar`, `cost_sar` or `margin_sar`. Margin is revenue from costed lines minus their supplier costs.

## Data problems

These counts are from the seed 2026 dataset, with the late correction included where relevant.

| Problem | Where | Count | Handling |
|---|---|---|---|
| Decimal commas (`9,49`) | W38 `unit_cost` | 126 of 160 rows | Replace the comma with a decimal point before casting. |
| Arabic-Indic digits (`٩٫٤٩`) | W38 `unit_cost` | 34 of 160 rows | Translate Arabic-Indic and Persian digits to `0-9`, and normalize the decimal separator. |
| Byte-order mark | W38 header | 1 file | DuckDB reads the first column as `supplier_id`, without the BOM. |
| `unit_cost` renamed to `cost_sar`, with `currency` added | W39 and the correction | 160 + 9 rows | Load text columns with `UNION ALL BY NAME` and use the populated cost field. |
| Zero or negative cost | W39 | 4 rows: `-1`, `-1`, `0`, `-5.5` | Reject as `invalid_cost`. The correction supplies usable prices for all four. |
| Unknown SKUs (`QF-NEW-...`) | W39 | 3 rows | Reject as `unknown_sku`. |
| Trailing spaces in product SKUs | `products` | 2 products (`QF-FRU-1107`, `QF-PAN-1331`), affecting 3 supplier rows | Trim both product and supplier SKUs so valid matches are retained. |
| Late W39 correction | `prices_2026-W39_fix.csv` | 9 rows: 4 replace rejected prices and 5 replace valid prices | Apply the filename precedence rule. |
| Excluded order lines | `order_items` | Of 172,545 lines, 20,014 belong to non-delivered orders, 129 have non-positive quantities on delivered orders, and 69,998 remaining lines predate the cutoff. This leaves 82,404 counted lines. | Filter before the cost join. |
| UTC and Riyadh dates differ | `orders.created_at` | 1,273 counted lines change date; 31 would be lost at the start boundary using UTC | Convert to Riyadh time before date comparisons. |
| No supplier quote valid on the order date | `weekly_margin` | 20,626 of 82,404 counted lines | Retain as uncosted. Each weekly file quotes only 160 of 356 products. |

## Tests

The project defines 27 dbt tests. All passed in the recorded run. `run.py` runs `dbt build`, including the tests, before publishing.

| Test | What it checks |
|---|---|
| `unique` and `not_null` on product IDs, trimmed SKUs and order IDs | Join keys exist and do not duplicate source rows. |
| `not_null` on order dates and supplier dates | Dates can be used in filtering and cost history. |
| `accepted_values` on rejection reasons | Reasons are `unknown_sku` or `invalid_cost`. |
| `relationships` from cost-history SKUs to products | Cost versions belong to known products. |
| `history_one_current_version_per_sku` | Each history SKU has one current version, matching `valid_to IS NULL`. |
| `history_versions_are_contiguous` | Versions have no gaps or overlaps, and end dates follow start dates. |
| `history_new_version_only_when_cost_changes` | Adjacent versions have different, positive costs. |
| `history_key_is_unique` | A SKU has only one version for a given `valid_from`. |
| `supplier_rows_are_all_accounted_for` | Validation preserves supplier row counts and quarantine contains every rejected row. |
| `weekly_margin_key_is_unique` | There is one row per week, city and category. |
| `weekly_margin_is_internally_consistent` | Margin reconciles to costed revenue minus cost, and counts and revenue stay within their totals. |
| `weekly_margin_reconciles_with_source` | Counted lines and revenue reconcile to the raw app tables. |

`tools/check.py` checks the output contract and internal consistency. It does not calculate the correct supplier costs or margins. Earlier verification also compared all three outputs with a separate plain-Python implementation, before and after the correction and on `--seed 11`. The outputs matched in those runs; a shared misunderstanding of the rules could still pass both implementations.

For a fresh verification of both datasets, including all output rows, repeated runs and snapshot history:

```bash
uv run --frozen python tools/verify_submission.py
```

This runs the pipeline four times for each of seeds 2026 and 11: initial load, unchanged inputs, the late correction, and unchanged corrected inputs. It compares all three Iceberg tables with a Python calculation from the source files, checks every dbt test and runs `tools/check.py`. Logs and the final JSON report stay under `.verification/`.

## Re-runs and late corrections

The following results were recorded from the earlier seed 2026 pipeline run. Snapshot IDs belong to that warehouse and will differ in a fresh run.

| Run | `silver.product_cost_history` | `gold.weekly_margin` | `quality.price_quarantine` |
|---|---|---|---|
| 1. Empty warehouse | `5981016639687727566`: append, 633 rows | `8600699754360832633`: append, 165 rows | `8940007364958719481`: append, 7 rows |
| 2. Same inputs | Same snapshot and rows | Same snapshot and rows | Same snapshot and rows |
| 3. W39 correction added | `6084255550135153735`: overwrite, 637 rows | `1217300711984114185`: overwrite, 165 rows | Unchanged |
| 4. Same corrected inputs | Unchanged | Unchanged | Unchanged |

`publish()` compares the new rows with the existing table contents. Equal contents produce no commit. Changed contents replace the data files in one overwrite commit on the existing table, preserving its earlier snapshots. The implementation uses the snapshot overwrite API because `Table.overwrite()` would create separate delete and append snapshots.

Reading the earlier `gold.weekly_margin` snapshot (`8600699754360832633`) after the correction retained the original numbers:

| Result | Total `margin_sar` | Week 39, Riyadh, Beverages `margin_sar` | Rows changed |
|---|---|---|---|
| Before correction | 801,016.88 | 7,730.40 | |
| After correction | 803,515.80 | 8,555.60 | 36 of 165 |

To add the correction after the first run:

```bash
uv run python tools/make_data.py --add-correction
uv run python run.py
uv run python tools/check.py
```

To run a second dataset in a separate warehouse:

```bash
uv run python tools/make_data.py --seed 11 --out data_11
uv run python run.py --data-dir data_11 --warehouse warehouse/seed_11
uv run python tools/check.py --data-dir data_11 --warehouse warehouse/seed_11
```

Generated data, environments and warehouse files should stay out of Git.

## What I would change for larger inputs

`ingest()` reads each SQLite table into Python memory. At 100 times the data volume, I would start by loading through DuckDB's SQLite scanner or Parquet. Margin is also rebuilt from the full history each time. Rebuilding only affected weeks and partitioning Iceberg by `iso_week` would reduce that work.

Daily supplier files would need a filename convention that preserves the required sort order. Names such as `W9` and `W10` do not sort chronologically. I would make the date and version part of the file contract, then review that change against the precedence rule. Checks that scan the entire history would also need a strategy for larger inputs.

## AI assistance

I used Claude (Anthropic, through Claude Code) to help me write the code for this project.
