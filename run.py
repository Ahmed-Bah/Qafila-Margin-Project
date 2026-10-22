"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, DateType, DoubleType, IntegerType, NestedField, StringType

DBT_DIR = Path(__file__).resolve().parent / "dbt"

# The four app tables landed from the SQLite database.
APP_TABLES = ["orders", "order_items", "products", "stores"]
# SQLite declared type -> Arrow type (anything else lands as text).
SQLITE_TO_ARROW = {"INTEGER": pa.int64(), "INT": pa.int64(), "REAL": pa.float64(), "TEXT": pa.string()}


def _q(text: str) -> str:
    """Quote a string as a SQL literal."""
    return "'" + text.replace("'", "''") + "'"


def _read_sqlite_table(sqlite_path: Path, table: str) -> pa.Table:
    """One SQLite table as a typed Arrow table, using the types the table declares."""
    with closing(sqlite3.connect(f"file:{sqlite_path.as_posix()}?mode=ro", uri=True)) as con:
        cols = [(r[1], (r[2] or "").upper()) for r in con.execute(f"PRAGMA table_info({table})")]
        rows = con.execute(f"SELECT * FROM {table}").fetchall()
    arrays = [pa.array([r[i] for r in rows], type=SQLITE_TO_ARROW.get(typ, pa.string())) for i, (_, typ) in enumerate(cols)]
    return pa.table(arrays, names=[name for name, _ in cols])


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    files = sorted((data_dir / "suppliers").glob("prices_*.csv"), key=lambda p: p.name)

    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        con.sql("BEGIN")  # all-or-nothing: a failed load leaves the previous raw layer untouched

        for table in APP_TABLES:
            con.register("_src", _read_sqlite_table(data_dir / "qafila_oltp.sqlite", table))
            con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM _src")
            con.unregister("_src")

        # Supplier files: read every column as text so nothing is lost or guessed at this layer. Each file is read
        # on its own because the columns differ between files (unit_cost / cost_sar); BY NAME fills the gaps with NULL.
        # row_num keeps the order of rows inside a file, so later steps can break ties deterministically.
        parts = [
            f"SELECT {_q(f.name)} AS source_file, row_number() OVER () AS row_num, * "
            f"FROM read_csv({_q(f.as_posix())}, header = true, all_varchar = true)"
            for f in files
        ]
        if parts:
            con.sql("CREATE OR REPLACE TABLE raw.supplier_prices AS " + " UNION ALL BY NAME ".join(f"({p})" for p in parts))
        else:
            con.sql("CREATE OR REPLACE TABLE raw.supplier_prices (source_file VARCHAR, row_num BIGINT)")
        # Downstream SQL reads these columns, so they must exist even if no file has them (e.g. before week 39).
        for col in ("sku", "valid_from", "unit_cost", "cost_sar"):
            con.sql(f"ALTER TABLE raw.supplier_prices ADD COLUMN IF NOT EXISTS {col} VARCHAR")

        con.sql("COMMIT")


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


# The three published tables: (column, Iceberg type, Arrow type), plus the key the rows are ordered and compared by.
PUBLISHED = {
    "silver.product_cost_history": {
        "key": ["sku", "valid_from"],
        "columns": [("sku", StringType(), pa.string()), ("unit_cost_sar", DoubleType(), pa.float64()),
                    ("valid_from", DateType(), pa.date32()), ("valid_to", DateType(), pa.date32()),
                    ("is_current", BooleanType(), pa.bool_())],
    },
    "gold.weekly_margin": {
        "key": ["iso_week", "city", "category"],
        "columns": [("iso_week", IntegerType(), pa.int32()), ("city", StringType(), pa.string()),
                    ("category", StringType(), pa.string()), ("lines", IntegerType(), pa.int32()),
                    ("revenue_sar", DoubleType(), pa.float64()), ("costed_revenue_sar", DoubleType(), pa.float64()),
                    ("cost_sar", DoubleType(), pa.float64()), ("margin_sar", DoubleType(), pa.float64()),
                    ("uncosted_lines", IntegerType(), pa.int32())],
    },
    "quality.price_quarantine": {
        "key": ["source_file", "sku", "valid_from", "reason"],
        "columns": [("source_file", StringType(), pa.string()), ("sku", StringType(), pa.string()),
                    ("valid_from", DateType(), pa.date32()), ("raw_cost", StringType(), pa.string()),
                    ("reason", StringType(), pa.string())],
    },
}


def _iceberg_schema(columns: list) -> Schema:
    return Schema(*[NestedField(i, name, ice_type, required=False) for i, (name, ice_type, _) in enumerate(columns, start=1)])


def _sync_iceberg_table(catalog: SqlCatalog, ident: str, spec: dict, data: pa.Table) -> str:
    """Make the Iceberg table hold exactly `data`, in place and with at most one new snapshot.

    - first run: create the table and append (one snapshot);
    - same content as the table already holds: do nothing, so a re-run adds no snapshot;
    - content changed: replace the data files in ONE overwrite commit. The table is never dropped, and the previous
      snapshot stays readable. (Table.overwrite() would commit a delete snapshot and an append snapshot.)
    """
    catalog.create_namespace_if_not_exists(ident.split(".")[0])
    try:
        table = catalog.load_table(ident)
    except NoSuchTableError:
        table = catalog.create_table(ident, schema=_iceberg_schema(spec["columns"]))
        table.append(data)
        return f"created, {data.num_rows} rows"

    sort = [(name, "ascending") for name, _, _ in spec["columns"]]
    if table.current_snapshot() is not None:
        current = table.scan().to_arrow().select(data.column_names).cast(data.schema)
        if current.sort_by(sort).equals(data.sort_by(sort)):
            return f"unchanged, {data.num_rows} rows"

    with table.transaction() as tx:
        with tx.update_snapshot().overwrite() as overwrite:
            if tx.table_metadata.current_snapshot() is not None:
                for task in table.scan().plan_files():
                    overwrite.delete_data_file(task.file)
            if data.num_rows:
                for data_file in _dataframe_to_data_files(
                    table_metadata=tx.table_metadata, write_uuid=overwrite.commit_uuid, df=data, io=table.io
                ):
                    overwrite.append_data_file(data_file)
    return f"updated, {data.num_rows} rows"


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        for ident, spec in PUBLISHED.items():
            cols = [name for name, _, _ in spec["columns"]]
            data = con.execute(f"SELECT {', '.join(cols)} FROM {ident} ORDER BY {', '.join(spec['key'])}").to_arrow_table()
            data = data.cast(pa.schema([pa.field(name, arrow_type) for name, _, arrow_type in spec["columns"]]))
            print(f"publish {ident}: {_sync_iceberg_table(catalog, ident, spec, data)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
