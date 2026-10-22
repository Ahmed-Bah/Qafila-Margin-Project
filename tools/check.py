"""Check your outputs against the contract in BRIEF.md before you submit.

    uv run python tools/check.py --data-dir data --warehouse warehouse

It checks shape and internal consistency: table names, columns, keys, SCD2 rules, that the margin
adds up, Iceberg history and your dbt tests. It does NOT know the correct costs or margins: the
grader compares those with the right answer on a dataset you haven't seen.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = {
    "silver.product_cost_history": ["sku", "unit_cost_sar", "valid_from", "valid_to", "is_current"],
    "gold.weekly_margin": ["iso_week", "city", "category", "lines", "revenue_sar", "costed_revenue_sar",
                           "cost_sar", "margin_sar", "uncosted_lines"],
    "quality.price_quarantine": ["source_file", "sku", "valid_from", "raw_cost", "reason"],
}
results: list[bool] = []


def check(ok: bool, msg: str, detail: str = "") -> bool:
    results.append(bool(ok))
    print(f"{'✅' if ok else '❌'} {msg}" + (f"  ({detail})" if detail and not ok else ""))
    return bool(ok)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=ROOT / "data")
    ap.add_argument("--warehouse", type=Path, default=ROOT / "warehouse")
    args = ap.parse_args()
    wh = args.warehouse.resolve()

    if not check((wh / "catalog.db").exists(), f"Iceberg catalog at {wh / 'catalog.db'}", "run run.py first"):
        sys.exit(1)
    cat = SqlCatalog("qafila", uri=f"sqlite:///{(wh / 'catalog.db').as_posix()}", warehouse=wh.as_posix())

    # 1. the three tables, with the contract's columns
    df: dict[str, pd.DataFrame] = {}
    for name, cols in COLUMNS.items():
        try:
            t = cat.load_table(name)
        except Exception:  # noqa: BLE001
            check(False, f"table {name} exists", "not found in the catalog 'qafila'")
            continue
        d = t.scan().to_pandas()
        missing = [c for c in cols if c not in d.columns]
        if check(not missing, f"table {name}: {len(d)} rows, columns as in the brief", f"missing {missing}"):
            df[name] = d
    if len(df) < 3:
        sys.exit(1)

    # 2. SCD2 rules
    h = df["silver.product_cost_history"].copy()
    h["valid_from"] = pd.to_datetime(h["valid_from"])
    h["valid_to"] = pd.to_datetime(h["valid_to"])
    h = h.sort_values(["sku", "valid_from"])
    check(not h.duplicated(["sku", "valid_from"]).any(), "history: one row per (sku, valid_from)")
    cur = h.groupby("sku")["is_current"].sum()
    check((cur == 1).all(), "history: exactly one current version per SKU", f"{int((cur != 1).sum())} SKUs differ")
    check((h["valid_to"].isna() == h["is_current"].astype(bool)).all(), "history: valid_to is empty exactly when is_current")
    nxt = h.groupby("sku")["valid_from"].shift(-1)
    gaps = h[nxt.notna() & (h["valid_to"] != nxt)]
    check(gaps.empty, "history: each version ends where the next one starts (valid_to is exclusive)", f"{len(gaps)} rows")
    check((h["unit_cost_sar"] > 0).all(), "history: every cost is positive")
    same = h.groupby("sku")["unit_cost_sar"].shift() == h["unit_cost_sar"]
    check(not same.any(), "history: a new version only when the cost changes", f"{int(same.sum())} repeated costs")

    # 3. quarantine
    q = df["quality.price_quarantine"]
    bad = set(q["reason"]) - {"unknown_sku", "invalid_cost"}
    check(not bad, "quarantine: reason is 'unknown_sku' or 'invalid_cost'", f"found {bad}")
    check(len(q) > 0, "quarantine: the planted bad supplier rows are there", "empty: the data has some")

    # 4. weekly margin
    m = df["gold.weekly_margin"]
    check(not m.duplicated(["iso_week", "city", "category"]).any(), "margin: one row per (iso_week, city, category)")
    check(m["iso_week"].between(36, 40).all(), "margin: iso_week between 36 and 40")
    diff = (m["costed_revenue_sar"] - m["cost_sar"] - m["margin_sar"]).abs()
    check((diff <= 0.011).all(), "margin: margin_sar = costed_revenue_sar - cost_sar")
    check((m["costed_revenue_sar"] <= m["revenue_sar"] + 0.01).all(), "margin: costed_revenue_sar <= revenue_sar")
    check((m["uncosted_lines"] <= m["lines"]).all(), "margin: uncosted_lines <= lines")
    db = args.data_dir / "qafila_oltp.sqlite"
    if db.exists():
        with closing(sqlite3.connect(db)) as con:
            n, rev = con.execute("""
                SELECT count(*), round(sum(round(i.quantity * i.unit_price - i.discount, 2)), 2)
                FROM order_items i JOIN orders o USING (order_id)
                WHERE o.status = 'delivered' AND i.quantity > 0
                  AND date(datetime(o.created_at, '+3 hours')) >= '2026-08-31'""").fetchone()
        check(int(m["lines"].sum()) == n and abs(m["revenue_sar"].sum() - rev) < 1,
              "margin: lines and revenue cover every eligible order line",
              f"expected {n} lines / {rev:,.2f} SAR, got {int(m['lines'].sum())} / {m['revenue_sar'].sum():,.2f}")

    # 5. Iceberg history and dbt tests
    snaps = len(cat.load_table("gold.weekly_margin").snapshots())
    check(snaps >= 1, f"Iceberg: gold.weekly_margin has {snaps} snapshot(s); unchanged inputs should keep this count unchanged")
    rr = ROOT / "dbt" / "target" / "run_results.json"
    if check(rr.exists(), "dbt: run_results.json exists", "run dbt build (run.py does)"):
        tests = [r for r in json.loads(rr.read_text(encoding="utf-8"))["results"] if r["unique_id"].startswith("test.")]
        failing = [r["unique_id"].split(".")[-2] for r in tests if r["status"] not in ("pass", "warn")]
        check(len(tests) >= 6, f"dbt: {len(tests)} tests (at least 6)")
        check(not failing, "dbt: every test passes", ", ".join(failing[:5]))

    print(f"\n{sum(results)}/{len(results)} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
