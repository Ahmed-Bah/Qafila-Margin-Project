"""Verify the submission before pushing it.

Run with: uv run python tools/verify_submission.py
Creates isolated data and warehouses under .verification/ and never pushes to Git.
The reference calculation uses Python, SQLite and Decimal, independently of dbt SQL.
"""

from __future__ import annotations

import argparse
import ast
from bisect import bisect_right
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CENT = Decimal('0.01')
START = date(2026, 8, 31)
COLUMNS = {
    'silver.product_cost_history': ['sku', 'unit_cost_sar', 'valid_from', 'valid_to', 'is_current'],
    'gold.weekly_margin': ['iso_week', 'city', 'category', 'lines', 'revenue_sar',
                         'costed_revenue_sar', 'cost_sar', 'margin_sar', 'uncosted_lines'],
    'quality.price_quarantine': ['source_file', 'sku', 'valid_from', 'raw_cost', 'reason'],
}
KEYS = {'silver.product_cost_history': ['sku', 'valid_from'],
        'gold.weekly_margin': ['iso_week', 'city', 'category']}
MONEY = {'unit_cost_sar', 'revenue_sar', 'costed_revenue_sar', 'cost_sar', 'margin_sar'}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def rounded(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def static_checks() -> dict:
    for file in [ROOT / 'run.py', *sorted((ROOT / 'tools').glob('*.py'))]:
        ast.parse(file.read_text(encoding='utf-8'), filename=str(file))
    tree = ast.parse((ROOT / 'run.py').read_text(encoding='utf-8'))
    functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
    require({'ingest', 'publish', 'get_catalog', 'transform'} <= functions, 'Required pipeline functions are missing')
    require(not any(isinstance(node, ast.Name) and node.id == 'NotImplementedError' for node in ast.walk(tree)),
            'run.py still has an unimplemented step')
    readme = (ROOT / 'README.md').read_text(encoding='utf-8')
    require('Submitted by: Ahmad Bahwerith' in readme, 'README submission name is missing')
    require(not re.search(r'<your name>|<2.?3 sentences|<None, or|<Show that|<A one-line', readme),
            'README still contains template placeholders')
    require('## AI assistance' in readme, 'README AI assistance statement is missing')
    sql_files = sorted((ROOT / 'dbt' / 'models').rglob('*.sql'))
    names = {file.stem for file in sql_files}
    require({'product_cost_history', 'weekly_margin', 'price_quarantine'} <= names, 'Required dbt models are missing')
    for file in sql_files:
        for target in re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", file.read_text(encoding='utf-8')):
            require(target in names, f'{file.name}: missing model {target}')
    tests = list((ROOT / 'dbt' / 'tests').glob('*.sql'))
    require(len(tests) >= 6, 'Fewer than six SQL tests are present')
    return {'python_syntax': 'pass', 'required_files_and_models': 'pass', 'sql_test_files': len(tests)}


def reference(data: Path) -> tuple[dict[str, list[dict]], dict]:
    """Apply the brief's rules directly to source files; no pipeline imports or SQL models."""
    with sqlite3.connect(f'file:{(data / "qafila_oltp.sqlite").as_posix()}?mode=ro', uri=True) as con:
        con.row_factory = sqlite3.Row
        products = {r['product_id']: dict(r) for r in con.execute('SELECT * FROM products')}
        stores = {r['store_id']: dict(r) for r in con.execute('SELECT * FROM stores')}
        orders = {r['order_id']: dict(r) for r in con.execute('SELECT * FROM orders')}
        items = [dict(row) for row in con.execute('SELECT * FROM order_items')]
    known_skus = {p['sku'].strip() for p in products.values()}
    digits = str.maketrans('\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669\u06f0\u06f1\u06f2\u06f3\u06f4\u06f5\u06f6\u06f7\u06f8\u06f9\u066b,', '01234567890123456789..')
    prices = {}
    quarantine = []
    for file in sorted((data / 'suppliers').glob('prices_*.csv'), key=lambda p: p.name):
        with file.open(encoding='utf-8-sig', newline='') as source:
            for row in csv.DictReader(source):
                sku = row['sku'].strip()
                valid_from = date.fromisoformat(row['valid_from'])
                raw = row.get('unit_cost') or row.get('cost_sar')
                try:
                    cost = Decimal(raw.strip().translate(digits)) if raw is not None else None
                except InvalidOperation:
                    cost = None
                reason = ('unknown_sku' if sku not in known_skus else
                          'invalid_cost' if cost is None or not cost.is_finite() or cost <= 0 else None)
                if reason:
                    quarantine.append({'source_file': file.name, 'sku': sku, 'valid_from': valid_from,
                                       'raw_cost': raw, 'reason': reason})
                else:
                    # Iterating sorted filenames and file rows implements both precedence rules.
                    prices[(sku, valid_from)] = rounded(cost)
    per_sku = defaultdict(list)
    for (sku, effective), cost in sorted(prices.items()):
        versions = per_sku[sku]
        if not versions or versions[-1][1] != cost:
            versions.append((effective, cost))
    history = []
    lookup = {}
    for sku, versions in per_sku.items():
        lookup[sku] = ([effective for effective, _ in versions], [cost for _, cost in versions])
        for index, (effective, cost) in enumerate(versions):
            end = versions[index + 1][0] if index + 1 < len(versions) else None
            history.append({'sku': sku, 'unit_cost_sar': cost, 'valid_from': effective,
                            'valid_to': end, 'is_current': end is None})
    groups = {}
    counts = Counter({'all_lines': len(items)})
    for item in items:
        order = orders[item['order_id']]
        if order['status'] != 'delivered':
            counts['non_delivered'] += 1
            continue
        if item['quantity'] <= 0:
            counts['nonpositive_delivered'] += 1
            continue
        utc = datetime.fromisoformat(order['created_at'])
        local_date = (utc + timedelta(hours=3)).date()
        if local_date < START:
            counts['before_start'] += 1
            continue
        counts['eligible'] += 1
        counts['different_riyadh_date'] += utc.date() != local_date
        counts['boundary_lines'] += utc.date() < START
        product = products[item['product_id']]
        key = (local_date.isocalendar().week, stores[order['store_id']]['city'], product['category'])
        if key not in groups:
            groups[key] = dict(zip(['iso_week', 'city', 'category'], key))
            groups[key].update(lines=0, revenue_sar=Decimal(0), costed_revenue_sar=Decimal(0),
                               cost_sar=Decimal(0), uncosted_lines=0)
        result = groups[key]
        revenue = rounded(Decimal(item['quantity']) * Decimal(str(item['unit_price'])) - Decimal(str(item['discount'])))
        result['lines'] += 1
        result['revenue_sar'] += revenue
        dates, costs = lookup.get(product['sku'].strip(), ([], []))
        position = bisect_right(dates, local_date) - 1
        if position < 0:
            result['uncosted_lines'] += 1
        else:
            result['costed_revenue_sar'] += revenue
            result['cost_sar'] += Decimal(item['quantity']) * costs[position]
    for result in groups.values():
        for column in ('revenue_sar', 'costed_revenue_sar', 'cost_sar'):
            result[column] = rounded(result[column])
        result['margin_sar'] = rounded(result['costed_revenue_sar'] - result['cost_sar'])
    return {'silver.product_cost_history': history, 'gold.weekly_margin': list(groups.values()),
            'quality.price_quarantine': quarantine}, dict(counts)


def fingerprint(rows: list[dict]) -> str:
    encoded = sorted(json.dumps(row, sort_keys=True, default=str, ensure_ascii=True) for row in rows)
    return hashlib.sha256('\n'.join(encoded).encode()).hexdigest()


def compare(name: str, actual: list[dict], expected: list[dict]) -> None:
    require(len(actual) == len(expected), f'{name}: expected {len(expected)} rows, got {len(actual)}')
    if name == 'quality.price_quarantine':
        as_counter = lambda rows: Counter(tuple(row[c] for c in COLUMNS[name]) for row in rows)
        require(as_counter(actual) == as_counter(expected), f'{name}: rejected rows differ from source reference')
        return
    key_columns = KEYS[name]
    keyed = lambda rows: {tuple(row[c] for c in key_columns): row for row in rows}
    left, right = keyed(actual), keyed(expected)
    require(len(left) == len(actual), f'{name}: duplicate keys')
    require(left.keys() == right.keys(), f'{name}: keys differ from source reference')
    for key, wanted in right.items():
        row = left[key]
        for column in COLUMNS[name]:
            if column in MONEY:
                value = Decimal(str(row[column]))
                tolerance = Decimal('0.00000001') if column == 'unit_cost_sar' else Decimal('0.02')
                require(value.is_finite() and abs(value - wanted[column]) <= tolerance,
                        f'{name} {key}: {column}: expected {wanted[column]}, got {row[column]}')
            else:
                require(row[column] == wanted[column], f'{name} {key}: {column} differs')


def command(arguments: list[str], log: Path) -> None:
    environment = {**os.environ, 'PYTHONUTF8': '1', 'PYTHONIOENCODING': 'utf-8', 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false'}
    result = subprocess.run([sys.executable, *arguments], cwd=ROOT, env=environment,
                            capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=900)
    log.write_text(result.stdout + '\n' + result.stderr, encoding='utf-8')
    if result.returncode:
        print((result.stdout + result.stderr)[-7000:])
    require(result.returncode == 0, f'Command failed: {arguments}. Log: {log}')


def inspect(warehouse: Path) -> dict:
    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog('qafila', uri=f'sqlite:///{(warehouse / "catalog.db").as_posix()}', warehouse=warehouse.as_posix())
    tables = {}
    for name, columns in COLUMNS.items():
        table = catalog.load_table(name)
        arrow = table.scan().to_arrow()
        require(set(columns) <= set(arrow.column_names), f'{name}: required columns missing')
        rows = arrow.select(columns).to_pylist()
        snapshot = table.current_snapshot()
        require(snapshot is not None, f'{name}: no snapshot')
        tables[name] = {'table': table, 'rows': rows, 'fingerprint': fingerprint(rows),
                        'snapshot': snapshot.snapshot_id, 'snapshots': [s.snapshot_id for s in table.snapshots()]}
    return tables


def check_dbt() -> int:
    result = json.loads((ROOT / 'dbt' / 'target' / 'run_results.json').read_text(encoding='utf-8'))
    tests = [r for r in result['results'] if r['unique_id'].startswith('test.')]
    require(len(tests) >= 6, 'Fewer than six dbt tests ran')
    failures = [r['unique_id'] for r in tests if r['status'] != 'pass']
    require(not failures, f'dbt tests did not pass: {failures}')
    return len(tests)


def run_seed(seed: int, workspace: Path) -> dict:
    data = workspace / f'data_{seed}'
    warehouse = workspace / f'warehouse_{seed}'
    require(not data.exists() and not warehouse.exists(), 'Verification requires fresh directories')
    print(f'Seed {seed}: generating fresh data', flush=True)
    command([str(ROOT / 'tools' / 'make_data.py'), '--seed', str(seed), '--out', str(data)], workspace / f'{seed}-generate.log')
    before_ref, counts = reference(data)
    runs = []
    states = []
    after_ref = None
    for number in range(1, 5):
        if number == 3:
            command([str(ROOT / 'tools' / 'make_data.py'), '--add-correction', '--out', str(data)], workspace / f'{seed}-correction.log')
            after_ref, _ = reference(data)
        expected = before_ref if number <= 2 else after_ref
        print(f'Seed {seed}: pipeline run {number}/4', flush=True)
        command([str(ROOT / 'run.py'), '--data-dir', str(data), '--warehouse', str(warehouse)], workspace / f'{seed}-run-{number}.log')
        test_count = check_dbt()
        command([str(ROOT / 'tools' / 'check.py'), '--data-dir', str(data), '--warehouse', str(warehouse)], workspace / f'{seed}-check-{number}.log')
        current = inspect(warehouse)
        for name in COLUMNS:
            compare(name, current[name]['rows'], expected[name])
            if number in (2, 4):
                previous = states[-1][name]
                require(current[name]['fingerprint'] == previous['fingerprint'], f'{name}: rerun changed content')
                require(current[name]['snapshots'] == previous['snapshots'], f'{name}: rerun added or changed snapshots')
            if number == 3:
                previous = states[0][name]
                changed = current[name]['fingerprint'] != previous['fingerprint']
                require(len(current[name]['snapshots']) == len(previous['snapshots']) + int(changed),
                        f'{name}: correction must add exactly one snapshot per changed table')
                old_rows = current[name]['table'].scan(snapshot_id=previous['snapshot']).to_arrow().select(COLUMNS[name]).to_pylist()
                require(fingerprint(old_rows) == previous['fingerprint'], f'{name}: time travel did not preserve the original data')
        runs.append({'run': number, 'dbt_tests_passed': test_count,
                     'tables': {name: {'rows': len(state['rows']), 'snapshot': state['snapshot'], 'snapshots': state['snapshots']}
                                for name, state in current.items()}})
        states.append(current)
        print(f'  PASS: {test_count} dbt tests, contract checks, all rows match source reference', flush=True)
    margin = lambda rows: str(sum((r['margin_sar'] for r in rows), Decimal(0)))
    before_margin = margin(before_ref['gold.weekly_margin'])
    after_margin = margin(after_ref['gold.weekly_margin'])
    if seed == 2026:
        require(before_margin == '801016.88' and after_margin == '803515.80', 'README margin totals differ from the source reference')
        require(len(before_ref['silver.product_cost_history']) == 633 and len(after_ref['silver.product_cost_history']) == 637,
                'README cost-history counts differ from source reference')
    return {'seed': seed, 'source_counts': counts, 'before_margin_sar': before_margin,
            'after_margin_sar': after_margin, 'rerun_idempotence': 'pass', 'time_travel': 'pass', 'runs': runs}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seeds', nargs='+', type=int, default=[2026, 11])
    parser.add_argument('--static-only', action='store_true')
    parser.add_argument('--reference-only', type=Path, help='Calculate the independent reference for an existing dataset; does not run the pipeline')
    args = parser.parse_args()
    summary = {'static_checks': static_checks()}
    if args.reference_only:
        outputs, counts = reference(args.reference_only.resolve())
        summary.update(source_counts=counts, output_rows={name: len(rows) for name, rows in outputs.items()},
                       margin_sar=str(sum((row['margin_sar'] for row in outputs['gold.weekly_margin']), Decimal(0))))
        print(json.dumps(summary, indent=2))
        return
    if args.static_only:
        print(json.dumps(summary, indent=2))
        return
    base = (ROOT / '.verification').resolve()
    base.mkdir(exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix='run-', dir=base)).resolve()
    require(workspace.parent == base, 'Verification output must stay inside .verification')
    print(f'Verification logs: {workspace}', flush=True)
    summary['seeds'] = [run_seed(seed, workspace) for seed in dict.fromkeys(args.seeds)]
    summary['status'] = 'PASS'
    report = workspace / 'verification-report.json'
    report.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(f'PASS: all verification checks completed. Report: {report}', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'VERIFICATION FAILED: {error}', file=sys.stderr)
        sys.exit(1)
