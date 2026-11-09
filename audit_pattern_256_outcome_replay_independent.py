
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pattern 256 independent +30% outcome replay, Clean V001.

Diagnostic hypotheses only. Does not authorize training or trading.
Reads source OHLCV without changing it.
"""
import argparse
import csv
import hashlib
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = 'Clean V001'
AUDIT = 'pattern_256_outcome_replay_independent'
STEPS = {'h1': 3600, 'h4': 14400, 'd1': 86400}
TIMES = ('timestamp', 'utc_time', 'datetime', 'date',
         'candle_date_time_utc', 'time', 'ts')
PRICES = {
    'open': ('open', 'opening_price'),
    'high': ('high', 'high_price'),
    'low': ('low', 'low_price'),
    'close': ('close', 'trade_price', 'closing_price')
}
HYPOTHESES = (
    ('A_CLOSE_INCLUDE', 'entry_close', 0, False),
    ('B_CLOSE_EXCLUDE', 'entry_close', 1, True),
    ('C_NEXT_OPEN_INCLUDE', 'next_open', 0, False),
    ('D_NEXT_OPEN_EXCLUDE', 'next_open', 1, True),
)
FIELDS = [
    'source_index', 'source', 'market', 'timeframe',
    'classification', 'window_status', 'entry_epoch',
    'horizon_bars', 'hypothesis', 'entry_price',
    'target_price', 'observed_bars', 'missing_bars',
    'first_hit_offset', 'first_hit_bar_1based',
    'first_hit_utc', 'max_return_pct', 'status', 'warning'
]


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1048576), b''):
            h.update(block)
    return h.hexdigest()


def read_json(path):
    with path.open(encoding='utf-8-sig') as f:
        return json.load(f)


def write_json(path, obj):
    with path.open('w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, sort_keys=True, indent=2)
        f.write('\n')


def select(names, aliases):
    found = {x.strip().lower(): x for x in names}
    for alias in aliases:
        if alias in found:
            return found[alias]
    raise ValueError('Required column absent: ' + '/'.join(aliases))


def to_epoch(raw):
    s = str(raw).strip()
    try:
        n = float(s)
        if not math.isfinite(n):
            raise ValueError('Nonfinite timestamp')
        if abs(n) >= 1e14:
            n /= 1e6
        elif abs(n) >= 1e11:
            n /= 1e3
        return int(n)
    except ValueError:
        d = datetime.fromisoformat(s.replace('Z', '+00:00'))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return int(d.timestamp())


def load_ohlcv(root, tf, market):
    if tf not in STEPS or not market.startswith('KRW-') or any(
        x in market for x in ('/', '\\', '..')
    ):
        raise ValueError('Invalid market/timeframe')

    choices = [
        root / 'data' / 'ohlcv' / tf / (market + '.csv'),
        root / 'data' / 'raw' / tf / (market + '.csv')
    ]
    path = next((p for p in choices if p.is_file()), None)
    if path is None:
        raise FileNotFoundError(
            'OHLCV missing: ' + tf + '/' + market
        )

    out = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError('Empty OHLCV file')
        tcol = select(reader.fieldnames, TIMES)
        cols = {
            k: select(reader.fieldnames, aliases)
            for k, aliases in PRICES.items()
        }
        for line, row in enumerate(reader, 2):
            t = to_epoch(row[tcol])
            if t in out:
                raise ValueError(
                    f'Duplicate OHLCV timestamp at line {line}'
                )
            out[t] = {
                k: float(row[v]) for k, v in cols.items()
            }
    return out, path


def valid_candle(c):
    if c is None:
        return False
    o, h, l, cl = (
        c[k] for k in ('open', 'high', 'low', 'close')
    )
    return (
        all(
            math.isfinite(x) and x > 0
            for x in (o, h, l, cl)
        )
        and l <= min(o, cl) <= max(o, cl) <= h
    )


def inspect_event(row, root, cache, target_pct):
    tf, market = row['timeframe'], row['market']
    start = int(row['entry_epoch'])
    horizon = int(row['expected_bars'])

    if horizon < 1 or horizon > 100000:
        raise ValueError('Invalid horizon')

    key = (tf, market)
    if key not in cache:
        cache[key] = load_ohlcv(root, tf, market)

    candles, _ = cache[key]
    step = STEPS[tf]
    entry = candles.get(start)
    nxt = candles.get(start + step)

    if not valid_candle(entry):
        raise ValueError('Entry candle missing/invalid')

    if row['coverage_reconciled'].lower() != 'true':
        raise ValueError('Previous coverage mismatch')

    if row['price_errors'].strip() not in ('', '{}'):
        raise ValueError('Previous price integrity error')

    complete = (
        row['window_status'] == 'PRICE_WINDOW_COMPLETE'
    )

    if complete and not valid_candle(nxt):
        raise ValueError(
            'Complete window lacks valid next candle'
        )

    results = []

    for label, basis, shift, causal in HYPOTHESES:
        price = (
            entry['close']
            if basis == 'entry_close'
            else nxt['open'] if nxt is not None else None
        )

        times = [
            start + (i + shift) * step
            for i in range(horizon)
        ]
        available = [candles.get(t) for t in times]
        missing = sum(c is None for c in available)
        invalid = sum(
            c is not None and not valid_candle(c)
            for c in available
        )

        base = {
            'source_index': row['index'],
            'source': row['source'],
            'market': market,
            'timeframe': tf,
            'classification': row['classification'],
            'window_status': row['window_status'],
            'entry_epoch': start,
            'horizon_bars': horizon,
            'hypothesis': label,
            'entry_price': (
                price if price is not None else ''
            ),
            'target_price': '',
            'observed_bars': horizon - missing,
            'missing_bars': missing,
            'first_hit_offset': '',
            'first_hit_bar_1based': '',
            'first_hit_utc': '',
            'max_return_pct': '',
            'status': '',
            'warning': (
                '' if causal
                else 'NONCAUSAL_DIAGNOSTIC_ONLY'
            )
        }

        if (
            price is None
            or not math.isfinite(price)
            or price <= 0
        ):
            base['status'] = 'ENTRY_PRICE_UNAVAILABLE'

        elif invalid:
            base['status'] = 'INVALID_OHLCV'

        elif not complete or missing:
            base['status'] = 'CENSORED_INCOMPLETE_WINDOW'

        else:
            target = price * (1.0 + target_pct / 100.0)
            hits = [
                i for i, c in enumerate(available)
                if c['high'] >= target
            ]
            first = hits[0] if hits else None

            base['target_price'] = round(target, 12)
            base['max_return_pct'] = round(
                (
                    max(c['high'] for c in available)
                    / price - 1
                ) * 100, 8
            )
            base['status'] = (
                'HIT' if first is not None else 'NO_HIT'
            )

            if first is not None:
                base['first_hit_offset'] = first + shift
                base['first_hit_bar_1based'] = first + 1
                base['first_hit_utc'] = (
                    datetime.fromtimestamp(
                        times[first],
                        tz=timezone.utc
                    ).isoformat()
                )

        results.append(base)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument(
        '--target-pct', type=float, default=30.0
    )
    args = parser.parse_args()

    if (
        not math.isfinite(args.target_pct)
        or args.target_pct <= 0
    ):
        raise ValueError('Invalid target percentage')

    root = Path(args.root).resolve()
    source = Path(args.input).resolve()
    out = Path(args.output).resolve()

    names = [
        'entry_price_window_detail.csv',
        'entry_price_window_result.json',
        'entry_price_window_manifest.json'
    ]
    files = {n: source / n for n in names}

    if not all(f.is_file() for f in files.values()):
        raise FileNotFoundError(
            'Missing entry price/window artifact files'
        )

    prior = read_json(files[names[1]])
    manifest = read_json(files[names[2]])

    if (
        prior.get('audit')
        != 'pattern_256_entry_price_window'
        or prior.get('execution') != 'PASS'
        or manifest.get('execution') != 'PASS'
        or prior.get('coverage_consistent') is not True
        or prior.get('price_integrity_verified') is not True
    ):
        raise ValueError('Unqualified prior audit')

    for name in names[:2]:
        expected = (
            manifest.get('output_sha256', {})
            .get(name, '').lower()
        )
        if (
            not expected
            or digest(files[name]) != expected
        ):
            raise ValueError(
                'SHA256 mismatch: ' + name
            )

    with files[names[0]].open(
        encoding='utf-8-sig', newline=''
    ) as f:
        rows = list(csv.DictReader(f))

    if len(rows) != prior.get('processed_rows'):
        raise ValueError('Source row count mismatch')

    cache = {}
    results = []
    failures = []

    for number, row in enumerate(rows, 1):
        try:
            results.extend(
                inspect_event(
                    row, root, cache, args.target_pct
                )
            )
        except Exception as exc:
            failures.append({
                'row': number,
                'market': row.get('market'),
                'error': str(exc)
            })

            for label, _, _, _ in HYPOTHESES:
                item = {k: '' for k in FIELDS}
                item.update({
                    'source_index': row.get('index', ''),
                    'market': row.get('market', ''),
                    'timeframe': row.get('timeframe', ''),
                    'hypothesis': label,
                    'status': 'ERROR',
                    'warning': str(exc)
                })
                results.append(item)

    out.mkdir(parents=True, exist_ok=True)

    detail = out / 'independent_outcome_detail.csv'
    with detail.open(
        'w', encoding='utf-8', newline=''
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=FIELDS
        )
        writer.writeheader()
        writer.writerows(results)

    status = Counter(
        (r['hypothesis'], r['status'])
        for r in results
    )

    per_hypothesis = {}
    for label, _, _, _ in HYPOTHESES:
        per_hypothesis[label] = {
            key: status[(label, key)]
            for key in (
                'HIT',
                'NO_HIT',
                'CENSORED_INCOMPLETE_WINDOW',
                'ENTRY_PRICE_UNAVAILABLE',
                'INVALID_OHLCV',
                'ERROR'
            )
        }

    complete_count = sum(
        r['window_status'] == 'PRICE_WINDOW_COMPLETE'
        for r in rows
    )
    incomplete_count = len(rows) - complete_count

    success = (
        not failures
        and len(results) == len(rows) * len(HYPOTHESES)
        and all(
            per_hypothesis[h]['HIT']
            + per_hypothesis[h]['NO_HIT']
            == complete_count
            for h in per_hypothesis
        )
        and all(
            per_hypothesis[h][
                'CENSORED_INCOMPLETE_WINDOW'
            ]
            + per_hypothesis[h][
                'ENTRY_PRICE_UNAVAILABLE'
            ]
            == incomplete_count
            for h in per_hypothesis
        )
    )

    summary = {
        'audit': AUDIT,
        'version': VERSION,
        'created_utc': datetime.now(
            timezone.utc
        ).isoformat(),
        'execution': (
            'PASS' if success else 'FAIL'
        ),
        'target_return_pct': args.target_pct,
        'source_events': len(rows),
        'complete_events': complete_count,
        'incomplete_events': incomplete_count,
        'hypothesis_rows': len(results),
        'ohlcv_files_loaded': len(cache),
        'hypotheses': per_hypothesis,
        'errors': failures[:100],
        'original_entry_convention_proven': False,
        'horizon_unit_proven': False,
        'independent_oos_proven': False,
        'trading_profitability_proven': False,
        'train_approved': False,
        'live_trading_approved': False,
        'research_decision': 'HOLD',
        'limitations': [
            'Uses prior supplied event timestamp and horizon',
            'A and C use pre-entry high and are noncausal diagnostics',
            'B and D are hypothetical and do not prove actual fills',
            'Incomplete windows are censored, not negative outcomes',
            'No original reported hit comparison or OOS proof',
            'No fees, slippage, liquidity or executable fills'
        ]
    }

    result_file = (
        out / 'independent_outcome_result.json'
    )
    write_json(result_file, summary)

    output_manifest = {
        'audit': AUDIT,
        'version': VERSION,
        'execution': summary['execution'],
        'research_decision': 'HOLD',
        'script_sha256': digest(
            Path(__file__).resolve()
        ),
        'input_sha256': {
            name: digest(file)
            for name, file in files.items()
        },
        'output_sha256': {
            detail.name: digest(detail),
            result_file.name: digest(result_file)
        }
    }

    write_json(
        out / 'independent_outcome_manifest.json',
        output_manifest
    )

    print('Execution:', summary['execution'])
    print(
        'Events:', len(rows),
        'Complete:', complete_count,
        'Incomplete:', incomplete_count
    )
    for label, counts in per_hypothesis.items():
        print(label, counts)
    print('Research decision: HOLD')

    return 0 if success else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print('[FATAL]', exc, file=sys.stderr)
        sys.exit(1)
