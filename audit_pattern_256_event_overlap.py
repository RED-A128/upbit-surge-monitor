
#!/usr/bin/env python3
"""Pattern 256 event overlap audit, Clean V001. Read-only input; no trading."""
import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = 'Clean V001'
DEFAULT_SOURCE = Path('data/reports/pattern_256_accumulation_candidate_independent_backtest/backtest_trades.csv')
DEFAULT_OUTPUT = Path('data/reports/pattern_256_event_overlap_audit')
REQUIRED = {'timeframe','market','signal_timestamp','classification','horizon','entry_timestamp','status','hit_30pct','observed_bars','required_bars'}
PERIOD = {'h1': timedelta(hours=1), 'h4': timedelta(hours=4), 'd1': timedelta(days=1)}


def timestamp(value):
    try:
        dt = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
        if dt.tzinfo is None:
            raise ValueError('timezone missing')
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError) as exc:
        raise ValueError(f'invalid UTC timestamp {value!r}: {exc}') from exc


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, obj):
    fd, name = tempfile.mkstemp(prefix='.tmp-', suffix='.json', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, sort_keys=True)
            f.write('\n')
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def atomic_csv(path, fields, rows):
    fd, name = tempfile.mkstemp(prefix='.tmp-', suffix='.csv', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main():
    parser = argparse.ArgumentParser(description='Read-only event-level overlap audit')
    parser.add_argument('--root', default='.')
    parser.add_argument('--source', default=str(DEFAULT_SOURCE))
    parser.add_argument('--output', default=str(DEFAULT_OUTPUT))
    parser.add_argument('--force', action='store_true', help='Recalculate, preserving source')
    args = parser.parse_args()
    root = Path(args.root).resolve()
    source = (root / args.source).resolve()
    output = (root / args.output).resolve()
    if not source.is_file():
        raise FileNotFoundError(f'CSV not found: {source}')
    if output == root or output == source.parent or output in source.parents or source in output.parents:
        raise ValueError('unsafe output directory')
    output.mkdir(parents=True, exist_ok=True)
    comparison_path = output / 'event_overlap_comparison.csv'
    result_path = output / 'event_overlap_result.json'
    manifest_path = output / 'event_overlap_manifest.json'
    if not args.force and all(p.is_file() for p in (comparison_path, result_path, manifest_path)):
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
            if (manifest.get('version') == VERSION
                and manifest.get('source_sha256') == digest(source)
                and all(manifest['output_sha256'][p.name] == digest(p) for p in (comparison_path, result_path))):
                print('RESUME: verified existing audit; SKIP')
                print(json.dumps(json.loads(result_path.read_text(encoding='utf-8')), indent=2))
                return
        except (ValueError, KeyError, OSError, json.JSONDecodeError):
            pass

    # One compact record per unique signal and horizon.
    # Large datasets may require substantial RAM.
    events = {}
    rows = 0
    duplicate_rows = 0
    inconsistent_rows = 0
    incomplete_rows = 0
    invalid_rows = 0
    by_status = Counter()
    with source.open('r', encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not REQUIRED.issubset(set(reader.fieldnames)):
            raise ValueError(f'Missing columns: {sorted(REQUIRED - set(reader.fieldnames or []))}')
        for line, row in enumerate(reader, 2):
            rows += 1
            if rows % 500000 == 0:
                print(f'Rows scanned: {rows:,}', flush=True)
            try:
                tf = row['timeframe'].strip()
                if tf not in PERIOD:
                    raise ValueError(f'unknown timeframe: {tf}')
                market = row['market'].strip()
                cls = row['classification'].strip()
                if not market or not cls:
                    raise ValueError('empty market or classification')
                signal = timestamp(row['signal_timestamp'])
                horizon = int(row['horizon'])
                if horizon <= 0:
                    raise ValueError('nonpositive horizon')
                status = row['status'].strip().upper()
                if status not in ('HIT', 'MISS', 'CENSORED'):
                    raise ValueError(f'unknown status {status}')
                key = (tf, market, signal, cls)
                event = events.get(key)
                if event is None:
                    event = {'timeframe':tf,'market':market,'signal':signal,'classification':cls,'horizons':{},'entry':None}
                    events[key] = event
                if horizon in event['horizons']:
                    duplicate_rows += 1
                    previous = event['horizons'][horizon]
                    if previous != (status, row['entry_timestamp'].strip(), row['hit_30pct'].strip(), row['observed_bars'].strip()):
                        inconsistent_rows += 1
                    continue
                event['horizons'][horizon] = (status, row['entry_timestamp'].strip(), row['hit_30pct'].strip(), row['observed_bars'].strip())
                by_status[status] += 1
                if status == 'CENSORED' or not row['entry_timestamp'].strip():
                    incomplete_rows += 1
                    continue
                entry = timestamp(row['entry_timestamp'])
                if entry < signal:
                    raise ValueError('entry precedes signal')
                if event['entry'] is not None and event['entry'] != entry:
                    inconsistent_rows += 1
                else:
                    event['entry'] = entry
            except (ValueError, TypeError) as exc:
                invalid_rows += 1
                if invalid_rows <= 10:
                    print(f'INVALID row {line}: {exc}', file=sys.stderr)

    # Calendar-based overlap between completed signal windows.
    import heapq
    grouped = defaultdict(list)
    event_counts = Counter()
    unique_signal_counts = Counter()
    horizon_multiplicity = Counter()
    for e in events.values():
        group = (e['timeframe'], e['classification'])
        event_counts[group] += 1
        horizon_multiplicity[len(e['horizons'])] += 1
        if e['entry'] is None:
            continue
        completed = [h for h, values in e['horizons'].items() if values[0] in ('HIT', 'MISS')]
        if not completed:
            continue
        end = e['entry'] + max(completed) * PERIOD[e['timeframe']]
        grouped[(e['timeframe'], e['market'])].append((e['entry'], end, e['classification']))
        unique_signal_counts[group] += 1

    overlap_counts = Counter()
    overlap_pairs = Counter()
    cross_class_overlap = Counter()
    for (tf, market), items in grouped.items():
        items.sort(key=lambda item: (item[0], item[1], item[2]))
        active = []
        active_cls = Counter()
        active_total = 0
        for start, end, cls in items:
            while active and active[0][0] <= start:
                _, old_cls = heapq.heappop(active)
                active_cls[old_cls] -= 1
                active_total -= 1
            if active_total:
                overlap_counts[(tf, cls)] += 1
                overlap_pairs[(tf, cls)] += active_total
                if active_total > active_cls[cls]:
                    cross_class_overlap[(tf, cls)] += 1
            heapq.heappush(active, (end, cls))
            active_cls[cls] += 1
            active_total += 1

    table = []
    for tf, cls in sorted(event_counts):
        n = unique_signal_counts[(tf, cls)]
        overlapping = overlap_counts[(tf, cls)]
        table.append({
            'timeframe':tf,
            'classification':cls,
            'unique_signals':event_counts[(tf, cls)],
            'evaluable_signals':n,
            'overlap_with_prior_signals':overlapping,
            'overlap_rate_pct':round(100 * overlapping / n, 6) if n else 0,
            'overlap_pairs_with_prior':overlap_pairs[(tf, cls)],
            'cross_class_overlap_signals':cross_class_overlap[(tf, cls)]
        })

    result = {
        'version':VERSION,
        'execution_status':'PASS' if not invalid_rows and not inconsistent_rows else 'REVIEW',
        'research_decision':'HOLD',
        'approved_for_live_trading':False,
        'statistical_independence_proven':False,
        'source_rows':rows,
        'unique_signal_events':len(events),
        'duplicate_horizon_rows':duplicate_rows,
        'inconsistent_rows':inconsistent_rows,
        'invalid_rows':invalid_rows,
        'incomplete_horizon_rows':incomplete_rows,
        'status_counts':dict(by_status),
        'horizons_per_signal':{str(k):v for k,v in sorted(horizon_multiplicity.items())},
        'groups':len(table),
        'overlap_definition':'Same market and timeframe, any classification, prior entry-to-max-completed-horizon interval overlaps current entry; calendar duration, not candle gap adjusted.',
        'caution':'Diagnostic only. Overlap counts depend on signal entry and theoretical horizon windows; does not prove independent trials, profitability or OOS validity.',
        'next_required_validation':'Event-level cluster-robust inference and prospective validation.'
    }
    atomic_csv(
        comparison_path,
        ['timeframe','classification','unique_signals','evaluable_signals',
         'overlap_with_prior_signals','overlap_rate_pct',
         'overlap_pairs_with_prior','cross_class_overlap_signals'],
        table
    )
    atomic_json(result_path, result)
    atomic_json(manifest_path, {
        'version':VERSION,
        'source_sha256':digest(source),
        'output_sha256':{
            p.name:digest(p) for p in (comparison_path,result_path)
        }
    })
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f'OUTPUT: {output}')
    if invalid_rows or inconsistent_rows:
        raise SystemExit('REVIEW: invalid or inconsistent rows; see result artifact')


if __name__ == '__main__':
    main()
