#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pattern 256 TRAIN-only feature join, Clean V001. Read-only sources."""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = 'Clean V001'
TF_SECONDS = {'h1': 3600, 'h4': 14400, 'd1': 86400}
BASE_REQUIRED = ('rsi_14', 'sma_20', 'sma_60', 'volume_ratio_20', 'atr_14')
CAND_REQUIRED = ('relative_volume_20', 'relative_trade_value_20', 'atr_ratio_20', 'ma_20', 'ma_60', 'trend_alignment', 'distance_to_resistance_20_pct')
EVENT_FIELDS = ('timeframe', 'market', 'signal_timestamp', 'classification', 'horizon', 'entry_timestamp', 'entry_price', 'target_price', 'status', 'hit_30pct', 'outcome_maturity_timestamp')
OUTPUT_FIELDS = list(EVENT_FIELDS) + ['feature_candle_timestamp', 'feature_candle_end_timestamp'] + ['base_' + k for k in BASE_REQUIRED] + ['candidate_' + k for k in CAND_REQUIRED]


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1048576), b''):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with open(path, encoding='utf-8-sig') as f:
        return json.load(f)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.tmp_', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(value, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write('\n')
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def epoch(text):
    dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('Naive timestamp: ' + text)
    return int(dt.timestamp())


def to_epoch_millis(text):
    return epoch(text) * 1000


def header(path, required):
    with open(path, newline='', encoding='utf-8-sig') as f:
        names = next(csv.reader(f), [])
    if len(names) != len(set(names)) or not set(required).issubset(names):
        raise ValueError('Invalid CSV header: ' + str(path))
    return names


def csv_rows(path):
    with open(path, newline='', encoding='utf-8-sig') as f:
        yield from csv.DictReader(f)


def feature_map(path, expected_market, tf, required, cutoff):
    header(path, ('market', 'timestamp', 'candle_date_time_utc', *required))
    step = TF_SECONDS[tf] * 1000
    result = {}
    previous = None
    run = 0
    for row in csv_rows(path):
        if row['market'] != expected_market:
            raise ValueError('Feature market mismatch: ' + str(path))
        t = int(row['timestamp'])
        if t != to_epoch_millis(row['candle_date_time_utc']):
            raise ValueError('Feature timestamp mismatch: ' + str(path))
        if previous is not None and t <= previous:
            raise ValueError('Feature timestamps not strictly ascending: ' + str(path))
        run = run + 1 if previous is not None and t - previous == step else 1
        previous = t
        if t >= cutoff:
            continue
        result[t] = (row, run)
    return result


def make_index(events, db_path, cutoff):
    conn = sqlite3.connect(str(db_path))
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.execute('PRAGMA synchronous=FULL')
    conn.execute('CREATE TABLE events (tf TEXT, market TEXT, signal_ms INTEGER, entry_ms INTEGER, payload TEXT)')
    count = 0
    batch = []
    for row in csv_rows(events):
        tf = row['timeframe']
        if tf not in TF_SECONDS:
            raise ValueError('Unknown timeframe: ' + tf)
        if row['status'] not in ('HIT', 'MISS') or row['hit_30pct'] not in ('0', '1'):
            raise ValueError('Invalid TRAIN outcome')
        if (row['status'] == 'HIT') != (row['hit_30pct'] == '1'):
            raise ValueError('Outcome inconsistency')
        signal = to_epoch_millis(row['signal_timestamp'])
        entry = to_epoch_millis(row['entry_timestamp'])
        maturity = to_epoch_millis(row['outcome_maturity_timestamp'])
        if not (signal < entry <= maturity < cutoff):
            raise ValueError('TRAIN boundary or timestamp violation')
        if row['classification'] not in ('BOTH', 'PATTERN_256_ONLY', 'ACCUMULATION_ONLY', 'NEITHER'):
            raise ValueError('Unknown classification')
        if int(row['horizon']) not in (24, 72, 168):
            raise ValueError('Unexpected horizon')
        batch.append((tf, row['market'], signal, entry, json.dumps(row, ensure_ascii=False)))
        count += 1
        if len(batch) == 10000:
            conn.executemany('INSERT INTO events VALUES (?,?,?,?,?)', batch)
            conn.commit()
            batch.clear()
        if count % 500000 == 0:
            print(f'[INDEX] {count:,}', flush=True)
    if batch:
        conn.executemany('INSERT INTO events VALUES (?,?,?,?,?)', batch)
        conn.commit()
    conn.execute('CREATE INDEX event_lookup ON events(tf,market,signal_ms)')
    conn.commit()
    return conn, count


def valid_number(text):
    try:
        v = float(text)
        return v == v and abs(v) != float('inf')
    except (ValueError, TypeError):
        return False


def process_pair(conn, tf, market, base_path, cand_path, output_path, cutoff):
    base = feature_map(base_path, market, tf, BASE_REQUIRED, cutoff)
    cand = feature_map(cand_path, market, tf, CAND_REQUIRED, cutoff)
    counters = Counter()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix('.partial')
    try:
        with open(tmp, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
            writer.writeheader()
            cursor = conn.execute('SELECT signal_ms, entry_ms, payload FROM events WHERE tf=? AND market=? ORDER BY signal_ms', (tf, market))
            for signal, entry, payload in cursor:
                counters['events'] += 1
                b = base.get(signal)
                c = cand.get(signal)
                if b is None or c is None:
                    counters['missing_exact_timestamp'] += 1
                    continue
                if b[1] < 120 or c[1] < 120:
                    counters['insufficient_contiguous_history'] += 1
                    continue
                candle_end = signal + TF_SECONDS[tf] * 1000
                if candle_end > entry:
                    counters['future_candle_risk'] += 1
                    continue
                if any(not valid_number(b[0][k]) for k in BASE_REQUIRED) or any(not valid_number(c[0][k]) for k in CAND_REQUIRED):
                    counters['missing_required_features'] += 1
                    continue
                event = json.loads(payload)
                event['feature_candle_timestamp'] = datetime.fromtimestamp(signal / 1000, timezone.utc).isoformat()
                event['feature_candle_end_timestamp'] = datetime.fromtimestamp(candle_end / 1000, timezone.utc).isoformat()
                event.update({'base_' + k: b[0][k] for k in BASE_REQUIRED})
                event.update({'candidate_' + k: c[0][k] for k in CAND_REQUIRED})
                writer.writerow(event)
                counters['joined'] += 1
        os.replace(tmp, output_path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return dict(counters)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--rebuild', action='store_true')
    args = parser.parse_args()
    root = Path(args.root).resolve()
    report = root / 'data/reports/pattern_256_feature_join_schema'
    train = root / 'data/reports/pattern_256_train_only'
    out = root / 'data/reports/pattern_256_train_feature_join'
    event_path = train / 'train_only_events.csv'
    train_result_path = train / 'train_only_result.json'
    train_manifest_path = train / 'train_only_manifest.json'
    schema_path = report / 'feature_join_schema_result.json'
    schema_manifest_path = report / 'feature_join_schema_manifest.json'
    for p in (event_path, train_result_path, train_manifest_path, schema_path, schema_manifest_path):
        if not p.is_file():
            raise FileNotFoundError(p)
    header(event_path, EVENT_FIELDS)
    tr = read_json(train_result_path)
    tm = read_json(train_manifest_path)
    sr = read_json(schema_path)
    sm = read_json(schema_manifest_path)
    if tr.get('execution_status') != 'PASS' or tr.get('train_dataset_prepared') is not True:
        raise ValueError('TRAIN prerequisite failed')
    if sr.get('execution_status') != 'PASS' or sr.get('feature_schema_variants') != 2:
        raise ValueError('Schema prerequisite failed')
    for name, path in [('train_only_events.csv', event_path), ('train_only_result.json', train_result_path)]:
        if digest(path) != tm['output_sha256'][name]:
            raise ValueError('TRAIN SHA256 mismatch: ' + name)
    if digest(schema_path) != sm['output_sha256']['feature_join_schema_result.json']:
        raise ValueError('Schema SHA256 mismatch')
    cutoff = to_epoch_millis(tr['train_cutoff'])
    if tr['train_cutoff'] != sr['train_cutoff']:
        raise ValueError('Cutoff mismatch')
    out.mkdir(parents=True, exist_ok=True)
    shards = out / 'shards'
    shards.mkdir(exist_ok=True)
    index_db = out / 'train_event_index.sqlite'
    index_meta = out / 'train_event_index.json'
    source_hash = digest(event_path)
    if index_db.exists() and index_meta.exists() and not args.rebuild:
        meta = read_json(index_meta)
        if meta.get('source_sha256') != source_hash or meta.get('cutoff') != cutoff or meta.get('db_sha256') != digest(index_db):
            raise ValueError('Index checksum invalid; rerun --rebuild')
        conn = sqlite3.connect(str(index_db))
        indexed = conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
        if indexed != meta['rows']:
            raise ValueError('Index row count mismatch')
        print('[INDEX] verified resume', indexed, flush=True)
    else:
        if index_db.exists():
            index_db.unlink()
        if index_meta.exists():
            index_meta.unlink()
        conn, indexed = make_index(event_path, index_db, cutoff)
        if indexed != tr['selected_rows']:
            raise ValueError(f'Indexed rows mismatch: {indexed}')
        conn.close()
        write_json(index_meta, {'source_sha256': source_hash, 'db_sha256': digest(index_db), 'cutoff': cutoff, 'rows': indexed})
        conn = sqlite3.connect(str(index_db))
    pairs = conn.execute('SELECT DISTINCT tf,market FROM events ORDER BY tf,market').fetchall()
    summary = Counter()
    completed = []
    try:
        for i, (tf, market) in enumerate(pairs, 1):
            if '/' in market or '\\' in market or '..' in market:
                raise ValueError('Unsafe market name')
            base = root / 'data/features' / tf / (market + '.csv')
            cand = root / 'data/features/pattern_256_candidates' / tf / (market + '.csv')
            shard = shards / tf / (market + '.csv')
            state = shards / tf / (market + '.json')
            if not base.is_file() or not cand.is_file():
                # Fail closed: missing source is not silently accepted.
                raise FileNotFoundError(f'Missing feature source for {tf} {market}')
            bh, ch = digest(base), digest(cand)
            if state.is_file() and shard.is_file() and not args.rebuild:
                saved = read_json(state)
                if saved.get('base_sha256') == bh and saved.get('candidate_sha256') == ch and saved.get('event_sha256') == source_hash and saved.get('output_sha256') == digest(shard) and saved.get('version') == VERSION:
                    counts = saved['counts']
                    summary.update(counts)
                    completed.append({'timeframe': tf, 'market': market, 'sha256': saved['output_sha256'], 'counts': counts})
                    if i % 25 == 0:
                        print(f'[JOIN] {i}/{len(pairs)} RESUME', flush=True)
                    continue
            counts = process_pair(conn, tf, market, base, cand, shard, cutoff)
            sha = digest(shard)
            write_json(state, {'version': VERSION, 'base_sha256': bh, 'candidate_sha256': ch, 'event_sha256': source_hash, 'output_sha256': sha, 'counts': counts})
            summary.update(counts)
            completed.append({'timeframe': tf, 'market': market, 'sha256': sha, 'counts': counts})
            if i % 25 == 0 or i == len(pairs):
                print(f'[JOIN] {i}/{len(pairs)} JOINED={summary["joined"]:,}', flush=True)
    finally:
        conn.close()
    if summary['events'] != indexed:
        raise ValueError('Event conservation failed')
    if summary['joined'] + sum(v for k, v in summary.items() if k not in ('events', 'joined')) != indexed:
        raise ValueError('Outcome conservation failed')
    result = {
        'version': VERSION, 'execution_status': 'PASS', 'source_train_rows': indexed,
        'joined_rows': summary['joined'], 'exclusions': {k: v for k, v in summary.items() if k not in ('events', 'joined')},
        'pair_count': len(pairs), 'train_cutoff': tr['train_cutoff'],
        'shards': completed, 'join_completed': True,
        'train_experiment_completed': False, 'research_decision': 'HOLD',
        'independent_oos_proven': False, 'profitability_proven': False,
        'approved_for_live_trading': False, 'automated_trading_enabled': False,
        'limitations': [
            '120 consecutive bars and required feature completeness are conservative row-level proxies, not a reproduction of the original integrity audit eligibility flags.',
            'Only selected feature columns are exported.',
            'This join does not establish statistical independence or OOS performance.',
            'Signal candle must be fully closed by the recorded entry timestamp.'
        ],
        'next_stage': 'TRAIN_ONLY_EXPERIMENT_DESIGN_AND_LABEL_DEPENDENCE_AUDIT'
    }
    result_path = out / 'train_feature_join_result.json'
    manifest_path = out / 'train_feature_join_manifest.json'
    write_json(result_path, result)
    write_json(manifest_path, {
        'version': VERSION,
        'inputs_sha256': {'train_only_events.csv': source_hash, 'train_only_result.json': digest(train_result_path), 'feature_join_schema_result.json': digest(schema_path)},
        'outputs_sha256': {'train_feature_join_result.json': digest(result_path)},
        'shard_count': len(completed), 'joined_rows': summary['joined']
    })
    if digest(result_path) != read_json(manifest_path)['outputs_sha256']['train_feature_join_result.json']:
        raise ValueError('Output SHA256 mismatch')
    print(f'PASS: {summary["joined"]:,}/{indexed:,} rows joined. Decision HOLD.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'FATAL: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)
