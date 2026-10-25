#!/usr/bin/env python3
"""Pattern 256 feature point-in-time and coverage audit (Clean V001).

Read-only audit of TRAIN-only events and joined feature shards. Never changes source.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = 'Clean V001'
TRAIN_COLUMNS = ('timeframe','market','signal_timestamp','classification','horizon','entry_timestamp','entry_price','target_price','status','hit_30pct','outcome_maturity_timestamp')
FEATURE_COLUMNS = ('feature_candle_timestamp','feature_candle_end_timestamp','base_rsi_14','base_sma_20','base_sma_60','base_volume_ratio_20','base_atr_14','candidate_relative_volume_20','candidate_relative_trade_value_20','candidate_atr_ratio_20','candidate_ma_20','candidate_ma_60','candidate_trend_alignment','candidate_distance_to_resistance_20_pct')
KEY_COLUMNS = ('timeframe','market','signal_timestamp','classification','horizon','entry_timestamp')


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def timestamp(value: str) -> datetime:
    if not value:
        raise ValueError('empty timestamp')
    dt = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError('timezone missing')
    return dt.astimezone(timezone.utc)


def atomic_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name+'.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, sort_keys=True)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def atomic_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name+'.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def require_columns(actual: list[str] | None, required: tuple[str, ...], path: Path) -> None:
    missing = set(required) - set(actual or [])
    if missing:
        raise ValueError(f'{path}: missing columns: {sorted(missing)}')


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--root', default='.')
    p.add_argument('--max-issues', type=int, default=1000)
    args = p.parse_args()
    if args.max_issues < 0:
        p.error('--max-issues must be >= 0')

    root = Path(args.root).resolve()
    reports = root / 'data' / 'reports'
    train = reports / 'pattern_256_train_only' / 'train_only_events.csv'
    join_dir = reports / 'pattern_256_train_feature_join'
    integrity_dir = reports / 'pattern_256_feature_join_integrity'
    output_dir = reports / 'pattern_256_feature_point_in_time'
    join_result_path = join_dir / 'train_feature_join_result.json'
    join_manifest_path = join_dir / 'train_feature_join_manifest.json'
    integrity_result_path = integrity_dir / 'feature_join_integrity_result.json'
    integrity_manifest_path = integrity_dir / 'feature_join_integrity_manifest.json'
    inputs = [train, join_result_path, join_manifest_path, integrity_result_path, integrity_manifest_path]
    for path in inputs:
        if not path.is_file():
            raise FileNotFoundError(f'Required source file missing: {path}')
    shards = sorted((join_dir / 'shards').glob('*/*.csv'))
    if not shards:
        raise FileNotFoundError(f'No joined feature shards in {join_dir / "shards"}')

    with join_result_path.open(encoding='utf-8') as f:
        join_result = json.load(f)
    with join_manifest_path.open(encoding='utf-8') as f:
        join_manifest = json.load(f)
    with integrity_result_path.open(encoding='utf-8') as f:
        integrity_result = json.load(f)
    with integrity_manifest_path.open(encoding='utf-8') as f:
        integrity_manifest = json.load(f)
    if not isinstance(join_manifest.get('inputs_sha256'), dict) or not isinstance(join_manifest.get('outputs_sha256'), dict):
        raise ValueError('JOIN manifest SHA256 dictionaries missing')
    if not isinstance(integrity_manifest.get('input_sha256'), dict) or not isinstance(integrity_manifest.get('output_sha256'), dict):
        raise ValueError('Integrity manifest SHA256 dictionaries missing')

    counts = Counter()
    issues = []
    coverage = defaultdict(Counter)
    schema_set = set()
    input_hashes = {}
    for path in inputs:
        input_hashes[str(path.relative_to(root)).replace('\\','/')] = digest(path)

    # Key-count reconciliation avoids holding 4M full TRAIN rows in memory.
    train_keys = Counter()
    with train.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        require_columns(reader.fieldnames, TRAIN_COLUMNS, train)
        for row in reader:
            counts['train_rows'] += 1
            key = tuple(row[c] for c in KEY_COLUMNS)
            train_keys[key] += 1
            coverage[(row['timeframe'], row['classification'])]['train_rows'] += 1
            if counts['train_rows'] % 500000 == 0:
                print(f'[TRAIN] {counts["train_rows"]:,}', flush=True)

    for path in shards:
        counts['shard_files'] += 1
        with path.open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            require_columns(reader.fieldnames, TRAIN_COLUMNS + FEATURE_COLUMNS, path)
            schema_set.add(tuple(reader.fieldnames or []))
            for line_no, row in enumerate(reader, start=2):
                counts['joined_rows'] += 1
                group = coverage[(row['timeframe'], row['classification'])]
                group['joined_rows'] += 1
                key = tuple(row[c] for c in KEY_COLUMNS)
                if train_keys[key] <= 0:
                    counts['unmatched_join_rows'] += 1
                    if len(issues) < args.max_issues:
                        issues.append({'issue':'UNMATCHED_JOIN_KEY','file':str(path.relative_to(root)).replace('\\','/'),'line':line_no,'market':row['market'],'signal_timestamp':row['signal_timestamp']})
                else:
                    train_keys[key] -= 1
                try:
                    start = timestamp(row['feature_candle_timestamp'])
                    end = timestamp(row['feature_candle_end_timestamp'])
                    signal = timestamp(row['signal_timestamp'])
                    entry = timestamp(row['entry_timestamp'])
                    if start >= end:
                        counts['invalid_candle_window'] += 1
                        kind = 'INVALID_CANDLE_WINDOW'
                    elif end > entry:
                        counts['feature_after_entry'] += 1
                        kind = 'FEATURE_AFTER_ENTRY'
                    elif end > signal:
                        # Signal timestamp may be the OPEN of the signal candle.
                        # Such rows are flagged for convention review, not silently accepted.
                        counts['feature_after_signal'] += 1
                        kind = 'FEATURE_AFTER_SIGNAL_REVIEW'
                    else:
                        counts['feature_available_at_signal'] += 1
                        kind = ''
                except (ValueError, OverflowError) as exc:
                    counts['invalid_timestamps'] += 1
                    kind = 'INVALID_TIMESTAMP'
                if kind and len(issues) < args.max_issues:
                    issues.append({'issue':kind,'file':str(path.relative_to(root)).replace('\\','/'),'line':line_no,'market':row['market'],'signal_timestamp':row['signal_timestamp'],'feature_candle_end_timestamp':row['feature_candle_end_timestamp']})
                if counts['joined_rows'] % 500000 == 0:
                    print(f'[JOIN] {counts["joined_rows"]:,}', flush=True)

    counts['unjoined_train_rows'] = sum(train_keys.values())
    counts['schema_variants'] = len(schema_set)
    # Verify available reported aggregate totals without guessing absent fields.
    for label, obj, key, actual in (
        ('join_result',join_result,'joined_rows',counts['joined_rows']),
        ('join_manifest',join_manifest,'joined_rows',counts['joined_rows']),
        ('join_manifest',join_manifest,'shard_count',counts['shard_files']),
    ):
        if key in obj and obj[key] is not None and int(obj[key]) != actual:
            raise ValueError(f'{label}.{key} mismatch: reported={obj[key]}, observed={actual}')

    # Any post-signal feature requires an explicit timestamp convention review.
    critical = counts['unmatched_join_rows'] + counts['invalid_timestamps'] + counts['invalid_candle_window'] + counts['feature_after_entry']
    needs_review = counts['feature_after_signal'] > 0
    status = 'PASS' if critical == 0 else 'FAIL'
    rows = []
    for (tf, classification), c in sorted(coverage.items()):
        n = c['train_rows']
        j = c['joined_rows']
        rows.append({'timeframe':tf,'classification':classification,'train_rows':n,'joined_rows':j,'join_rate_pct':round(100*j/n,6) if n else 0})
    result = {
        'version': VERSION,
        'execution_status': status,
        'research_decision': 'HOLD',
        'point_in_time_proven': status == 'PASS' and not needs_review and counts['joined_rows'] > 0,
        'timestamp_convention_review_required': needs_review,
        'feature_join_verified': critical == 0 and counts['unjoined_train_rows'] + counts['joined_rows'] == counts['train_rows'],
        'train_experiment_approved': False,
        'independent_oos_proven': False,
        'profitability_proven': False,
        'approved_for_live_trading': False,
        'counts': dict(counts),
        'issues_recorded': len(issues),
        'issues_truncated': sum(counts[k] for k in ('unmatched_join_rows','invalid_timestamps','invalid_candle_window','feature_after_entry','feature_after_signal')) > len(issues),
        'notes': [
            'Feature candle END is compared with signal and entry timestamps.',
            'If signal_timestamp denotes the opening of the signal candle, feature_after_signal may require an explicit next-open decision convention.',
            'A PASS indicates the audit ran without critical detected contradictions, not proof of absence of leakage in upstream feature formulas.',
            'Source files are never modified. Missing TRAIN rows remain excluded from JOIN-based experiments.',
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    coverage_path = output_dir / 'feature_point_in_time_coverage.csv'
    issues_path = output_dir / 'feature_point_in_time_issues.csv'
    result_path = output_dir / 'feature_point_in_time_result.json'
    manifest_path = output_dir / 'feature_point_in_time_manifest.json'
    atomic_csv(coverage_path, ['timeframe','classification','train_rows','joined_rows','join_rate_pct'], rows)
    atomic_csv(issues_path, ['issue','file','line','market','signal_timestamp','feature_candle_end_timestamp'], issues)
    atomic_json(result_path, result)
    manifest = {'version':VERSION,'input_sha256':input_hashes,'output_sha256':{p.name:digest(p) for p in (coverage_path,issues_path,result_path)},'source_shards':counts['shard_files'],'source_shard_content_hashes_not_verified':True}
    atomic_json(manifest_path, manifest)
    print(json.dumps({'execution_status':status,'counts':dict(counts),'point_in_time_proven':result['point_in_time_proven'],'output_dir':str(output_dir)}, indent=2), flush=True)
    return 0 if status == 'PASS' else 2


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'FATAL: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        sys.exit(1)
