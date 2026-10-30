# -*- coding: utf-8 -*-
"""
audit_pattern_256_split_candles.py
Clean V001

Read-only audit of:
- Per-row OHLCV / Feature / Signal timestamp alignment
- Actual candle counts in retrospective research periods
- Time gaps and duplicate timestamps
- Optional explicitly named binary signal column
- SHA256-protected resumable checkpoints

This script does NOT prove independent OOS performance.
"""

import argparse
import csv
import hashlib
import itertools
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"

PERIODS = (
    ("DEVELOPMENT", "2017-01-01", "2024-01-01"),
    ("VALIDATION", "2024-01-01", "2025-07-01"),
    ("RETROSPECTIVE_TEST", "2025-07-01", "2026-10-01"),
)

TIMEFRAMES = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIMESTAMP_NAMES = (
    "timestamp",
    "datetime",
    "date",
    "time",
    "timestamp_utc",
    "candle_date_time_utc",
    "candle_date_time_kst",
    "open_time",
)

OUTPUT_COLUMNS = (
    "timeframe",
    "market",
    "period",
    "rows",
    "first_timestamp",
    "last_timestamp",
    "duplicate_timestamps",
    "out_of_order",
    "timestamp_mismatches",
    "large_gaps",
    "signal_column",
    "signal_count",
    "signal_status",
    "status",
    "error",
)


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def atomic_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(temp, path)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def parse_time(value):
    value = str(value).strip()
    if not value:
        raise ValueError("Empty timestamp")

    try:
        number = float(value)
        if number == number and abs(number) >= 1e8:
            if abs(number) >= 1e11:
                number /= 1000
            return datetime.fromtimestamp(number, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        pass

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def timestamp_column(fieldnames):
    lookup = {str(c).lower().strip(): c for c in fieldnames}
    for name in TIMESTAMP_NAMES:
        if name in lookup:
            return lookup[name]
    raise ValueError(f"No timestamp column: {fieldnames}")


def verify_previous(root):
    report = root / "data/reports/pattern_256_temporal_split_plan"
    result_path = report / "temporal_split_result.json"
    manifest_path = report / "temporal_split_manifest.json"

    require(result_path.is_file(), "Missing temporal split result")
    require(manifest_path.is_file(), "Missing temporal split manifest")

    result = load_json(result_path)
    manifest = load_json(manifest_path)

    require(result.get("execution") == "PASS", "Temporal plan not PASS")
    require(result.get("alignment_issues") == 0, "Temporal alignment issues")
    require(result.get("research_decision") == "HOLD", "Unexpected decision")
    require(result.get("split_dates_approved") is False, "Unexpected approval")

    hashes = manifest.get("output_sha256", {})
    for filename in (
        "temporal_alignment.csv",
        "temporal_coverage.csv",
        "temporal_split_result.json",
    ):
        path = report / filename
        require(path.is_file(), f"Missing evidence: {filename}")
        require(
            sha(path) == hashes.get(filename),
            f"Evidence hash mismatch: {filename}",
        )

    return sha(manifest_path)


def get_paths(root, timeframe, market):
    return (
        root / "data/ohlcv" / timeframe / f"{market}.csv",
        root / "data/features" / timeframe / f"{market}.csv",
        root / "data/signals/256" / timeframe / f"{market}.csv",
    )


def period_for(dt):
    for name, start, end in PERIODS:
        if start <= dt.date().isoformat() < end:
            return name
    return None


def audit_market(root, timeframe, market, signal_column):
    paths = get_paths(root, timeframe, market)
    for path in paths:
        require(path.is_file(), f"Missing CSV: {path}")

    source_hashes = [sha(p) for p in paths]

    files = [p.open("r", encoding="utf-8-sig", newline="") for p in paths]
    try:
        readers = [csv.DictReader(f) for f in files]
        cols = [timestamp_column(r.fieldnames or []) for r in readers]

        signal_available = bool(
            signal_column and signal_column in (readers[2].fieldnames or [])
        )

        if signal_column and not signal_available:
            raise ValueError(
                f"Signal column not found: {signal_column}; "
                f"available={readers[2].fieldnames}"
            )

        stats = {}
        for name, _, _ in PERIODS:
            stats[name] = {
                "rows": 0,
                "first_timestamp": "",
                "last_timestamp": "",
                "duplicate_timestamps": 0,
                "out_of_order": 0,
                "timestamp_mismatches": 0,
                "large_gaps": 0,
                "signal_count": 0,
                "error": "",
            }

        previous = None
        total = 0
        mismatches = 0

        for triple in itertools.zip_longest(*readers):
            total += 1
            if any(row is None for row in triple):
                raise ValueError(f"CSV row-count mismatch at row {total}")

            times = [
                parse_time(row[column])
                for row, column in zip(triple, cols)
            ]

            t = times[0]
            period = period_for(t)

            if times[1] != t or times[2] != t:
                mismatches += 1

            if period is not None:
                s = stats[period]
                s["rows"] += 1

                iso = t.isoformat()
                if not s["first_timestamp"]:
                    s["first_timestamp"] = iso
                s["last_timestamp"] = iso

                if times[1] != t or times[2] != t:
                    s["timestamp_mismatches"] += 1

                if previous is not None:
                    delta = (t - previous).total_seconds()
                    if delta == 0:
                        s["duplicate_timestamps"] += 1
                    elif delta < 0:
                        s["out_of_order"] += 1
                    elif delta > TIMEFRAMES[timeframe]:
                        # Crypto OHLCV may omit no-trade candles.
                        # This is a gap observation, not automatically corruption.
                        s["large_gaps"] += 1

                if signal_available:
                    value = str(triple[2][signal_column]).strip().lower()
                    if value in ("1", "1.0", "true"):
                        s["signal_count"] += 1
                    elif value not in ("0", "0.0", "false", "", "nan"):
                        raise ValueError(
                            f"Non-binary signal value at row {total}: {value}"
                        )

            previous = t

        rows = []
        for name, _, _ in PERIODS:
            s = stats[name]
            problem = (
                s["duplicate_timestamps"]
                + s["out_of_order"]
                + s["timestamp_mismatches"]
            )
            rows.append({
                "timeframe": timeframe,
                "market": market,
                "period": name,
                "rows": s["rows"],
                "first_timestamp": s["first_timestamp"],
                "last_timestamp": s["last_timestamp"],
                "duplicate_timestamps": s["duplicate_timestamps"],
                "out_of_order": s["out_of_order"],
                "timestamp_mismatches": s["timestamp_mismatches"],
                "large_gaps": s["large_gaps"],
                "signal_column": signal_column or "",
                "signal_count": (
                    s["signal_count"] if signal_available else ""
                ),
                "signal_status": (
                    "COUNTED" if signal_available else "NOT_VERIFIED"
                ),
                "status": "PASS" if problem == 0 else "REVIEW_REQUIRED",
                "error": "",
            })

        return {
            "source_sha256": source_hashes,
            "total_source_rows": total,
            "total_timestamp_mismatches": mismatches,
            "rows": rows,
        }

    finally:
        for f in files:
            f.close()


def write_csv(path, rows):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def run(root, signal_column, force):
    root = root.resolve()
    previous_hash = verify_previous(root)

    out = root / "data/reports/pattern_256_split_candles"
    checkpoints = out / "checkpoints"
    checkpoints.mkdir(parents=True, exist_ok=True)

    jobs = []
    for timeframe in TIMEFRAMES:
        folder = root / "data/ohlcv" / timeframe
        require(folder.is_dir(), f"Missing OHLCV folder: {folder}")
        markets = sorted(p.stem for p in folder.glob("KRW-*.csv"))
        require(markets, f"No markets: {timeframe}")
        jobs.extend((timeframe, market) for market in markets)

    all_rows = []
    resumed = 0
    executed = 0
    errors = []

    for index, (timeframe, market) in enumerate(jobs, 1):
        checkpoint = checkpoints / f"{timeframe}_{market}.json"
        paths = get_paths(root, timeframe, market)

        try:
            hashes = [sha(p) for p in paths]

            cached = None
            if not force and checkpoint.is_file():
                try:
                    candidate = load_json(checkpoint)
                    if (
                        candidate.get("version") == VERSION
                        and candidate.get("source_sha256") == hashes
                        and candidate.get("previous_manifest_sha256")
                        == previous_hash
                        and candidate.get("signal_column") == signal_column
                        and len(candidate.get("rows", [])) == len(PERIODS)
                    ):
                        cached = candidate
                except (ValueError, OSError, TypeError, KeyError):
                    cached = None

            if cached is not None:
                result = cached
                resumed += 1
            else:
                result = audit_market(root, timeframe, market, signal_column)
                result.update({
                    "version": VERSION,
                    "previous_manifest_sha256": previous_hash,
                    "signal_column": signal_column,
                })
                atomic_json(checkpoint, result)
                executed += 1

            all_rows.extend(result["rows"])

        except Exception as exc:
            errors.append({
                "timeframe": timeframe,
                "market": market,
                "error": f"{type(exc).__name__}: {exc}",
            })
            print(f"[ERROR] {timeframe} {market}: {exc}", flush=True)

        if index % 20 == 0 or index == len(jobs):
            print(
                f"[PROGRESS] {index}/{len(jobs)} "
                f"resumed={resumed} executed={executed} "
                f"errors={len(errors)}",
                flush=True,
            )

    csv_path = out / "split_candle_checks.csv"
    result_path = out / "split_candle_result.json"
    manifest_path = out / "split_candle_manifest.json"

    write_csv(csv_path, all_rows)

    status_counts = Counter(row["status"] for row in all_rows)
    period_totals = {
        name: sum(
            int(row["rows"])
            for row in all_rows
            if row["period"] == name
        )
        for name, _, _ in PERIODS
    }

    complete = (
        len(errors) == 0
        and len(all_rows) == len(jobs) * len(PERIODS)
    )
    clean = complete and status_counts.get("REVIEW_REQUIRED", 0) == 0

    result = {
        "audit": "pattern_256_split_candles",
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "execution": "PASS" if clean else "REVIEW_REQUIRED",
        "jobs": len(jobs),
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "completed_jobs": executed + resumed,
        "failed_jobs": len(errors),
        "errors": errors,
        "period_rows": period_totals,
        "period_records": len(all_rows),
        "status_counts": dict(status_counts),
        "signal_column": signal_column,
        "signal_counts_verified": bool(signal_column) and complete,
        "per_row_timestamp_alignment_proven": clean,
        "historical_research_overlap_checked": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Only the three retrospective candidate periods are inspected.",
            "Large time gaps are reported but not treated as automatic errors.",
            "Signal counts require an explicitly named binary signal column.",
            "Previous research exposure is not reconstructed.",
            "This audit does not calculate returns or prediction accuracy.",
            "Passing timestamp alignment does not prove absence of leakage.",
        ],
    }

    atomic_json(result_path, result)

    manifest = {
        "audit": "pattern_256_split_candles",
        "version": VERSION,
        "execution": result["execution"],
        "previous_manifest_sha256": previous_hash,
        "output_sha256": {
            "split_candle_checks.csv": sha(csv_path),
            "split_candle_result.json": sha(result_path),
        },
    }
    atomic_json(manifest_path, manifest)

    print(json.dumps({
        "execution": result["execution"],
        "jobs": len(jobs),
        "resumed": resumed,
        "executed": executed,
        "errors": len(errors),
        "period_rows": period_totals,
        "signal_counts_verified": result["signal_counts_verified"],
        "research_decision": "HOLD",
    }, indent=2))

    return 0 if complete else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--signal-column", default="")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    try:
        return run(Path(args.root), args.signal_column, args.force)
    except Exception as exc:
        print(f"[FAIL] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
