
# -*- coding: utf-8 -*-
"""
audit_pattern_256_signal_counts.py
Clean V001

Read-only full-history Signal 256 counting audit.

- Explicit columns: signal_setup, signal_ready, signal_trigger
- All KRW markets, h1/h4/d1
- Three retrospective research periods
- Source SHA256 + checkpoint/resume
- Previous schema evidence verification
- No training, backtesting, trading, or OOS approval

Run:
    py audit_pattern_256_signal_counts.py --root .
    py audit_pattern_256_signal_counts.py --root . --force
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
TIMEFRAMES = ("h1", "h4", "d1")
SIGNALS = ("signal_setup", "signal_ready", "signal_trigger")
PERIODS = (
    ("DEVELOPMENT", "2017-01-01", "2024-01-01"),
    ("VALIDATION", "2024-01-01", "2025-07-01"),
    ("RETROSPECTIVE_TEST", "2025-07-01", "2026-10-01"),
)
TIMESTAMP_COLUMNS = (
    "candle_date_time_utc",
    "timestamp_utc",
    "timestamp",
    "datetime",
)
REPORT_COLUMNS = (
    "timeframe", "market", "period", "rows",
    "signal_setup", "signal_ready", "signal_trigger",
    "setup_ready", "ready_trigger", "setup_trigger",
    "all_three", "duplicate_timestamps",
    "out_of_order", "first_timestamp", "last_timestamp",
    "status",
)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, data):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(temp, path)


def save_csv(path, rows):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def parse_time(value):
    value = str(value).strip()
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def period_name(timestamp):
    date = timestamp.date().isoformat()
    for name, start, end in PERIODS:
        if start <= date < end:
            return name
    return None


def binary(value, column, line):
    text = str(value or "").strip().lower()
    if text in ("1", "1.0", "true"):
        return 1
    if text in ("0", "0.0", "false"):
        return 0
    raise ValueError(
        f"Non-binary value in {column}, row {line}: {value!r}"
    )


def verify_previous(root):
    folder = root / "data/reports/pattern_256_signal_schema"
    result_path = folder / "signal_schema_result.json"
    manifest_path = folder / "signal_schema_manifest.json"

    require(result_path.is_file(), "Missing schema result")
    require(manifest_path.is_file(), "Missing schema manifest")

    result = load(result_path)
    manifest = load(manifest_path)

    require(result.get("execution") == "PASS", "Schema audit not PASS")
    require(result.get("failed_jobs") == 0, "Schema audit has failures")
    require(
        result.get("completed_jobs") == result.get("jobs"),
        "Schema audit incomplete",
    )
    require(
        result.get("research_decision") == "HOLD",
        "Unexpected prior research decision",
    )
    require(
        manifest.get("execution") == "PASS",
        "Schema manifest not PASS",
    )

    for filename in (
        "signal_schema_files.csv",
        "signal_schema_columns.csv",
        "signal_schema_result.json",
    ):
        path = folder / filename
        require(path.is_file(), f"Missing evidence: {filename}")
        expected = manifest.get("output_sha256", {}).get(filename)
        require(
            isinstance(expected, str) and len(expected) == 64,
            f"Missing hash: {filename}",
        )
        require(
            digest(path) == expected.lower(),
            f"SHA256 mismatch: {filename}",
        )

    return digest(manifest_path), int(result["jobs"])


def inspect(path, timeframe, market):
    stats = {}
    for name, _, _ in PERIODS:
        stats[name] = Counter()

    bounds = {
        name: {"first": "", "last": ""}
        for name, _, _ in PERIODS
    }

    previous = None
    total = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []

        require(len(columns) == len(set(columns)),
                f"Duplicate header: {path}")

        for signal in SIGNALS:
            require(signal in columns, f"Missing {signal}: {path}")

        time_column = next(
            (c for c in TIMESTAMP_COLUMNS if c in columns), None
        )
        require(time_column is not None,
                f"Missing UTC timestamp: {path}")

        for line, row in enumerate(reader, start=2):
            total += 1
            t = parse_time(row[time_column])
            period = period_name(t)

            values = {
                name: binary(row[name], name, line)
                for name in SIGNALS
            }

            if previous is not None and t <= previous:
                raise ValueError(
                    f"Duplicate or out-of-order timestamp at row {line}"
                )

            if period:
                s = stats[period]
                s["rows"] += 1
                for name, value in values.items():
                    s[name] += value

                setup = values["signal_setup"]
                ready = values["signal_ready"]
                trigger = values["signal_trigger"]

                s["setup_ready"] += setup * ready
                s["ready_trigger"] += ready * trigger
                s["setup_trigger"] += setup * trigger
                s["all_three"] += setup * ready * trigger

                iso = t.isoformat()
                if not bounds[period]["first"]:
                    bounds[period]["first"] = iso
                bounds[period]["last"] = iso

            previous = t

    rows = []
    for name, _, _ in PERIODS:
        s = stats[name]
        rows.append({
            "timeframe": timeframe,
            "market": market,
            "period": name,
            "rows": s["rows"],
            "signal_setup": s["signal_setup"],
            "signal_ready": s["signal_ready"],
            "signal_trigger": s["signal_trigger"],
            "setup_ready": s["setup_ready"],
            "ready_trigger": s["ready_trigger"],
            "setup_trigger": s["setup_trigger"],
            "all_three": s["all_three"],
            "duplicate_timestamps": 0,
            "out_of_order": 0,
            "first_timestamp": bounds[name]["first"],
            "last_timestamp": bounds[name]["last"],
            "status": "PASS",
        })

    return {"total_rows": total, "rows": rows}


def checkpoint_ok(data, source_hash, previous_hash, script_hash):
    return (
        isinstance(data, dict)
        and data.get("version") == VERSION
        and data.get("source_sha256") == source_hash
        and data.get("previous_manifest_sha256") == previous_hash
        and data.get("script_sha256") == script_hash
        and isinstance(data.get("rows"), list)
        and len(data["rows"]) == len(PERIODS)
        and all(
            isinstance(row, dict)
            and row.get("status") == "PASS"
            and all(column in row for column in REPORT_COLUMNS)
            for row in data["rows"]
        )
    )


def run(root, force):
    root = root.resolve()
    previous_hash, expected_jobs = verify_previous(root)
    script_hash = digest(Path(__file__).resolve())

    output = root / "data/reports/pattern_256_signal_counts"
    checkpoints = output / "checkpoints"
    checkpoints.mkdir(parents=True, exist_ok=True)

    jobs = []
    for timeframe in TIMEFRAMES:
        directory = root / "data/signals/256" / timeframe
        require(directory.is_dir(), f"Missing directory: {directory}")
        files = sorted(directory.glob("KRW-*.csv"))
        require(files, f"No signal files: {timeframe}")
        jobs.extend((timeframe, p.stem, p) for p in files)

    require(
        len(jobs) == expected_jobs,
        f"Job count changed: {len(jobs)} != {expected_jobs}",
    )

    rows = []
    errors = []
    resumed = 0
    executed = 0

    for index, (timeframe, market, path) in enumerate(jobs, 1):
        try:
            source_hash = digest(path)
            checkpoint = checkpoints / f"{timeframe}_{market}.json"
            cached = None

            if not force and checkpoint.is_file():
                try:
                    candidate = load(checkpoint)
                    if checkpoint_ok(
                        candidate, source_hash, previous_hash, script_hash
                    ):
                        cached = candidate
                except (OSError, ValueError, TypeError):
                    cached = None

            if cached is not None:
                data = cached
                resumed += 1
            else:
                data = inspect(path, timeframe, market)
                data.update({
                    "version": VERSION,
                    "source_sha256": source_hash,
                    "previous_manifest_sha256": previous_hash,
                    "script_sha256": script_hash,
                })
                save_json(checkpoint, data)
                executed += 1

            rows.extend(data["rows"])

        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors.append({
                "timeframe": timeframe,
                "market": market,
                "error": message,
            })
            print(f"[ERROR] {timeframe} {market}: {message}", flush=True)

        if index % 25 == 0 or index == len(jobs):
            print(
                f"[PROGRESS] {index}/{len(jobs)} "
                f"executed={executed} resumed={resumed} "
                f"errors={len(errors)}",
                flush=True,
            )

    complete = (
        not errors
        and executed + resumed == len(jobs)
        and len(rows) == len(jobs) * len(PERIODS)
    )

    totals = {}
    for period, _, _ in PERIODS:
        selected = [r for r in rows if r["period"] == period]
        totals[period] = {
            "rows": sum(int(r["rows"]) for r in selected),
            "signal_setup": sum(int(r["signal_setup"]) for r in selected),
            "signal_ready": sum(int(r["signal_ready"]) for r in selected),
            "signal_trigger": sum(int(r["signal_trigger"]) for r in selected),
            "all_three": sum(int(r["all_three"]) for r in selected),
        }

    result = {
        "audit": "pattern_256_signal_counts",
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "execution": "PASS" if complete else "REVIEW_REQUIRED",
        "jobs": len(jobs),
        "completed_jobs": executed + resumed,
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "failed_jobs": len(errors),
        "errors": errors,
        "period_records": len(rows),
        "period_totals": totals,
        "signal_columns": list(SIGNALS),
        "signal_values_binary_verified": complete,
        "signal_counts_verified": complete,
        "signal_semantics_verified": False,
        "independent_oos_proven": False,
        "prediction_performance_verified": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Column names do not establish signal semantics.",
            "Counts are signal-positive candles, not distinct trades.",
            "Repeated consecutive signals are counted separately.",
            "Retrospective periods are not independent OOS.",
            "No +30% outcome or profitability is calculated.",
            "Historical research exposure remains unverified.",
        ],
    }

    csv_path = output / "signal_count_periods.csv"
    result_path = output / "signal_count_result.json"
    manifest_path = output / "signal_count_manifest.json"

    save_csv(csv_path, rows)
    save_json(result_path, result)

    manifest = {
        "audit": "pattern_256_signal_counts",
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": script_hash,
        "previous_manifest_sha256": previous_hash,
        "output_sha256": {
            "signal_count_periods.csv": digest(csv_path),
            "signal_count_result.json": digest(result_path),
        },
    }
    save_json(manifest_path, manifest)

    print(json.dumps({
        "execution": result["execution"],
        "jobs": len(jobs),
        "completed": executed + resumed,
        "errors": len(errors),
        "period_totals": totals,
        "research_decision": "HOLD",
    }, indent=2, ensure_ascii=False))

    return 0 if complete else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    try:
        return run(Path(args.root), args.force)
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
