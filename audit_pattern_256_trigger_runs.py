
# -*- coding: utf-8 -*-
"""
audit_pattern_256_trigger_runs.py
Clean V001

Full-history trigger run and source-code reference audit.

Read-only:
- Verify previous signal count evidence and SHA256.
- Verify current Signal CSV SHA256 against prior schema inventory.
- Count signal_trigger=1 candles and contiguous runs.
- Count 0 -> 1 transitions across period boundaries.
- Record trigger-related source code line references.
- Checkpoint/resume with SHA256 validation.
- Preserve research HOLD.

No trade, return, or independent OOS validation.
"""

import argparse
import ast
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
PERIODS = (
    ("DEVELOPMENT", "2017-01-01", "2024-01-01"),
    ("VALIDATION", "2024-01-01", "2025-07-01"),
    ("RETROSPECTIVE_TEST", "2025-07-01", "2026-10-01"),
)
TIME_COLUMNS = (
    "candle_date_time_utc",
    "timestamp_utc",
    "timestamp",
    "datetime",
)
COLUMNS = (
    "timeframe", "market", "period",
    "candles", "trigger_positive_candles",
    "trigger_run_starts", "runs_ending_in_period",
    "longest_run_candles", "status",
)


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def atomic_json(path, obj):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(temp, path)


def atomic_csv(path, rows, columns):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def parse_time(value):
    dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def period_of(dt):
    day = dt.date().isoformat()
    for name, start, end in PERIODS:
        if start <= day < end:
            return name
    return None


def verify_prior(root):
    folder = root / "data/reports/pattern_256_signal_counts"
    result_file = folder / "signal_count_result.json"
    manifest_file = folder / "signal_count_manifest.json"

    require(result_file.is_file(), "Missing signal count result")
    require(manifest_file.is_file(), "Missing signal count manifest")

    result = read_json(result_file)
    manifest = read_json(manifest_file)

    require(result.get("execution") == "PASS", "Previous audit not PASS")
    require(result.get("failed_jobs") == 0, "Previous audit has failures")
    require(result.get("signal_counts_verified") is True,
            "Previous counts not verified")
    require(result.get("research_decision") == "HOLD",
            "Previous research decision changed")
    require(manifest.get("execution") == "PASS",
            "Previous manifest not PASS")

    for filename in (
        "signal_count_periods.csv",
        "signal_count_result.json",
    ):
        file = folder / filename
        require(file.is_file(), f"Missing {filename}")
        expected = manifest.get("output_sha256", {}).get(filename)
        require(isinstance(expected, str) and len(expected) == 64,
                f"Missing SHA256 for {filename}")
        require(sha(file) == expected.lower(),
                f"SHA256 mismatch: {filename}")

    return sha(manifest_file), result


def schema_hashes(root):
    folder = root / "data/reports/pattern_256_signal_schema"
    manifest_file = folder / "signal_schema_manifest.json"
    inventory_file = folder / "signal_schema_files.csv"

    require(manifest_file.is_file(), "Missing schema manifest")
    require(inventory_file.is_file(), "Missing schema inventory")

    manifest = read_json(manifest_file)
    expected = manifest.get("output_sha256", {}).get(
        "signal_schema_files.csv"
    )
    require(isinstance(expected, str) and sha(inventory_file) == expected,
            "Schema inventory SHA256 mismatch")

    index = {}
    with inventory_file.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        for row in csv.DictReader(f):
            key = (row["timeframe"], row["market"])
            require(key not in index, f"Duplicate schema entry: {key}")
            require(row["status"] == "PASS",
                    f"Schema entry not PASS: {key}")
            index[key] = row["source_sha256"].lower()

    return index


def inspect_source(path):
    require(path.is_file(), f"Missing detector source: {path}")
    source = path.read_text(encoding="utf-8-sig")
    ast.parse(source, filename=str(path))

    references = []
    for number, line in enumerate(source.splitlines(), 1):
        if "signal_trigger" in line:
            references.append({
                "line": number,
                "source": line.strip()[:300],
            })

    require(references, "No signal_trigger reference in detect_256.py")

    return {
        "file": path.name,
        "sha256": sha(path),
        "trigger_references": references,
        "semantics_verified": False,
    }


def binary(value, line):
    v = str(value if value is not None else "").strip().lower()
    if v in ("1", "1.0", "true"):
        return 1
    if v in ("0", "0.0", "false"):
        return 0
    raise ValueError(f"Invalid trigger value at CSV row {line}: {value!r}")


def inspect_csv(path, timeframe, market):
    stats = {name: Counter() for name, _, _ in PERIODS}
    previous_time = None
    previous_trigger = 0
    current_run = 0
    run_start_period = None
    total_rows = 0
    total_runs = 0
    max_run = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []

        require(len(fields) == len(set(fields)), "Duplicate CSV headers")
        require("signal_trigger" in fields, "Missing signal_trigger")

        timestamp_col = next(
            (c for c in TIME_COLUMNS if c in fields), None
        )
        require(timestamp_col is not None, "Missing UTC timestamp")

        for line, row in enumerate(reader, 2):
            total_rows += 1
            t = parse_time(row[timestamp_col])
            trigger = binary(row["signal_trigger"], line)

            if previous_time is not None:
                require(t > previous_time,
                        f"Duplicate/reversed timestamp at {line}")

            period = period_of(t)

            if period:
                stats[period]["candles"] += 1
                stats[period]["trigger_positive_candles"] += trigger

            if trigger == 1 and previous_trigger == 0:
                total_runs += 1
                current_run = 1
                run_start_period = period
                if period:
                    stats[period]["trigger_run_starts"] += 1

            elif trigger == 1 and previous_trigger == 1:
                current_run += 1

            elif trigger == 0 and previous_trigger == 1:
                max_run = max(max_run, current_run)
                if period:
                    stats[period]["runs_ending_in_period"] += 1
                current_run = 0
                run_start_period = None

            previous_time = t
            previous_trigger = trigger

    if current_run:
        max_run = max(max_run, current_run)
        if run_start_period:
            stats[run_start_period]["open_run_at_file_end"] += 1

    rows = []
    for name, _, _ in PERIODS:
        s = stats[name]
        rows.append({
            "timeframe": timeframe,
            "market": market,
            "period": name,
            "candles": s["candles"],
            "trigger_positive_candles": s["trigger_positive_candles"],
            "trigger_run_starts": s["trigger_run_starts"],
            "runs_ending_in_period": s["runs_ending_in_period"],
            # Longest run is calculated over the whole market file.
            # Do not misinterpret it as period-specific.
            "longest_run_candles": "",
            "status": "PASS",
        })

    return {
        "total_rows": total_rows,
        "total_runs": total_runs,
        "longest_run_candles": max_run,
        "rows": rows,
    }


def valid_checkpoint(
    obj, source_hash, previous_hash, script_hash, detector_hash
):
    if not isinstance(obj, dict):
        return False

    if any((
        obj.get("version") != VERSION,
        obj.get("source_sha256") != source_hash,
        obj.get("previous_sha256") != previous_hash,
        obj.get("script_sha256") != script_hash,
        obj.get("detector_sha256") != detector_hash,
    )):
        return False

    rows = obj.get("rows")
    if not isinstance(rows, list) or len(rows) != len(PERIODS):
        return False

    return all(
        isinstance(row, dict)
        and row.get("status") == "PASS"
        and all(k in row for k in COLUMNS)
        for row in rows
    )


def run(root, force):
    root = root.resolve()
    previous_hash, previous_result = verify_prior(root)
    source_index = schema_hashes(root)
    detector = inspect_source(root / "detect_256.py")
    script_hash = sha(Path(__file__).resolve())

    output = root / "data/reports/pattern_256_trigger_runs"
    checkpoints = output / "checkpoints"
    checkpoints.mkdir(parents=True, exist_ok=True)

    jobs = []
    for tf in TIMEFRAMES:
        folder = root / "data/signals/256" / tf
        require(folder.is_dir(), f"Missing signal folder: {folder}")
        jobs.extend(
            (tf, p.stem, p)
            for p in sorted(folder.glob("KRW-*.csv"))
        )

    require(len(jobs) == int(previous_result["jobs"]),
            "Job count differs from previous audit")
    require(len(source_index) == len(jobs),
            "Schema inventory job count mismatch")

    rows = []
    errors = []
    resumed = 0
    executed = 0
    runs_total = 0
    max_run = 0

    for i, (tf, market, path) in enumerate(jobs, 1):
        try:
            source_hash = sha(path)
            key = (tf, market)
            require(source_index.get(key) == source_hash,
                    f"Source changed since schema audit: {key}")

            checkpoint = checkpoints / f"{tf}_{market}.json"
            cached = None
            if not force and checkpoint.is_file():
                try:
                    candidate = read_json(checkpoint)
                    if valid_checkpoint(
                        candidate, source_hash, previous_hash,
                        script_hash, detector["sha256"]
                    ):
                        cached = candidate
                except (OSError, ValueError, TypeError):
                    pass

            if cached is None:
                data = inspect_csv(path, tf, market)
                data.update({
                    "version": VERSION,
                    "source_sha256": source_hash,
                    "previous_sha256": previous_hash,
                    "script_sha256": script_hash,
                    "detector_sha256": detector["sha256"],
                })
                atomic_json(checkpoint, data)
                executed += 1
            else:
                data = cached
                resumed += 1

            rows.extend(data["rows"])
            runs_total += int(data["total_runs"])
            max_run = max(max_run, int(data["longest_run_candles"]))

        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            errors.append({"timeframe": tf, "market": market,
                           "error": error})
            print(f"[ERROR] {tf} {market}: {error}", flush=True)

        if i % 25 == 0 or i == len(jobs):
            print(
                f"[PROGRESS] {i}/{len(jobs)} "
                f"executed={executed} resumed={resumed} "
                f"errors={len(errors)}",
                flush=True,
            )

    period_totals = {}
    for name, _, _ in PERIODS:
        selected = [r for r in rows if r["period"] == name]
        period_totals[name] = {
            "candles": sum(int(r["candles"]) for r in selected),
            "trigger_positive_candles": sum(
                int(r["trigger_positive_candles"]) for r in selected
            ),
            "trigger_run_starts": sum(
                int(r["trigger_run_starts"]) for r in selected
            ),
        }

    expected = previous_result["period_totals"]
    count_matches = all(
        period_totals[name]["candles"] == int(expected[name]["rows"])
        and period_totals[name]["trigger_positive_candles"]
        == int(expected[name]["signal_trigger"])
        for name, _, _ in PERIODS
    )

    complete = (
        not errors
        and executed + resumed == len(jobs)
        and len(rows) == len(jobs) * len(PERIODS)
        and count_matches
    )

    result = {
        "audit": "pattern_256_trigger_runs",
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "execution": "PASS" if complete else "REVIEW_REQUIRED",
        "jobs": len(jobs),
        "completed_jobs": executed + resumed,
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "failed_jobs": len(errors),
        "errors": errors,
        "previous_signal_counts_matched": count_matches,
        "period_totals": period_totals,
        "all_history_trigger_runs": runs_total,
        "longest_market_run_candles": max_run,
        "detector_source": detector,
        "trigger_semantics_verified": False,
        "independent_trade_events_verified": False,
        "prediction_performance_verified": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Trigger run starts are not independent trades.",
            "Consecutive rows define runs; time gaps are not split.",
            "Runs may cross retrospective period boundaries.",
            "Source references are not a semantic formula proof.",
            "No +30% outcome or profitability is calculated.",
        ],
    }

    csv_file = output / "trigger_run_periods.csv"
    result_file = output / "trigger_run_result.json"
    manifest_file = output / "trigger_run_manifest.json"

    atomic_csv(csv_file, rows, COLUMNS)
    atomic_json(result_file, result)

    manifest = {
        "audit": "pattern_256_trigger_runs",
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": script_hash,
        "detector_sha256": detector["sha256"],
        "previous_manifest_sha256": previous_hash,
        "output_sha256": {
            "trigger_run_periods.csv": sha(csv_file),
            "trigger_run_result.json": sha(result_file),
        },
    }
    atomic_json(manifest_file, manifest)

    print(json.dumps({
        "execution": result["execution"],
        "jobs": len(jobs),
        "failed": len(errors),
        "count_matches": count_matches,
        "trigger_runs": runs_total,
        "longest_run": max_run,
        "period_totals": period_totals,
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
        print(f"[FAIL] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
