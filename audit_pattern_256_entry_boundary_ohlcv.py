
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pattern 256 Entry Boundary OHLCV Coverage - Clean V001."""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_entry_boundary_ohlcv"

STEPS = {"h1": 3600, "h4": 14400, "d1": 86400}

TIMESTAMP_COLUMNS = (
    "timestamp", "utc_time", "datetime", "date",
    "candle_date_time_utc", "time", "ts",
)

OUTPUT_COLUMNS = (
    "index", "source", "timeframe", "market",
    "classification", "entry_epoch", "expected_bars",
    "ohlcv_file", "ohlcv_rows", "entry_present",
    "include_actual", "exclude_actual",
    "include_reported", "exclude_reported",
    "include_missing", "exclude_missing",
    "include_missing_epochs", "exclude_missing_epochs",
    "include_count_agrees", "exclude_count_agrees",
    "coverage_status", "error",
)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path, obj):
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False,
                  sort_keys=True)
        f.write("\n")


def to_epoch(value):
    s = str(value).strip()

    if not s:
        raise ValueError("Empty timestamp")

    try:
        n = float(s)
        if n > 100000000000000:
            n /= 1000000
        elif n > 100000000000:
            n /= 1000
        return int(n)
    except ValueError:
        pass

    s = s.replace("Z", "+00:00")

    dt = datetime.fromisoformat(s)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp())


def find_timestamp_column(fieldnames):
    mapping = {name.lower().strip(): name
               for name in fieldnames}

    for candidate in TIMESTAMP_COLUMNS:
        if candidate in mapping:
            return mapping[candidate]

    raise ValueError(
        "No recognized timestamp column: "
        + ",".join(fieldnames)
    )


def read_ohlcv(path):
    timestamps = set()
    total = 0
    duplicates = 0

    with path.open("r", encoding="utf-8-sig",
                   newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("Empty OHLCV header")

        column = find_timestamp_column(reader.fieldnames)

        for row in reader:
            total += 1
            epoch = to_epoch(row[column])

            if epoch in timestamps:
                duplicates += 1

            timestamps.add(epoch)

    if duplicates:
        raise ValueError(
            f"Duplicate OHLCV timestamps: {duplicates}"
        )

    return timestamps, total


def locate_ohlcv(root, timeframe, market):
    candidates = [
        root / "data" / "ohlcv" / timeframe /
        f"{market}.csv",
        root / "data" / "raw" / timeframe /
        f"{market}.csv",
    ]

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    return None


def inspect_event(row, root, cache):
    result = {key: "" for key in OUTPUT_COLUMNS}

    for key in (
        "index", "source", "timeframe", "market",
        "classification", "entry_epoch", "expected_bars",
    ):
        result[key] = row[key]

    tf = row["timeframe"]
    market = row["market"]
    step = STEPS[tf]
    entry = int(row["entry_epoch"])
    horizon = int(row["expected_bars"])

    if entry <= 0 or horizon <= 0:
        raise ValueError("Invalid entry or horizon")

    path = locate_ohlcv(root, tf, market)

    if path is None:
        result["coverage_status"] = "SOURCE_MISSING"
        result["error"] = "OHLCV file not found"
        return result

    result["ohlcv_file"] = str(path)

    cache_key = str(path)

    if cache_key not in cache:
        cache[cache_key] = read_ohlcv(path)

    timestamps, total = cache[cache_key]
    result["ohlcv_rows"] = total

    included = [
        entry + i * step
        for i in range(horizon)
    ]

    excluded = [
        entry + (i + 1) * step
        for i in range(horizon)
    ]

    missing_a = [
        t for t in included if t not in timestamps
    ]
    missing_b = [
        t for t in excluded if t not in timestamps
    ]

    actual_a = horizon - len(missing_a)
    actual_b = horizon - len(missing_b)

    reported_a = int(row["include_available"])
    reported_b = int(row["exclude_available"])

    agree_a = actual_a == reported_a
    agree_b = actual_b == reported_b

    result.update({
        "entry_present": entry in timestamps,
        "include_actual": actual_a,
        "exclude_actual": actual_b,
        "include_reported": reported_a,
        "exclude_reported": reported_b,
        "include_missing": len(missing_a),
        "exclude_missing": len(missing_b),
        "include_missing_epochs": json.dumps(missing_a),
        "exclude_missing_epochs": json.dumps(missing_b),
        "include_count_agrees": agree_a,
        "exclude_count_agrees": agree_b,
        "coverage_status": (
            "COUNTS_AGREE"
            if agree_a and agree_b
            else "COUNT_DIFFERENCE"
        ),
    })

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--root", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = Path(args.input).resolve()
    output = Path(args.output).resolve()

    required = {
        "boundary_review.csv":
            source / "boundary_review.csv",
        "boundary_result.json":
            source / "boundary_result.json",
        "boundary_manifest.json":
            source / "boundary_manifest.json",
    }

    for name, path in required.items():
        if not path.is_file():
            raise FileNotFoundError(name)

    prior = read_json(required["boundary_result.json"])
    manifest = read_json(required["boundary_manifest.json"])

    if prior.get("execution") != "PASS":
        raise ValueError("Prior boundary audit not PASS")

    if manifest.get("execution") != "PASS":
        raise ValueError("Prior manifest not PASS")

    if prior.get("audit") != "pattern_256_entry_boundary":
        raise ValueError("Unexpected prior audit")

    for name in ("boundary_review.csv",
                 "boundary_result.json"):
        expected = manifest.get(
            "output_sha256", {}
        ).get(name)

        if not expected:
            raise ValueError(f"Missing hash: {name}")

        if sha256(required[name]) != expected:
            raise ValueError(f"SHA256 mismatch: {name}")

    with required["boundary_review.csv"].open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("Invalid boundary CSV")

        rows = list(reader)

    if len(rows) != prior.get("review_rows"):
        raise ValueError("Review row count mismatch")

    cache = {}
    results = []
    counts = Counter()
    errors = []

    for position, row in enumerate(rows, start=1):
        try:
            if row["timeframe"] not in STEPS:
                raise ValueError("Unknown timeframe")

            if not row["market"].startswith("KRW-"):
                raise ValueError("Invalid market")

            if row["classification"] not in (
                "FIRST_HIT_BAR_CHANGED",
                "INCOMPLETE_COMPARISON",
                "HIT_CHANGED",
            ):
                raise ValueError("Unexpected classification")

            item = inspect_event(row, root, cache)

        except Exception as exc:
            item = {key: "" for key in OUTPUT_COLUMNS}

            for key in (
                "index", "source", "timeframe", "market",
                "classification", "entry_epoch",
                "expected_bars",
            ):
                item[key] = row.get(key, "")

            item["coverage_status"] = "ERROR"
            item["error"] = str(exc)

            errors.append({
                "position": position,
                "market": row.get("market", ""),
                "error": str(exc),
            })

        results.append(item)
        counts[item["coverage_status"]] += 1

    if len(results) != len(rows):
        raise ValueError("Processed count mismatch")

    output.mkdir(parents=True, exist_ok=True)

    detail_path = output / "ohlcv_coverage_detail.csv"

    with detail_path.open(
        "w", encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=OUTPUT_COLUMNS
        )
        writer.writeheader()
        writer.writerows(results)

    # A missing source or differing count is a diagnostic,
    # not evidence that the original research is correct.
    # Parsing failures are reported as execution failure.
    execution = "FAIL" if errors else "PASS"

    summary = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "input_review_rows": len(rows),
        "processed_rows": len(results),
        "market_timeframe_files_loaded": len(cache),
        "coverage_status_counts": dict(sorted(counts.items())),
        "errors": errors[:100],
        "all_counts_agree": (
            counts["COUNTS_AGREE"] == len(rows)
        ),
        "entry_convention_proven": False,
        "horizon_unit_proven": False,
        "outcome_replay_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Checks OHLCV timestamp coverage only",
            "Assumes entry_epoch is a UTC candle timestamp",
            "Assumes fixed timeframe spacing",
            "Does not independently establish horizon units",
            "Does not reconstruct executable entry prices",
            "Does not verify +30 percent hit outcomes",
            "Does not establish profitability",
        ],
    }

    result_path = output / "ohlcv_coverage_result.json"
    write_json(result_path, summary)

    report_manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "input_sha256": {
            name: sha256(path)
            for name, path in required.items()
        },
        "output_sha256": {
            detail_path.name: sha256(detail_path),
            result_path.name: sha256(result_path),
        },
    }

    write_json(
        output / "ohlcv_coverage_manifest.json",
        report_manifest,
    )

    print("=== Pattern 256 Entry Boundary OHLCV ===")
    print(f"Execution: {execution}")
    print(f"Input events: {len(rows)}")
    print(f"Loaded OHLCV files: {len(cache)}")

    for key, count in sorted(counts.items()):
        print(f"{key}: {count}")

    print("Research decision: HOLD")

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)
