
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pattern 256 Entry Price & Window Integrity - Clean V001.

Read-only audit. Does not determine executable entry prices,
original entry convention, +30% hits or profitability.
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

VERSION = "Clean V001"
AUDIT = "pattern_256_entry_price_window"
STEPS = {"h1": 3600, "h4": 14400, "d1": 86400}
TIME_COLS = (
    "timestamp", "utc_time", "datetime", "date",
    "candle_date_time_utc", "time", "ts"
)
PRICE_COLS = {
    "open": ("open", "opening_price"),
    "high": ("high", "high_price"),
    "low": ("low", "low_price"),
    "close": ("close", "trade_price", "closing_price"),
}
DETAIL_COLS = [
    "index", "source", "market", "timeframe",
    "classification", "expected_bars",
    "entry_epoch", "entry_present",
    "entry_open", "entry_close", "next_open",
    "next_close", "entry_to_next_open_pct",
    "entry_to_next_close_pct",
    "include_expected", "include_available",
    "include_missing", "exclude_expected",
    "exclude_available", "exclude_missing",
    "coverage_reconciled", "price_errors",
    "window_status", "error"
]


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for part in iter(lambda: f.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, value):
    with path.open("w", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False,
                  indent=2, sort_keys=True)
        f.write("\n")


def column(names, candidates):
    lookup = {name.strip().lower(): name for name in names}
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    raise ValueError("Missing column: " + "/".join(candidates))


def epoch(value):
    text = str(value).strip()
    if not text:
        raise ValueError("Empty timestamp")
    try:
        number = float(text)
        if not math.isfinite(number):
            raise ValueError("Nonfinite timestamp")
        if abs(number) >= 1e14:
            number /= 1e6
        elif abs(number) >= 1e11:
            number /= 1e3
        return int(number)
    except ValueError:
        dt = datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())


def read_prices(path):
    candles = {}
    errors = []
    count = 0

    with path.open("r", encoding="utf-8-sig",
                   newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("Empty OHLCV CSV")

        tcol = column(reader.fieldnames, TIME_COLS)
        pcols = {
            key: column(reader.fieldnames, aliases)
            for key, aliases in PRICE_COLS.items()
        }

        for rownum, row in enumerate(reader, start=2):
            count += 1
            t = epoch(row[tcol])
            if t in candles:
                raise ValueError(
                    f"Duplicate timestamp at row {rownum}"
                )

            values = {}
            for key, col in pcols.items():
                try:
                    values[key] = float(row[col])
                except (ValueError, TypeError):
                    values[key] = float("nan")

            candles[t] = values

    return candles, count, errors


def check_candle(candle):
    if candle is None:
        return ["MISSING_CANDLE"]

    issues = []
    for key, value in candle.items():
        if not math.isfinite(value) or value <= 0:
            issues.append("INVALID_" + key.upper())

    if issues:
        return issues

    o, h, l, c = (
        candle["open"], candle["high"],
        candle["low"], candle["close"]
    )

    if l > h:
        issues.append("LOW_ABOVE_HIGH")
    if h < max(o, c):
        issues.append("HIGH_BELOW_BODY")
    if l > min(o, c):
        issues.append("LOW_ABOVE_BODY")
    return issues


def locate(root, timeframe, market):
    if timeframe not in STEPS:
        raise ValueError("Invalid timeframe")
    if not market.startswith("KRW-") or (
        "/" in market or "\\" in market or ".." in market
    ):
        raise ValueError("Invalid market")

    candidates = [
        root / "data" / "ohlcv" / timeframe /
        (market + ".csv"),
        root / "data" / "raw" / timeframe /
        (market + ".csv"),
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "OHLCV missing: " + timeframe + "/" + market
    )


def inspect(row, root, cache):
    result = {key: "" for key in DETAIL_COLS}
    for key in (
        "index", "source", "market", "timeframe",
        "classification", "expected_bars",
        "entry_epoch", "entry_present"
    ):
        result[key] = row[key]

    tf = row["timeframe"]
    market = row["market"]
    start = int(row["entry_epoch"])
    bars = int(row["expected_bars"])

    if bars < 1 or bars > 100000:
        raise ValueError("Invalid horizon")

    path = locate(root, tf, market)
    key = str(path.resolve())

    if key not in cache:
        cache[key] = read_prices(path)

    candles, total, _ = cache[key]
    step = STEPS[tf]

    include_times = [
        start + i * step for i in range(bars)
    ]
    exclude_times = [
        start + (i + 1) * step for i in range(bars)
    ]

    include_available = sum(
        t in candles for t in include_times
    )
    exclude_available = sum(
        t in candles for t in exclude_times
    )

    entry = candles.get(start)
    next_candle = candles.get(start + step)

    issues = Counter()
    for t in set(include_times + exclude_times):
        if t in candles:
            for issue in check_candle(candles[t]):
                issues[issue] += 1

    result.update({
        "entry_present": entry is not None,
        "include_expected": bars,
        "include_available": include_available,
        "include_missing": bars - include_available,
        "exclude_expected": bars,
        "exclude_available": exclude_available,
        "exclude_missing": bars - exclude_available,
        "price_errors": json.dumps(
            dict(sorted(issues.items()))
        ),
    })

    if entry is not None:
        result["entry_open"] = entry["open"]
        result["entry_close"] = entry["close"]

    if next_candle is not None:
        result["next_open"] = next_candle["open"]
        result["next_close"] = next_candle["close"]

    if (
        entry is not None
        and next_candle is not None
        and not check_candle(entry)
        and not check_candle(next_candle)
    ):
        result["entry_to_next_open_pct"] = round(
            (next_candle["open"] / entry["close"] - 1)
            * 100, 8
        )
        result["entry_to_next_close_pct"] = round(
            (next_candle["close"] / entry["close"] - 1)
            * 100, 8
        )

    reconciled = (
        include_available == int(row["include_actual"])
        and exclude_available == int(row["exclude_actual"])
        and bars - include_available
        == int(row["include_missing"])
        and bars - exclude_available
        == int(row["exclude_missing"])
    )

    result["coverage_reconciled"] = reconciled

    if not reconciled:
        status = "COVERAGE_MISMATCH"
    elif issues:
        status = "INVALID_PRICE"
    elif entry is None:
        status = "ENTRY_MISSING"
    elif next_candle is None:
        status = "NEXT_CANDLE_MISSING"
    elif (
        include_available != bars
        or exclude_available != bars
    ):
        status = "WINDOW_GAPS"
    else:
        status = "PRICE_WINDOW_COMPLETE"

    result["window_status"] = status
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    inp = Path(args.input).resolve()
    out = Path(args.output).resolve()

    source = {
        "ohlcv_coverage_detail.csv":
            inp / "ohlcv_coverage_detail.csv",
        "ohlcv_coverage_result.json":
            inp / "ohlcv_coverage_result.json",
        "ohlcv_coverage_manifest.json":
            inp / "ohlcv_coverage_manifest.json",
    }

    for name, path in source.items():
        if not path.is_file():
            raise FileNotFoundError(name)

    previous = read_json(
        source["ohlcv_coverage_result.json"]
    )
    manifest = read_json(
        source["ohlcv_coverage_manifest.json"]
    )

    if (
        previous.get("execution") != "PASS"
        or manifest.get("execution") != "PASS"
        or previous.get("audit")
        != "pattern_256_entry_boundary_ohlcv"
        or not previous.get("all_counts_agree")
    ):
        raise ValueError("Previous audit not qualified")

    for name in (
        "ohlcv_coverage_detail.csv",
        "ohlcv_coverage_result.json",
    ):
        expected = manifest.get(
            "output_sha256", {}
        ).get(name, "").lower()

        if not expected or sha256(source[name]) != expected:
            raise ValueError("Input SHA256 mismatch: " + name)

    with source["ohlcv_coverage_detail.csv"].open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        rows = list(csv.DictReader(f))

    if len(rows) != previous.get("processed_rows"):
        raise ValueError("Input row count mismatch")

    cache = {}
    results = []
    counts = Counter()
    fatal_errors = []

    for index, row in enumerate(rows, start=1):
        try:
            item = inspect(row, root, cache)
        except Exception as exc:
            item = {key: "" for key in DETAIL_COLS}
            for key in (
                "index", "source", "market", "timeframe",
                "classification", "expected_bars",
                "entry_epoch"
            ):
                item[key] = row.get(key, "")
            item["window_status"] = "ERROR"
            item["error"] = str(exc)
            fatal_errors.append({
                "row": index,
                "error": str(exc)
            })

        results.append(item)
        counts[item["window_status"]] += 1

    out.mkdir(parents=True, exist_ok=True)
    detail = out / "entry_price_window_detail.csv"

    with detail.open(
        "w", encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=DETAIL_COLS
        )
        writer.writeheader()
        writer.writerows(results)

    execution = "FAIL" if fatal_errors else "PASS"

    summary = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "input_rows": len(rows),
        "processed_rows": len(results),
        "ohlcv_files_loaded": len(cache),
        "status_counts": dict(sorted(counts.items())),
        "fatal_errors": fatal_errors[:100],
        "coverage_consistent": all(
            r["coverage_reconciled"] is True
            for r in results
        ),
        "price_integrity_verified": (
            execution == "PASS"
            and counts["INVALID_PRICE"] == 0
            and counts["COVERAGE_MISMATCH"] == 0
        ),
        "entry_convention_proven": False,
        "horizon_unit_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Uses supplied entry timestamp and horizon",
            "Checks price validity in observed candles",
            "Does not determine original entry convention",
            "Does not independently replay +30% outcomes",
            "Does not model fees or slippage",
            "Does not establish trading profitability"
        ]
    }

    result_file = out / "entry_price_window_result.json"
    save_json(result_file, summary)

    output_manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "input_sha256": {
            name: sha256(path)
            for name, path in source.items()
        },
        "output_sha256": {
            detail.name: sha256(detail),
            result_file.name: sha256(result_file),
        }
    }

    save_json(
        out / "entry_price_window_manifest.json",
        output_manifest
    )

    print("=== Pattern 256 Entry Price & Window ===")
    print("Execution:", execution)
    print("Events:", len(rows))
    print("OHLCV files:", len(cache))
    for status, count in sorted(counts.items()):
        print(f"{status}: {count}")
    print("Research decision: HOLD")

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("[FATAL]", exc, file=sys.stderr)
        sys.exit(1)
