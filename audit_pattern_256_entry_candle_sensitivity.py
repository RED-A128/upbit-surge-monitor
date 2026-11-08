
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Entry Candle Sensitivity
Clean V001

Compares:
 A: entry candle included
 B: entry candle excluded

Uses actual OHLCV for each sampled event.

Does not modify source OHLCV.
Does not approve training or trading.
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
AUDIT = "pattern_256_entry_candle_sensitivity"

TIMEFRAME_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIMESTAMP_COLUMNS = (
    "timestamp",
    "utc_time",
    "candle_date_time_utc",
    "datetime",
    "time",
)

HIGH_COLUMNS = ("high", "high_price")
OPEN_COLUMNS = ("open", "opening_price")


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)
    return digest.hexdigest()


def read_json(path):
    with open(path, encoding="utf-8-sig") as handle:
        return json.load(handle)


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(
            value,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")


def read_csv(path):
    with open(
        path,
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)
        return list(reader), reader.fieldnames or []


def parse_epoch(value):
    value = str(value).strip()

    if not value:
        raise ValueError("Empty timestamp")

    try:
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("Non-finite timestamp")
        if numeric > 1e14:
            numeric /= 1_000_000
        elif numeric > 1e11:
            numeric /= 1000
        return int(numeric)
    except ValueError:
        pass

    value = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp())


def parse_positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("Invalid positive number")
    return number


def parse_integer(value):
    number = float(value)
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError("Invalid integer")
    return int(number)


def select_column(fieldnames, candidates):
    mapping = {
        str(name).strip().lower(): name
        for name in fieldnames
    }

    for candidate in candidates:
        if candidate in mapping:
            return mapping[candidate]

    raise ValueError(
        "Required column missing: " + ", ".join(candidates)
    )


def load_ohlcv(path):
    rows, columns = read_csv(path)

    timestamp_col = select_column(
        columns, TIMESTAMP_COLUMNS
    )
    high_col = select_column(columns, HIGH_COLUMNS)
    open_col = select_column(columns, OPEN_COLUMNS)

    candles = {}

    for row in rows:
        epoch = parse_epoch(row[timestamp_col])
        high = parse_positive(row[high_col])
        opening = parse_positive(row[open_col])

        if high < opening:
            raise ValueError(
                f"High below open at {epoch}"
            )

        if epoch in candles:
            raise ValueError(
                f"Duplicate timestamp: {epoch}"
            )

        candles[epoch] = {
            "high": high,
            "open": opening,
        }

    return candles


def evaluate_window(
    candles,
    entry_epoch,
    bars,
    step_seconds,
    offset,
    entry_price,
):
    """
    offset 0: include entry candle
    offset 1: exclude entry candle

    Uses exactly `bars` expected timestamp slots.
    Never compresses missing candles.
    """
    timestamps = [
        entry_epoch + (offset + i) * step_seconds
        for i in range(bars)
    ]

    missing = [
        ts for ts in timestamps
        if ts not in candles
    ]

    result = {
        "complete": len(missing) == 0,
        "expected_bars": bars,
        "available_bars": bars - len(missing),
        "missing_slots": len(missing),
        "max_high": None,
        "hit_30pct": None,
        "first_hit_bar": None,
    }

    if missing:
        return result

    highs = [
        candles[ts]["high"]
        for ts in timestamps
    ]

    result["max_high"] = max(highs)

    if entry_price is not None:
        target = entry_price * 1.30
        result["hit_30pct"] = any(
            high >= target for high in highs
        )

        for i, high in enumerate(highs, start=1):
            if high >= target:
                result["first_hit_bar"] = i
                break

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--ohlcv-root", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    input_dir = Path(args.input).resolve()
    ohlcv_root = Path(args.ohlcv_root).resolve()
    output_dir = Path(args.output).resolve()

    required = {
        "replay_details.csv": input_dir / "replay_details.csv",
        "replay_result.json": input_dir / "replay_result.json",
        "replay_manifest.json": input_dir / "replay_manifest.json",
    }

    for name, path in required.items():
        if not path.is_file():
            raise FileNotFoundError(name)

    manifest = read_json(required["replay_manifest.json"])
    result = read_json(required["replay_result.json"])

    if manifest.get("execution") != "PASS":
        raise ValueError("Input manifest is not PASS")

    if result.get("execution") != "PASS":
        raise ValueError("Input result is not PASS")

    for name in (
        "replay_details.csv",
        "replay_result.json",
    ):
        expected = manifest.get(
            "output_sha256", {}
        ).get(name)

        if not expected or sha256(required[name]) != expected:
            raise ValueError(f"Input SHA256 mismatch: {name}")

    events, columns = read_csv(
        required["replay_details.csv"]
    )

    needed = {
        "source",
        "timeframe",
        "market",
        "entry_epoch",
        "entry_price_reported",
        "expected_bars",
        "status",
    }

    if not needed.issubset(set(columns)):
        raise ValueError("Replay detail schema mismatch")

    if len(events) != result.get("processed_events"):
        raise ValueError("Replay event count mismatch")

    cache = {}
    cache_errors = {}
    details = []
    counts = Counter()

    for index, event in enumerate(events, start=1):
        source = event["source"].strip()
        timeframe = event["timeframe"].strip()
        market = event["market"].strip()

        if source not in ("gap", "independent"):
            raise ValueError(f"Invalid source at row {index}")

        if timeframe not in TIMEFRAME_SECONDS:
            raise ValueError(f"Invalid timeframe at row {index}")

        if not market.startswith("KRW-"):
            raise ValueError(f"Invalid market at row {index}")

        entry_epoch = parse_epoch(event["entry_epoch"])
        bars = parse_integer(event["expected_bars"])

        if bars <= 0:
            raise ValueError(f"Invalid horizon at row {index}")

        entry_price = None

        if source == "independent":
            entry_price = parse_positive(
                event["entry_price_reported"]
            )

        key = (timeframe, market)

        path = (
            ohlcv_root
            / timeframe
            / f"{market}.csv"
        )

        if key not in cache and key not in cache_errors:
            try:
                if not path.is_file():
                    raise FileNotFoundError(str(path))
                cache[key] = load_ohlcv(path)
            except Exception as exc:
                cache_errors[key] = str(exc)

        row = {
            "index": index,
            "source": source,
            "timeframe": timeframe,
            "market": market,
            "entry_epoch": entry_epoch,
            "expected_bars": bars,
            "original_status": event["status"],
            "include_complete": "",
            "exclude_complete": "",
            "include_available": "",
            "exclude_available": "",
            "include_hit": "",
            "exclude_hit": "",
            "include_first_hit": "",
            "exclude_first_hit": "",
            "classification": "",
            "error": "",
        }

        if key in cache_errors:
            row["classification"] = "OHLCV_ERROR"
            row["error"] = cache_errors[key]
            counts["OHLCV_ERROR"] += 1
            details.append(row)
            continue

        candles = cache[key]
        step = TIMEFRAME_SECONDS[timeframe]

        included = evaluate_window(
            candles, entry_epoch, bars,
            step, 0, entry_price
        )

        excluded = evaluate_window(
            candles, entry_epoch, bars,
            step, 1, entry_price
        )

        row["include_complete"] = included["complete"]
        row["exclude_complete"] = excluded["complete"]
        row["include_available"] = included["available_bars"]
        row["exclude_available"] = excluded["available_bars"]
        row["include_hit"] = included["hit_30pct"]
        row["exclude_hit"] = excluded["hit_30pct"]
        row["include_first_hit"] = included["first_hit_bar"]
        row["exclude_first_hit"] = excluded["first_hit_bar"]

        if not included["complete"] or not excluded["complete"]:
            classification = "INCOMPLETE_COMPARISON"

        elif source == "gap":
            classification = "WINDOW_ONLY"

        elif included["hit_30pct"] != excluded["hit_30pct"]:
            classification = "HIT_CHANGED"

        elif included["first_hit_bar"] != excluded["first_hit_bar"]:
            classification = "FIRST_HIT_BAR_CHANGED"

        else:
            classification = "UNCHANGED"

        row["classification"] = classification
        counts[classification] += 1
        details.append(row)

        if index % 100 == 0:
            print(
                f"[진행] {index}/{len(events)}",
                flush=True,
            )

    output_dir.mkdir(parents=True, exist_ok=True)

    detail_path = output_dir / "entry_sensitivity_detail.csv"

    fieldnames = list(details[0].keys()) if details else []

    with open(
        detail_path,
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames
        )
        writer.writeheader()
        writer.writerows(details)

    output_result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "sampled_events": len(events),
        "market_timeframe_pairs": len(cache) + len(cache_errors),
        "classification_counts": dict(counts),
        "comparison_conventions": {
            "included": "entry candle is first evaluation bar",
            "excluded": "evaluation begins one candle later",
        },
        "horizon_units": {
            "independent": "bars as supplied by replay",
            "gap": "bars derived by prior replay",
        },
        "horizon_unit_verified": False,
        "entry_candle_convention_verified": False,
        "independent_oos_proven": False,
        "full_population_replay_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Sensitivity analysis, not proof of original convention",
            "Only independent events have usable entry prices",
            "Incomplete windows are not treated as misses",
            "OHLCV high does not establish executable profit",
            "No fees, slippage or intrabar execution model",
            "Sample audit, not full population",
        ],
    }

    result_path = output_dir / "entry_sensitivity_result.json"
    write_json(result_path, output_result)

    output_manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "research_decision": "HOLD",
        "script_sha256": sha256(Path(__file__).resolve()),
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
        output_dir / "entry_sensitivity_manifest.json",
        output_manifest,
    )

    print("\n=== Pattern 256 Entry Candle Sensitivity ===")
    print(f"Version: {VERSION}")
    print(f"Events: {len(events)}")

    for name, count in sorted(counts.items()):
        print(f"{name}: {count}")

    print("Execution: PASS")
    print("Research decision: HOLD")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
