
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
backtest_pattern_256_accumulation_candidate_independent.py
Clean V005

Independent reproduction of the original 30% validation.

Reference:
    validate_pattern_256_accumulation_30pct.py
    Clean V001

IMPORTANT
---------
1. The original validation results are reference data only.
2. OHLCV data is independently loaded and recalculated.
3. Entry = next available OHLCV row after signal.
4. Horizon = number of available OHLCV rows.
5. A time gap is recorded separately, not automatically rejected.
6. HIT takes precedence over CENSORED for incomplete windows.
7. Differences are reported; they are never silently accepted.
8. Production inputs are read-only.
9. No automated trading.
10. V005 uses its own checkpoint directory.

Requirements:
    Python 3.10+
    pandas >= 2.2
    numpy >= 2.0

Usage:
    py backtest_pattern_256_accumulation_candidate_independent.py

    py backtest_pattern_256_accumulation_candidate_independent.py --smoke-test

    py backtest_pattern_256_accumulation_candidate_independent.py --root "C:\\path\\to\\project"

    py backtest_pattern_256_accumulation_candidate_independent.py --output "C:\\path\\to\\output"

    py backtest_pattern_256_accumulation_candidate_independent.py --force

Output:
    candidate_independent_backtest_trades.csv
    candidate_independent_backtest_summary.csv
    candidate_independent_backtest_result.json
    candidate_independent_backtest_manifest.json
    candidate_independent_backtest_diagnostics.csv

Compatibility aliases:
    backtest_trades.csv
    backtest_summary.csv
    backtest_result.json
    backtest_manifest.json

Checkpoint:
    checkpoints_v005/state.json
    checkpoints_v005/trades.partial.csv
    checkpoints_v005/diagnostics.partial.csv

NOTE:
    The aliases are provided for workflow compatibility.
    Their content is identical to the corresponding primary output.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import os
import shutil
import sys
import tempfile
import time
import traceback

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PROGRAM_NAME = (
    "backtest_pattern_256_accumulation_candidate_independent.py"
)
PROGRAM_VERSION = "Clean V005"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

TIMEFRAME_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TARGET_RETURN_PCT = 30.0
TARGET_MULTIPLIER = 1.30

CSV_ENCODING = "utf-8-sig"

CHECKPOINT_INTERVAL = 10000

FLOAT_TOLERANCE_ABS = 1e-7
FLOAT_TOLERANCE_REL = 1e-8

REFERENCE_COLUMNS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "pattern_256_score",
    "accumulation_score",
    "signal_open",
    "signal_close",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "target_return_pct",
    "target_price",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "first_hit_timestamp",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
    "source_ohlcv_sha256",
]

COMPARISON_FIELDS = [
    "entry_timestamp",
    "entry_price",
    "target_price",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "first_hit_timestamp",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
]

NUMERIC_FIELDS = {
    "entry_price",
    "target_price",
    "first_hit_bars",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
}

TRADE_COLUMNS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
    "entry_timestamp",
    "entry_price",
    "target_price",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "first_hit_timestamp",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
    "entry_time_gap",
    "gap_count",
    "reference_status",
    "reference_hit_30pct",
    "mismatch_fields",
    "match",
]

DIAGNOSTIC_COLUMNS = [
    "source_row",
    "timeframe",
    "market",
    "signal_timestamp",
    "horizon",
    "category",
    "field",
    "reference_value",
    "independent_value",
    "details",
]

SUMMARY_COLUMNS = [
    "timeframe",
    "classification",
    "horizon",
    "total",
    "complete",
    "hit",
    "miss",
    "censored",
    "invalid",
    "matched",
    "mismatched",
    "hit_rate_pct",
    "entry_time_gaps",
    "observation_gaps",
]


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {message}", flush=True)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                data,
                handle,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_copy(source, destination):
    source = Path(source)
    destination = Path(destination)

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=destination.name + ".",
        suffix=".tmp",
        dir=str(destination.parent),
    )

    os.close(fd)

    temporary = Path(temporary_name)

    try:
        shutil.copyfile(source, temporary)
        os.replace(temporary, destination)

    finally:
        if temporary.exists():
            temporary.unlink()


def parse_timestamp(value):
    if value is None:
        return pd.NaT

    try:
        if pd.isna(value):
            return pd.NaT
    except (TypeError, ValueError):
        pass

    text = str(value).strip()

    if not text:
        return pd.NaT

    try:
        numeric = float(text)

        if not math.isfinite(numeric):
            return pd.NaT

        magnitude = abs(numeric)

        if magnitude >= 1e17:
            unit = "ns"
        elif magnitude >= 1e14:
            unit = "us"
        elif magnitude >= 1e11:
            unit = "ms"
        else:
            unit = "s"

        return pd.to_datetime(
            numeric,
            unit=unit,
            utc=True,
            errors="coerce",
        )

    except (ValueError, OverflowError):
        return pd.to_datetime(
            text,
            utc=True,
            errors="coerce",
        )


def timestamp_text(value):
    parsed = parse_timestamp(value)

    if pd.isna(parsed):
        return ""

    return parsed.isoformat()


def normalize_timeframe(value):
    text = str(value).strip().lower()

    aliases = {
        "1h": "h1",
        "60m": "h1",
        "h1": "h1",
        "4h": "h4",
        "240m": "h4",
        "h4": "h4",
        "1d": "d1",
        "24h": "d1",
        "d1": "d1",
    }

    return aliases.get(text, text)


def normalize_market(value):
    return str(value).strip().upper()


def optional_float(value):
    try:
        number = float(value)

        if math.isfinite(number):
            return number

    except (ValueError, TypeError):
        pass

    return None


def equivalent(left, right, field):
    left_text = (
        "" if left is None else str(left).strip()
    )

    right_text = (
        "" if right is None else str(right).strip()
    )

    if not left_text and not right_text:
        return True

    if field in {
        "entry_timestamp",
        "first_hit_timestamp",
    }:
        return (
            timestamp_text(left_text)
            == timestamp_text(right_text)
        )

    if field in NUMERIC_FIELDS:
        left_number = optional_float(left_text)
        right_number = optional_float(right_text)

        if left_number is None or right_number is None:
            return (
                left_number is None
                and right_number is None
                and not left_text
                and not right_text
            )

        return math.isclose(
            left_number,
            right_number,
            rel_tol=FLOAT_TOLERANCE_REL,
            abs_tol=FLOAT_TOLERANCE_ABS,
        )

    return left_text == right_text


def inspect_ohlcv_file(path):
    with Path(path).open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        required = {
            "market",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
        }

        if not reader.fieldnames:
            return None

        if not required.issubset(reader.fieldnames):
            return None

        first = next(reader, None)

        if not first:
            return None

        market = normalize_market(
            first.get("market", "")
        )

        if not market.startswith("KRW-"):
            return None

        return market


def discover_ohlcv(root):
    mapping = {}

    ohlcv_root = root / "data" / "ohlcv"

    if not ohlcv_root.is_dir():
        raise FileNotFoundError(
            f"OHLCV directory missing: {ohlcv_root}"
        )

    for timeframe in TIMEFRAMES:
        folder = ohlcv_root / timeframe

        if not folder.is_dir():
            raise FileNotFoundError(
                f"OHLCV timeframe directory missing: {folder}"
            )

        paths = sorted(folder.rglob("*.csv"))

        log(
            f"[SCAN] {timeframe} OHLCV files={len(paths):,}"
        )

        for path in paths:
            market = inspect_ohlcv_file(path)

            if market is None:
                continue

            key = (timeframe, market)

            if key in mapping:
                raise ValueError(
                    "Duplicate OHLCV source: "
                    f"{key}: {mapping[key]} / {path}"
                )

            mapping[key] = path

    return mapping


def load_ohlcv(path, market):
    frame = pd.read_csv(
        path,
        encoding=CSV_ENCODING,
        low_memory=False,
    )

    required = {
        "market",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
    }

    missing = required - set(frame.columns)

    if missing:
        raise ValueError(
            f"Missing OHLCV columns: {sorted(missing)}"
        )

    frame["market"] = (
        frame["market"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    if not frame["market"].eq(market).all():
        raise ValueError(
            f"OHLCV market mismatch: {path}"
        )

    frame["parsed_timestamp"] = (
        frame["timestamp"].map(parse_timestamp)
    )

    if frame["parsed_timestamp"].isna().any():
        raise ValueError(
            f"Invalid OHLCV timestamp: {path}"
        )

    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        )

    prices = frame[
        ["open", "high", "low", "close"]
    ]

    if prices.isna().any().any():
        raise ValueError(
            f"Invalid OHLCV price: {path}"
        )

    if (prices <= 0).any().any():
        raise ValueError(
            f"Nonpositive OHLCV price: {path}"
        )

    invalid = (
        (frame["high"] < frame["low"])
        | (frame["high"] < frame["open"])
        | (frame["high"] < frame["close"])
        | (frame["low"] > frame["open"])
        | (frame["low"] > frame["close"])
    )

    if invalid.any():
        raise ValueError(
            f"Invalid OHLCV range: {path}"
        )

    frame = frame.sort_values(
        "parsed_timestamp",
        kind="stable",
    ).reset_index(drop=True)

    if frame["parsed_timestamp"].duplicated().any():
        raise ValueError(
            f"Duplicate OHLCV timestamps: {path}"
        )

    timestamps = list(
        frame["parsed_timestamp"]
    )

    index = {
        timestamp: position
        for position, timestamp in enumerate(timestamps)
    }

    return {
        "frame": frame,
        "timestamps": timestamps,
        "index": index,
        "open": frame["open"].to_numpy(
            dtype=np.float64
        ),
        "high": frame["high"].to_numpy(
            dtype=np.float64
        ),
        "low": frame["low"].to_numpy(
            dtype=np.float64
        ),
        "close": frame["close"].to_numpy(
            dtype=np.float64
        ),
    }


def load_oos_configuration(root):
    report_root = (
        root
        / "data"
        / "reports"
        / "pattern_256_accumulation_candidate_oos"
    )

    candidates = [
        report_root / "candidate_oos_result.json",
        report_root / "candidate_oos_summary.csv",
    ]

    configuration = {
        "h1": None,
        "h4": None,
        "d1": None,
    }

    result_path = candidates[0]

    if result_path.is_file():
        with result_path.open(
            "r",
            encoding="utf-8-sig",
        ) as handle:
            data = json.load(handle)

        def scan(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    normalized = str(key).lower()

                    if normalized in {
                        "oos_start",
                        "oos_start_by_timeframe",
                        "oos_start_dates",
                    } and isinstance(child, dict):
                        for timeframe in TIMEFRAMES:
                            if timeframe in child:
                                parsed = parse_timestamp(
                                    child[timeframe]
                                )

                                if not pd.isna(parsed):
                                    configuration[
                                        timeframe
                                    ] = parsed

                    scan(child)

            elif isinstance(value, list):
                for child in value:
                    scan(child)

        scan(data)

    return configuration


def calculate_independent(
    reference,
    source,
    timeframe,
):
    frame = source["frame"]
    timestamps = source["timestamps"]
    timestamp_index = source["index"]

    signal_timestamp = parse_timestamp(
        reference["signal_timestamp"]
    )

    if pd.isna(signal_timestamp):
        raise ValueError(
            "Invalid reference signal timestamp"
        )

    if signal_timestamp not in timestamp_index:
        raise ValueError(
            "Signal timestamp missing in OHLCV: "
            f"{timeframe}/{reference['market']} "
            f"{signal_timestamp}"
        )

    horizon = int(reference["horizon"])

    if horizon not in HORIZONS:
        raise ValueError(
            f"Unsupported horizon: {horizon}"
        )

    signal_index = timestamp_index[
        signal_timestamp
    ]

    entry_index = signal_index + 1

    result = {
        "entry_timestamp": "",
        "entry_price": "",
        "target_price": "",
        "status": "",
        "hit_30pct": "",
        "first_hit_bars": "",
        "first_hit_timestamp": "",
        "max_high": "",
        "max_return_pct": "",
        "min_low": "",
        "max_drawdown_pct": "",
        "observed_bars": 0,
        "required_bars": horizon,
        "entry_time_gap": 0,
        "gap_count": 0,
    }

    if entry_index >= len(frame):
        result["status"] = "CENSORED"
        return result

    entry_timestamp = timestamps[
        entry_index
    ]

    entry_price = float(
        source["open"][entry_index]
    )

    if not math.isfinite(entry_price) or entry_price <= 0:
        result["status"] = "INVALID"
        return result

    target_price = (
        entry_price * TARGET_MULTIPLIER
    )

    result["entry_timestamp"] = (
        entry_timestamp.isoformat()
    )

    result["entry_price"] = entry_price
    result["target_price"] = target_price

    expected_delta = pd.Timedelta(
        seconds=TIMEFRAME_SECONDS[timeframe]
    )

    actual_delta = (
        entry_timestamp - signal_timestamp
    )

    if actual_delta != expected_delta:
        result["entry_time_gap"] = 1

    end_index = min(
        entry_index + horizon,
        len(frame),
    )

    observed = end_index - entry_index

    result["observed_bars"] = observed

    if observed == 0:
        result["status"] = "CENSORED"
        return result

    highs = source["high"][
        entry_index:end_index
    ]

    lows = source["low"][
        entry_index:end_index
    ]

    max_high = float(np.max(highs))
    min_low = float(np.min(lows))

    result["max_high"] = max_high
    result["min_low"] = min_low

    result["max_return_pct"] = (
        (max_high / entry_price) - 1.0
    ) * 100.0

    result["max_drawdown_pct"] = (
        (min_low / entry_price) - 1.0
    ) * 100.0

    window_timestamps = timestamps[
        entry_index:end_index
    ]

    gaps = 0

    for position in range(
        1,
        len(window_timestamps),
    ):
        difference = (
            window_timestamps[position]
            - window_timestamps[position - 1]
        )

        if difference != expected_delta:
            gaps += 1

    result["gap_count"] = gaps

    hit_positions = np.flatnonzero(
        highs >= target_price
    )

    # Reproduce original Clean V001:
    # HIT is checked before incomplete-window censoring.

    if len(hit_positions) > 0:
        first_position = int(
            hit_positions[0]
        )

        result["status"] = "HIT"
        result["hit_30pct"] = 1
        result["first_hit_bars"] = (
            first_position + 1
        )

        result["first_hit_timestamp"] = (
            timestamps[
                entry_index + first_position
            ].isoformat()
        )

    elif observed < horizon:
        result["status"] = "CENSORED"

    else:
        result["status"] = "MISS"
        result["hit_30pct"] = 0

    return result


def compare_reference(reference, independent):
    mismatches = []

    for field in COMPARISON_FIELDS:
        if not equivalent(
            reference.get(field, ""),
            independent.get(field, ""),
            field,
        ):
            mismatches.append(field)

    return mismatches


def append_diagnostic(
    writer,
    source_row,
    reference,
    category,
    field,
    reference_value,
    independent_value,
    details="",
):
    writer.writerow({
        "source_row": source_row,
        "timeframe": reference.get(
            "timeframe", ""
        ),
        "market": reference.get(
            "market", ""
        ),
        "signal_timestamp": reference.get(
            "signal_timestamp", ""
        ),
        "horizon": reference.get(
            "horizon", ""
        ),
        "category": category,
        "field": field,
        "reference_value": reference_value,
        "independent_value": independent_value,
        "details": details,
    })


def verify_reference_header(path):
    with path.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "Reference CSV has no header"
            )

        missing = (
            set(REFERENCE_COLUMNS)
            - set(reader.fieldnames)
        )

        if missing:
            raise ValueError(
                "Reference CSV missing columns: "
                f"{sorted(missing)}"
            )


def read_checkpoint(state_path):
    if not state_path.is_file():
        return None

    with state_path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        return json.load(handle)


def verify_partial_file(
    path,
    expected_columns,
    expected_rows,
    expected_hash,
):
    if not path.is_file():
        return False

    if sha256_file(path) != expected_hash:
        return False

    with path.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != expected_columns:
            return False

        count = sum(1 for _ in reader)

    return count == expected_rows


def save_checkpoint(
    checkpoint_root,
    source_hash,
    program_hash,
    processed_rows,
    trade_rows,
    diagnostic_rows,
    trades_path,
    diagnostics_path,
):
    state_path = (
        checkpoint_root / "state.json"
    )

    data = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "source_sha256": source_hash,
        "program_sha256": program_hash,
        "processed_rows": processed_rows,
        "trade_rows": trade_rows,
        "diagnostic_rows": diagnostic_rows,
        "trades_sha256": sha256_file(
            trades_path
        ),
        "diagnostics_sha256": sha256_file(
            diagnostics_path
        ),
        "updated_at_utc": utc_now(),
    }

    atomic_json(state_path, data)


def open_csv_append(path, columns, resume):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    mode = "a" if resume else "w"

    handle = path.open(
        mode,
        encoding=CSV_ENCODING,
        newline="",
    )

    writer = csv.DictWriter(
        handle,
        fieldnames=columns,
        extrasaction="ignore",
    )

    if not resume:
        writer.writeheader()

    return handle, writer


def calculate_summary(trades_path):
    stats = defaultdict(
        lambda: {
            "total": 0,
            "complete": 0,
            "hit": 0,
            "miss": 0,
            "censored": 0,
            "invalid": 0,
            "matched": 0,
            "mismatched": 0,
            "entry_time_gaps": 0,
            "observation_gaps": 0,
        }
    )

    with trades_path.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            key = (
                row["timeframe"],
                row["classification"],
                int(row["horizon"]),
            )

            item = stats[key]

            item["total"] += 1

            status = row["status"]

            if status == "HIT":
                item["hit"] += 1
                item["complete"] += 1

            elif status == "MISS":
                item["miss"] += 1
                item["complete"] += 1

            elif status == "CENSORED":
                item["censored"] += 1

            elif status == "INVALID":
                item["invalid"] += 1

            else:
                raise ValueError(
                    f"Unexpected status: {status}"
                )

            if row["match"] == "1":
                item["matched"] += 1
            else:
                item["mismatched"] += 1

            if int(row["entry_time_gap"]):
                item["entry_time_gaps"] += 1

            if int(row["gap_count"]):
                item["observation_gaps"] += 1

    rows = []

    for key in sorted(stats):
        timeframe, classification, horizon = key

        item = stats[key]

        complete = item["complete"]

        if complete:
            hit_rate = (
                item["hit"] / complete * 100.0
            )
        else:
            hit_rate = ""

        rows.append({
            "timeframe": timeframe,
            "classification": classification,
            "horizon": horizon,
            **item,
            "hit_rate_pct": hit_rate,
        })

    return rows


def write_csv_atomic(path, columns, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding=CSV_ENCODING,
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
    )

    parser.add_argument(
        "--smoke-test",
        action="store_true",
    )

    parser.add_argument(
        "--limit-events",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--limit-rows",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=CHECKPOINT_INTERVAL,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    started = time.monotonic()

    root = args.root.resolve()

    reference_path = (
        root
        / "data"
        / "reports"
        / "pattern_256_accumulation_30pct"
        / "validation_detail.csv"
    )

    if args.output is not None:
        output_root = args.output.resolve()
    else:
        output_root = (
            root
            / "data"
            / "reports"
            / "pattern_256_accumulation_candidate_independent_backtest"
        )

    if args.smoke_test:
        if args.output is None:
            output_root = (
                root
                / "data"
                / "reports"
                / "pattern_256_accumulation_candidate_independent_backtest_v005_test"
            )

    if not reference_path.is_file():
        raise FileNotFoundError(
            f"Reference missing: {reference_path}"
        )

    if args.limit_events < 0 or args.limit_rows < 0:
        raise ValueError(
            "Row limits must be nonnegative"
        )

    if args.checkpoint_interval <= 0:
        raise ValueError(
            "Checkpoint interval must be positive"
        )

    limit = (
        args.limit_events
        or args.limit_rows
    )

    if args.smoke_test and limit == 0:
        limit = 100

    output_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    checkpoint_root = (
        output_root / "checkpoints_v005"
    )

    checkpoint_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    partial_trades = (
        checkpoint_root / "trades.partial.csv"
    )

    partial_diagnostics = (
        checkpoint_root / "diagnostics.partial.csv"
    )

    state_path = (
        checkpoint_root / "state.json"
    )

    trades_path = (
        output_root
        / "candidate_independent_backtest_trades.csv"
    )

    diagnostics_path = (
        output_root
        / "candidate_independent_backtest_diagnostics.csv"
    )

    summary_path = (
        output_root
        / "candidate_independent_backtest_summary.csv"
    )

    result_path = (
        output_root
        / "candidate_independent_backtest_result.json"
    )

    manifest_path = (
        output_root
        / "candidate_independent_backtest_manifest.json"
    )

    log("=" * 70)
    log(f"{PROGRAM_NAME} | {PROGRAM_VERSION}")
    log("=" * 70)

    verify_reference_header(reference_path)

    source_hash_before = sha256_file(
        reference_path
    )

    program_path = Path(__file__).resolve()

    program_hash = sha256_file(
        program_path
    )

    log(
        f"[REFERENCE] SHA256={source_hash_before}"
    )

    log("[READ] OOS configuration")

    oos_configuration = load_oos_configuration(
        root
    )

    for timeframe in TIMEFRAMES:
        value = oos_configuration[timeframe]

        log(
            f"[OOS] {timeframe}: "
            f"{value.isoformat() if value is not None else 'NOT_AVAILABLE'}"
        )

    source_files = discover_ohlcv(root)

    source_hashes = {}

    # Only one OHLCV market is held in memory at a time.
    current_key = None
    current_source = None

    processed_rows = 0
    trade_rows = 0
    diagnostic_rows = 0

    checkpoint = None

    if not args.force:
        checkpoint = read_checkpoint(
            state_path
        )

    resume = False

    if checkpoint is not None:
        valid_identity = (
            checkpoint.get("version")
            == PROGRAM_VERSION
            and checkpoint.get("source_sha256")
            == source_hash_before
            and checkpoint.get("program_sha256")
            == program_hash
        )

        if not valid_identity:
            raise RuntimeError(
                "V005 checkpoint provenance mismatch. "
                "Resume refused. Use a new output directory "
                "or --force to start a fresh V005 run."
            )

        valid_trades = verify_partial_file(
            partial_trades,
            TRADE_COLUMNS,
            int(checkpoint["trade_rows"]),
            checkpoint["trades_sha256"],
        )

        valid_diagnostics = verify_partial_file(
            partial_diagnostics,
            DIAGNOSTIC_COLUMNS,
            int(checkpoint["diagnostic_rows"]),
            checkpoint["diagnostics_sha256"],
        )

        if not valid_trades or not valid_diagnostics:
            raise RuntimeError(
                "Checkpoint content integrity failure. "
                "Resume refused."
            )

        processed_rows = int(
            checkpoint["processed_rows"]
        )

        trade_rows = int(
            checkpoint["trade_rows"]
        )

        diagnostic_rows = int(
            checkpoint["diagnostic_rows"]
        )

        resume = True

        log(
            f"[RESUME] Verified rows: {processed_rows:,}"
        )

    trade_handle, trade_writer = open_csv_append(
        partial_trades,
        TRADE_COLUMNS,
        resume,
    )

    diagnostic_handle, diagnostic_writer = (
        open_csv_append(
            partial_diagnostics,
            DIAGNOSTIC_COLUMNS,
            resume,
        )
    )

    try:
        with reference_path.open(
            "r",
            encoding=CSV_ENCODING,
            newline="",
        ) as source_handle:
            reader = csv.DictReader(
                source_handle
            )

            for source_row, reference in enumerate(
                reader,
                start=1,
            ):
                if source_row <= processed_rows:
                    continue

                if limit and source_row > limit:
                    break

                timeframe = normalize_timeframe(
                    reference["timeframe"]
                )

                market = normalize_market(
                    reference["market"]
                )

                if timeframe not in TIMEFRAMES:
                    raise ValueError(
                        f"Unsupported timeframe: {timeframe}"
                    )

                key = (timeframe, market)

                if key not in source_files:
                    raise FileNotFoundError(
                        f"OHLCV source missing: {key}"
                    )

                if key != current_key:
                    current_source = None

                    path = source_files[key]

                    source_hash = sha256_file(
                        path
                    )

                    if key in source_hashes:
                        if (
                            source_hashes[key]
                            != source_hash
                        ):
                            raise RuntimeError(
                                "OHLCV changed during execution: "
                                f"{key}"
                            )

                    source_hashes[key] = (
                        source_hash
                    )

                    current_source = load_ohlcv(
                        path,
                        market,
                    )

                    current_key = key

                    log(
                        f"[OHLCV] {timeframe}/{market} "
                        f"candles={len(current_source['frame']):,}"
                    )

                independent = calculate_independent(
                    reference,
                    current_source,
                    timeframe,
                )

                mismatches = compare_reference(
                    reference,
                    independent,
                )

                output = {
                    "timeframe": timeframe,
                    "market": market,
                    "signal_timestamp": (
                        reference["signal_timestamp"]
                    ),
                    "classification": (
                        reference["classification"]
                    ),
                    "horizon": (
                        reference["horizon"]
                    ),
                    **independent,
                    "reference_status": (
                        reference["status"]
                    ),
                    "reference_hit_30pct": (
                        reference["hit_30pct"]
                    ),
                    "mismatch_fields": (
                        "|".join(mismatches)
                    ),
                    "match": (
                        0 if mismatches else 1
                    ),
                }

                trade_writer.writerow(
                    output
                )

                trade_rows += 1

                for field in mismatches:
                    append_diagnostic(
                        diagnostic_writer,
                        source_row,
                        reference,
                        "REFERENCE_MISMATCH",
                        field,
                        reference.get(field, ""),
                        independent.get(field, ""),
                    )

                    diagnostic_rows += 1

                if independent["entry_time_gap"]:
                    append_diagnostic(
                        diagnostic_writer,
                        source_row,
                        reference,
                        "DATA_TIME_GAP",
                        "entry_timestamp",
                        reference.get(
                            "entry_timestamp", ""
                        ),
                        independent[
                            "entry_timestamp"
                        ],
                        "Next available OHLCV candle "
                        "is not at the expected clock interval.",
                    )

                    diagnostic_rows += 1

                if independent["gap_count"]:
                    append_diagnostic(
                        diagnostic_writer,
                        source_row,
                        reference,
                        "OBSERVATION_TIME_GAP",
                        "gap_count",
                        "",
                        independent["gap_count"],
                        "Nonstandard intervals inside "
                        "the observation window.",
                    )

                    diagnostic_rows += 1

                processed_rows = source_row

                if (
                    processed_rows
                    % args.checkpoint_interval
                    == 0
                ):
                    trade_handle.flush()
                    diagnostic_handle.flush()

                    os.fsync(
                        trade_handle.fileno()
                    )

                    os.fsync(
                        diagnostic_handle.fileno()
                    )

                    save_checkpoint(
                        checkpoint_root,
                        source_hash_before,
                        program_hash,
                        processed_rows,
                        trade_rows,
                        diagnostic_rows,
                        partial_trades,
                        partial_diagnostics,
                    )

                    log(
                        f"[CHECKPOINT] source={processed_rows:,} "
                        f"trades={trade_rows:,}"
                    )

        trade_handle.flush()
        diagnostic_handle.flush()

        os.fsync(
            trade_handle.fileno()
        )

        os.fsync(
            diagnostic_handle.fileno()
        )

        save_checkpoint(
            checkpoint_root,
            source_hash_before,
            program_hash,
            processed_rows,
            trade_rows,
            diagnostic_rows,
            partial_trades,
            partial_diagnostics,
        )

    finally:
        trade_handle.close()
        diagnostic_handle.close()

    # Confirm that reference data has not changed.
    source_hash_after = sha256_file(
        reference_path
    )

    if source_hash_after != source_hash_before:
        raise RuntimeError(
            "Reference validation CSV changed."
        )

    for key, original_hash in source_hashes.items():
        current_hash = sha256_file(
            source_files[key]
        )

        if current_hash != original_hash:
            raise RuntimeError(
                f"OHLCV source changed: {key}"
            )

    log("[WRITE] Final trade CSV")

    atomic_copy(
        partial_trades,
        trades_path,
    )

    atomic_copy(
        partial_diagnostics,
        diagnostics_path,
    )

    summary_rows = calculate_summary(
        trades_path
    )

    write_csv_atomic(
        summary_path,
        SUMMARY_COLUMNS,
        summary_rows,
    )

    total_matched = sum(
        int(row["matched"])
        for row in summary_rows
    )

    total_mismatched = sum(
        int(row["mismatched"])
        for row in summary_rows
    )

    total_entry_gaps = sum(
        int(row["entry_time_gaps"])
        for row in summary_rows
    )

    total_observation_gaps = sum(
        int(row["observation_gaps"])
        for row in summary_rows
    )

    if total_matched + total_mismatched != trade_rows:
        raise RuntimeError(
            "Summary row count mismatch."
        )

    is_limited = bool(
        args.smoke_test or limit
    )

    independent_oos_proven = (
        not is_limited
        and trade_rows > 0
        and total_mismatched == 0
        and all(
            oos_configuration[timeframe]
            is not None
            for timeframe in TIMEFRAMES
        )
    )

    if total_mismatched > 0:
        execution_status = "FAIL"

    elif is_limited:
        execution_status = "SMOKE_TEST_PASS"

    else:
        execution_status = "PASS"

    elapsed = round(
        time.monotonic() - started,
        3,
    )

    result = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "status": execution_status,
        "execution_status": execution_status,
        "created_at_utc": utc_now(),
        "source_rows": processed_rows,
        "oos_rows": trade_rows,
        "matched_source_rows": trade_rows,
        "output_rows": trade_rows,
        "matched_rows": total_matched,
        "mismatched_rows": total_mismatched,
        "status_mismatches": None,
        "entry_price_mismatches": None,
        "first_hit_mismatches": None,
        "observed_bars_mismatches": None,
        "max_return_mismatches": None,
        "max_drawdown_mismatches": None,
        "entry_time_mismatches": None,
        "entry_time_gaps": total_entry_gaps,
        "observation_gaps": total_observation_gaps,
        "independent_oos_proven": (
            independent_oos_proven
        ),
        "approved_for_live_trading": False,
        "live_trading_approved": False,
        "smoke_test": is_limited,
        "entry_rule": "NEXT_AVAILABLE_OHLCV_ROW_OPEN",
        "horizon_rule": "AVAILABLE_OHLCV_ROW_COUNT",
        "target_return_pct": TARGET_RETURN_PCT,
        "reference_program": (
            "validate_pattern_256_accumulation_30pct.py"
        ),
        "reference_version": "Clean V001",
        "reference_sha256": source_hash_before,
        "elapsed_seconds": elapsed,
    }

    # Count individual mismatches without hiding them.
    mismatch_counts = {
        field: 0
        for field in COMPARISON_FIELDS
    }

    with trades_path.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(
            handle
        )

        for row in reader:
            fields = [
                value
                for value in row[
                    "mismatch_fields"
                ].split("|")
                if value
            ]

            for field in fields:
                mismatch_counts[field] += 1

    result.update({
        "status_mismatches": (
            mismatch_counts["status"]
        ),
        "entry_price_mismatches": (
            mismatch_counts["entry_price"]
        ),
        "first_hit_mismatches": (
            mismatch_counts["first_hit_bars"]
            + mismatch_counts["first_hit_timestamp"]
        ),
        "observed_bars_mismatches": (
            mismatch_counts["observed_bars"]
        ),
        "max_return_mismatches": (
            mismatch_counts["max_return_pct"]
        ),
        "max_drawdown_mismatches": (
            mismatch_counts["max_drawdown_pct"]
        ),
        "entry_time_mismatches": (
            mismatch_counts["entry_timestamp"]
        ),
        "mismatch_counts": mismatch_counts,
    })

    atomic_json(
        result_path,
        result,
    )

    # Compatibility output names.
    aliases = {
        "backtest_trades.csv": trades_path,
        "backtest_summary.csv": summary_path,
        "backtest_result.json": result_path,
    }

    for alias, source in aliases.items():
        atomic_copy(
            source,
            output_root / alias,
        )

    output_files = [
        trades_path,
        diagnostics_path,
        summary_path,
        result_path,
    ]

    for alias in aliases:
        output_files.append(
            output_root / alias
        )

    manifest = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "created_at_utc": utc_now(),
        "reference_path": str(
            reference_path
        ),
        "reference_sha256": source_hash_before,
        "program_sha256": program_hash,
        "ohlcv_sources": [
            {
                "timeframe": key[0],
                "market": key[1],
                "path": str(
                    source_files[key]
                ),
                "sha256": source_hashes[key],
            }
            for key in sorted(source_hashes)
        ],
        "outputs": {
            path.name: sha256_file(path)
            for path in output_files
        },
        "approved_for_live_trading": False,
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    atomic_copy(
        manifest_path,
        output_root / "backtest_manifest.json",
    )

    log("=" * 70)
    log(
        f"EXECUTION STATUS: {execution_status}"
    )
    log(
        f"SOURCE ROWS: {processed_rows:,}"
    )
    log(
        f"OOS ROWS: {trade_rows:,}"
    )
    log(
        f"MATCHED SOURCE ROWS: {trade_rows:,}"
    )
    log(
        f"OUTPUT ROWS: {trade_rows:,}"
    )
    log(
        f"MATCHED ROWS: {total_matched:,}"
    )
    log(
        f"MISMATCHED ROWS: {total_mismatched:,}"
    )
    log(
        "STATUS MISMATCHES: "
        f"{result['status_mismatches']:,}"
    )
    log(
        "ENTRY_PRICE_MISMATCHES: "
        f"{result['entry_price_mismatches']:,}"
    )
    log(
        "FIRST_HIT_MISMATCHES: "
        f"{result['first_hit_mismatches']:,}"
    )
    log(
        "OBSERVED_BARS_MISMATCHES: "
        f"{result['observed_bars_mismatches']:,}"
    )
    log(
        "MAX_RETURN_MISMATCHES: "
        f"{result['max_return_mismatches']:,}"
    )
    log(
        "MAX_DRAWDOWN_MISMATCHES: "
        f"{result['max_drawdown_mismatches']:,}"
    )
    log(
        "ENTRY_TIME_MISMATCHES: "
        f"{result['entry_time_mismatches']:,}"
    )
    log(
        f"ENTRY TIME GAPS: {total_entry_gaps:,}"
    )
    log(
        f"OBSERVATION GAPS: {total_observation_gaps:,}"
    )
    log(
        "INDEPENDENT OOS PROVEN: "
        f"{str(independent_oos_proven).upper()}"
    )
    log("LIVE TRADING APPROVED: FALSE")
    log(
        f"ELAPSED SECONDS: {elapsed}"
    )
    log(
        f"[OUTPUT] {output_root}"
    )
    log("=" * 70)

    if execution_status == "FAIL":
        return 1

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log(
            "[STOP] Interrupted. "
            "Last verified checkpoint remains available."
        )
        sys.exit(130)

    except Exception as exc:
        log(
            f"[FAIL] {type(exc).__name__}: {exc}"
        )
        traceback.print_exc()
        sys.exit(1)
