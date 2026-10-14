
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
backtest_pattern_256_accumulation_candidate_independent.py

UPBIT SURGE MONITOR
PATTERN 256 + ACCUMULATION
INDEPENDENT CANDIDATE BACKTEST
CLEAN V001

IMPORTANT
---------
This program evaluates frozen candidate rules on
historical OHLCV data.

It does not:
- Modify original OHLCV files
- Modify research reports
- Refit candidate thresholds
- Execute trades
- Approve candidates for live trading
- Prove that supplied features are free from leakage

Required inputs
---------------
1. Frozen candidate rules JSON
2. Historical candidate feature CSV
3. OHLCV CSV files
4. Research end timestamp
5. Holdout start timestamp

Feature CSV required columns:
    market
    timeframe
    timestamp
    pattern_256_score
    accumulation_score

OHLCV CSV required columns:
    timestamp (or candle_date_time_utc)
    opening_price (or open)
    high_price (or high)
    low_price (or low)
    trade_price (or close)

Example rules JSON:
{
  "version": "frozen-v1",
  "frozen_before_holdout": true,
  "rules": [
    {
      "candidate_id": "CANDIDATE_001",
      "timeframe": "h1",
      "pattern_256_min": 70,
      "accumulation_min": 70
    }
  ]
}

IMPORTANT:
The example thresholds above are illustrative only.
They are NOT verified project candidate thresholds.

Execution example:
py backtest_pattern_256_accumulation_candidate_independent.py ^
  --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor" ^
  --signals "C:\\PATH\\TO\\candidate_features.csv" ^
  --rules "C:\\PATH\\TO\\frozen_candidate_rules.json" ^
  --research-end "2025-12-31T23:00:00Z" ^
  --holdout-start "2026-01-01T00:00:00Z"

Replace the example paths and dates with verified values.

Output:
data/reports/
  pattern_256_accumulation_candidate_independent_backtest/
    independent_backtest_trades.csv
    independent_backtest_summary.csv
    independent_backtest_checks.csv
    independent_backtest_result.json
    independent_backtest_manifest.json

Dependencies:
    Python 3.11+
    Standard library only
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import sys
import tempfile

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROGRAM = (
    "backtest_pattern_256_accumulation_"
    "candidate_independent.py"
)

VERSION = "Clean V001"

OUTPUT_RELATIVE = (
    Path("data")
    / "reports"
    / "pattern_256_accumulation_candidate_independent_backtest"
)

TIMEFRAME_ALIASES = {
    "h1": "h1",
    "1h": "h1",
    "h4": "h4",
    "4h": "h4",
    "d1": "d1",
    "1d": "d1",
}

TIMEFRAME_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

HORIZONS = (24, 72, 168)

TARGET_RETURN = 0.30

TRADE_FIELDS = (
    "candidate_id",
    "market",
    "timeframe",
    "signal_time",
    "entry_time",
    "entry_price",
    "horizon",
    "target_price",
    "status",
    "hit_30pct",
    "target_hit_bar",
    "max_favorable_return",
    "max_adverse_return",
    "observed_bars",
    "required_bars",
)

SUMMARY_FIELDS = (
    "candidate_id",
    "timeframe",
    "horizon",
    "total_events",
    "complete_events",
    "hit_events",
    "miss_events",
    "incomplete_events",
    "hit_rate",
    "average_max_favorable_return",
    "average_max_adverse_return",
    "status",
)

CHECK_FIELDS = (
    "check_id",
    "status",
    "detail",
)

OUTPUT_NAMES = (
    "independent_backtest_trades.csv",
    "independent_backtest_summary.csv",
    "independent_backtest_checks.csv",
    "independent_backtest_result.json",
    "independent_backtest_manifest.json",
)


# ============================================================
# BASIC UTILITIES
# ============================================================

def log(message: str) -> None:
    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(
        f"{now} | {message}",
        flush=True,
    )


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat(
        timespec="seconds"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            chunk = stream.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def parse_timestamp(value: Any) -> datetime:
    text = str(value).strip()

    if not text:
        raise ValueError(
            "Empty timestamp."
        )

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        result = datetime.fromisoformat(
            text
        )
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO timestamp: {value}"
        ) from exc

    if result.tzinfo is None:
        raise ValueError(
            "Timezone required for timestamp: "
            f"{value}"
        )

    return result.astimezone(
        timezone.utc
    )


def parse_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid numeric value: {value}"
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            f"Non-finite numeric value: {value}"
        )

    return result


def canonical_timeframe(value: Any) -> str:
    text = str(value).strip().lower()

    if text not in TIMEFRAME_ALIASES:
        raise ValueError(
            f"Unsupported timeframe: {value}"
        )

    return TIMEFRAME_ALIASES[text]


def add_check(
    checks: list[dict[str, str]],
    check_id: str,
    status: str,
    detail: str,
) -> None:
    checks.append({
        "check_id": check_id,
        "status": status,
        "detail": detail,
    })

    log(
        f"[{status}] {check_id}: {detail}"
    )


# ============================================================
# CSV / JSON
# ============================================================

def read_csv(
    path: Path,
) -> tuple[list[str], list[dict[str, str]]]:
    if not path.is_file():
        raise FileNotFoundError(
            f"CSV missing: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                f"CSV header missing: {path}"
            )

        columns = list(
            reader.fieldnames
        )

        if len(columns) != len(set(columns)):
            raise ValueError(
                f"Duplicate CSV columns: {path}"
            )

        rows = []

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            if None in row:
                raise ValueError(
                    "Unexpected extra CSV columns: "
                    f"{path}, line {line_number}"
                )

            if any(
                value is None
                for value in row.values()
            ):
                raise ValueError(
                    "Missing CSV field: "
                    f"{path}, line {line_number}"
                )

            rows.append(row)

    return columns, rows


def read_json(path: Path) -> Any:
    if not path.is_file():
        raise FileNotFoundError(
            f"JSON missing: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        return json.load(stream)


def require_columns(
    columns: list[str],
    required: tuple[str, ...],
    source: str,
) -> None:
    missing = [
        name
        for name in required
        if name not in columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns in {source}: "
            + ", ".join(missing)
        )


# ============================================================
# ATOMIC OUTPUT
# ============================================================

def atomic_write_text(
    path: Path,
    content: str,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
            delete=False,
        ) as stream:
            temporary = Path(
                stream.name
            )

            stream.write(content)
            stream.flush()
            os.fsync(
                stream.fileno()
            )

        os.replace(
            temporary,
            path,
        )

    finally:
        if (
            temporary is not None
            and temporary.exists()
        ):
            temporary.unlink()


def write_csv(
    path: Path,
    fields: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    buffer = io.StringIO(
        newline=""
    )

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        extrasaction="ignore",
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    atomic_write_text(
        path,
        buffer.getvalue(),
    )


def write_json(
    path: Path,
    payload: dict[str, Any],
) -> None:
    atomic_write_text(
        path,
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


# ============================================================
# FROZEN RULES
# ============================================================

def load_rules(
    path: Path,
) -> list[dict[str, Any]]:
    payload = read_json(path)

    if not isinstance(payload, dict):
        raise ValueError(
            "Frozen rules must be a JSON object."
        )

    if payload.get(
        "frozen_before_holdout"
    ) is not True:
        raise ValueError(
            "Frozen rules declaration missing or false."
        )

    raw_rules = payload.get(
        "rules"
    )

    if (
        not isinstance(raw_rules, list)
        or not raw_rules
    ):
        raise ValueError(
            "Frozen rules list is empty."
        )

    rules = []
    seen = set()

    for item in raw_rules:
        if not isinstance(item, dict):
            raise ValueError(
                "Invalid candidate rule object."
            )

        required = (
            "candidate_id",
            "timeframe",
            "pattern_256_min",
            "accumulation_min",
        )

        missing = [
            name
            for name in required
            if name not in item
        ]

        if missing:
            raise ValueError(
                "Candidate rule fields missing: "
                + ", ".join(missing)
            )

        candidate_id = str(
            item["candidate_id"]
        ).strip()

        if not candidate_id:
            raise ValueError(
                "Empty candidate ID."
            )

        timeframe = canonical_timeframe(
            item["timeframe"]
        )

        pattern_min = parse_float(
            item["pattern_256_min"]
        )

        accumulation_min = parse_float(
            item["accumulation_min"]
        )

        key = (
            candidate_id,
            timeframe,
        )

        if key in seen:
            raise ValueError(
                f"Duplicate frozen candidate: {key}"
            )

        seen.add(key)

        rules.append({
            "candidate_id": candidate_id,
            "timeframe": timeframe,
            "pattern_256_min": pattern_min,
            "accumulation_min": accumulation_min,
        })

    return rules


# ============================================================
# SIGNAL FEATURE DATA
# ============================================================

def load_signals(
    path: Path,
    holdout_start: datetime,
) -> tuple[
    list[dict[str, Any]],
    dict[str, int],
]:
    columns, rows = read_csv(
        path
    )

    require_columns(
        columns,
        (
            "market",
            "timeframe",
            "timestamp",
            "pattern_256_score",
            "accumulation_score",
        ),
        str(path),
    )

    signals = []
    seen = set()

    excluded_before_holdout = 0
    duplicate_count = 0

    for row in rows:
        market = str(
            row["market"]
        ).strip().upper()

        if not market.startswith(
            "KRW-"
        ):
            raise ValueError(
                f"Unexpected market: {market}"
            )

        timeframe = canonical_timeframe(
            row["timeframe"]
        )

        timestamp = parse_timestamp(
            row["timestamp"]
        )

        pattern_score = parse_float(
            row["pattern_256_score"]
        )

        accumulation_score = parse_float(
            row["accumulation_score"]
        )

        if timestamp < holdout_start:
            excluded_before_holdout += 1
            continue

        key = (
            market,
            timeframe,
            timestamp,
        )

        if key in seen:
            duplicate_count += 1
            continue

        seen.add(key)

        signals.append({
            "market": market,
            "timeframe": timeframe,
            "timestamp": timestamp,
            "pattern_256_score": pattern_score,
            "accumulation_score": accumulation_score,
        })

    signals.sort(
        key=lambda row: (
            row["market"],
            row["timeframe"],
            row["timestamp"],
        )
    )

    return signals, {
        "source_rows": len(rows),
        "holdout_rows": len(signals),
        "excluded_before_holdout": (
            excluded_before_holdout
        ),
        "duplicate_rows": duplicate_count,
    }


# ============================================================
# OHLCV LOADING
# ============================================================

def find_price_column(
    columns: list[str],
    aliases: tuple[str, ...],
) -> str:
    for name in aliases:
        if name in columns:
            return name

    raise ValueError(
        "OHLCV required column missing. "
        f"Expected one of: {aliases}"
    )


def load_ohlcv(
    path: Path,
) -> list[dict[str, Any]]:
    columns, rows = read_csv(
        path
    )

    time_column = find_price_column(
        columns,
        (
            "candle_date_time_utc",
            "timestamp",
        ),
    )

    open_column = find_price_column(
        columns,
        (
            "opening_price",
            "open",
        ),
    )

    high_column = find_price_column(
        columns,
        (
            "high_price",
            "high",
        ),
    )

    low_column = find_price_column(
        columns,
        (
            "low_price",
            "low",
        ),
    )

    close_column = find_price_column(
        columns,
        (
            "trade_price",
            "close",
        ),
    )

    candles = []

    seen = set()

    for row in rows:
        timestamp = parse_timestamp(
            row[time_column]
        )

        if timestamp in seen:
            raise ValueError(
                "Duplicate OHLCV timestamp: "
                f"{path}, {timestamp}"
            )

        seen.add(timestamp)

        opening = parse_float(
            row[open_column]
        )

        high = parse_float(
            row[high_column]
        )

        low = parse_float(
            row[low_column]
        )

        close = parse_float(
            row[close_column]
        )

        if min(
            opening,
            high,
            low,
            close,
        ) <= 0:
            raise ValueError(
                f"Non-positive OHLCV price: {path}"
            )

        if high < max(
            opening,
            close,
            low,
        ):
            raise ValueError(
                f"Invalid OHLCV high: {path}"
            )

        if low > min(
            opening,
            close,
            high,
        ):
            raise ValueError(
                f"Invalid OHLCV low: {path}"
            )

        candles.append({
            "timestamp": timestamp,
            "open": opening,
            "high": high,
            "low": low,
            "close": close,
        })

    candles.sort(
        key=lambda item: item["timestamp"]
    )

    return candles


def resolve_ohlcv_file(
    ohlcv_root: Path,
    timeframe: str,
    market: str,
) -> Path:
    directory = (
        ohlcv_root / timeframe
    )

    candidates = (
        directory / f"{market}.csv",
        directory / f"{market.replace('-', '_')}.csv",
    )

    found = [
        path
        for path in candidates
        if path.is_file()
    ]

    if len(found) != 1:
        raise FileNotFoundError(
            "Unable to resolve exactly one OHLCV file "
            f"for {market}/{timeframe}. "
            f"Checked: {candidates}"
        )

    return found[0]


# ============================================================
# SIGNAL SELECTION
# ============================================================

def select_candidate_signals(
    signals: list[dict[str, Any]],
    rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected = []

    rules_by_timeframe = defaultdict(
        list
    )

    for rule in rules:
        rules_by_timeframe[
            rule["timeframe"]
        ].append(rule)

    for signal in signals:
        candidates = rules_by_timeframe.get(
            signal["timeframe"],
            [],
        )

        for rule in candidates:
            if (
                signal["pattern_256_score"]
                < rule["pattern_256_min"]
            ):
                continue

            if (
                signal["accumulation_score"]
                < rule["accumulation_min"]
            ):
                continue

            selected.append({
                **signal,
                "candidate_id": (
                    rule["candidate_id"]
                ),
            })

    selected.sort(
        key=lambda row: (
            row["candidate_id"],
            row["market"],
            row["timeframe"],
            row["timestamp"],
        )
    )

    return selected


# ============================================================
# BACKTEST
# ============================================================

def backtest_event(
    signal: dict[str, Any],
    candles: list[dict[str, Any]],
    index_by_time: dict[datetime, int],
    horizon: int,
) -> dict[str, Any]:
    signal_time = signal[
        "timestamp"
    ]

    if signal_time not in index_by_time:
        raise ValueError(
            "Signal timestamp not present in OHLCV: "
            f"{signal['market']} "
            f"{signal['timeframe']} "
            f"{signal_time}"
        )

    signal_index = index_by_time[
        signal_time
    ]

    entry_index = (
        signal_index + 1
    )

    if entry_index >= len(candles):
        raise ValueError(
            "Signal has no next candle for entry."
        )

    seconds = TIMEFRAME_SECONDS[
        signal["timeframe"]
    ]

    expected_entry_time = (
        signal_time.timestamp()
        + seconds
    )

    entry_candle = candles[
        entry_index
    ]

    if (
        entry_candle["timestamp"].timestamp()
        != expected_entry_time
    ):
        raise ValueError(
            "Missing next candle after signal. "
            "Entry cannot be assumed."
        )

    entry_price = entry_candle[
        "open"
    ]

    target_price = (
        entry_price
        * (1.0 + TARGET_RETURN)
    )

    observed = 0
    highest = entry_price
    lowest = entry_price
    hit_bar = None

    # Include entry candle as the first
    # observable trading candle.
    #
    # The high and low are both known only
    # after that candle closes.
    #
    # No exact intrabar order is inferred.

    for offset in range(
        horizon
    ):
        index = (
            entry_index + offset
        )

        if index >= len(candles):
            break

        candle = candles[index]

        expected_timestamp = (
            entry_candle["timestamp"].timestamp()
            + offset * seconds
        )

        if (
            candle["timestamp"].timestamp()
            != expected_timestamp
        ):
            # Missing candles cause censoring.
            break

        observed += 1

        highest = max(
            highest,
            candle["high"],
        )

        lowest = min(
            lowest,
            candle["low"],
        )

        if (
            hit_bar is None
            and candle["high"] >= target_price
        ):
            hit_bar = (
                offset + 1
            )

    complete = (
        observed == horizon
    )

    if complete:
        status = (
            "HIT"
            if hit_bar is not None
            else "MISS"
        )

        hit_30pct = (
            1 if hit_bar is not None
            else 0
        )
    else:
        # Even if an early target hit was observed,
        # keep the event censored to preserve the
        # complete-horizon evaluation contract.
        status = "INCOMPLETE"
        hit_30pct = ""

    favorable = (
        highest / entry_price
    ) - 1.0

    adverse = (
        lowest / entry_price
    ) - 1.0

    return {
        "candidate_id": (
            signal["candidate_id"]
        ),
        "market": signal["market"],
        "timeframe": (
            signal["timeframe"]
        ),
        "signal_time": (
            signal_time.isoformat()
        ),
        "entry_time": (
            entry_candle["timestamp"].isoformat()
        ),
        "entry_price": entry_price,
        "horizon": horizon,
        "target_price": target_price,
        "status": status,
        "hit_30pct": hit_30pct,
        "target_hit_bar": (
            "" if hit_bar is None
            else hit_bar
        ),
        "max_favorable_return": favorable,
        "max_adverse_return": adverse,
        "observed_bars": observed,
        "required_bars": horizon,
    }


def run_backtest(
    selected_signals: list[dict[str, Any]],
    ohlcv_root: Path,
) -> tuple[
    list[dict[str, Any]],
    dict[str, str],
    dict[str, int],
]:
    grouped = defaultdict(
        list
    )

    for signal in selected_signals:
        key = (
            signal["market"],
            signal["timeframe"],
        )

        grouped[key].append(
            signal
        )

    trades = []
    input_hashes = {}
    suppressed = 0
    no_entry = 0

    for (
        market,
        timeframe,
    ), signals in sorted(
        grouped.items()
    ):
        path = resolve_ohlcv_file(
            ohlcv_root,
            timeframe,
            market,
        )

        log(
            f"[OHLCV] {market} / {timeframe}"
        )

        input_hashes[
            str(path.resolve())
        ] = sha256_file(
            path
        )

        candles = load_ohlcv(
            path
        )

        index_by_time = {
            candle["timestamp"]: index
            for index, candle in enumerate(
                candles
            )
        }

        # For each candidate/market/timeframe,
        # keep at most one event within the
        # maximum evaluation horizon.
        #
        # This reduces overlapping observations,
        # but does not make cross-market or
        # cross-timeframe events independent.

        last_selected_index = {}

        signals.sort(
            key=lambda item: (
                item["timestamp"],
                item["candidate_id"],
            )
        )

        for signal in signals:
            candidate_id = (
                signal["candidate_id"]
            )

            timestamp = signal[
                "timestamp"
            ]

            if timestamp not in index_by_time:
                raise ValueError(
                    "Signal timestamp missing in OHLCV: "
                    f"{market}/{timeframe}/{timestamp}"
                )

            signal_index = index_by_time[
                timestamp
            ]

            previous = last_selected_index.get(
                candidate_id
            )

            if (
                previous is not None
                and signal_index - previous
                <= max(HORIZONS)
            ):
                suppressed += 1
                continue

            if signal_index + 1 >= len(candles):
                no_entry += 1
                continue

            # The next candle must be contiguous.
            expected_next = (
                timestamp.timestamp()
                + TIMEFRAME_SECONDS[timeframe]
            )

            actual_next = candles[
                signal_index + 1
            ]["timestamp"].timestamp()

            if actual_next != expected_next:
                no_entry += 1
                continue

            last_selected_index[
                candidate_id
            ] = signal_index

            for horizon in HORIZONS:
                result = backtest_event(
                    signal,
                    candles,
                    index_by_time,
                    horizon,
                )

                trades.append(
                    result
                )

    trades.sort(
        key=lambda row: (
            row["candidate_id"],
            row["timeframe"],
            row["market"],
            row["signal_time"],
            row["horizon"],
        )
    )

    return (
        trades,
        input_hashes,
        {
            "overlap_suppressed": suppressed,
            "missing_entry_excluded": no_entry,
        },
    )


# ============================================================
# SUMMARY
# ============================================================

def average(
    values: list[float],
) -> float | None:
    if not values:
        return None

    return sum(values) / len(values)


def build_summary(
    trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped = defaultdict(
        list
    )

    for row in trades:
        key = (
            row["candidate_id"],
            row["timeframe"],
            row["horizon"],
        )

        grouped[key].append(
            row
        )

    summary = []

    for key, rows in sorted(
        grouped.items()
    ):
        candidate_id, timeframe, horizon = key

        complete = [
            row
            for row in rows
            if row["status"] in (
                "HIT",
                "MISS",
            )
        ]

        hits = sum(
            1
            for row in complete
            if row["status"] == "HIT"
        )

        misses = sum(
            1
            for row in complete
            if row["status"] == "MISS"
        )

        incomplete = (
            len(rows) - len(complete)
        )

        hit_rate = (
            hits / len(complete)
            if complete
            else None
        )

        favorable = average([
            row["max_favorable_return"]
            for row in complete
        ])

        adverse = average([
            row["max_adverse_return"]
            for row in complete
        ])

        # Descriptive results only.
        # No minimum sample-size or statistical
        # significance claim is made here.

        status = (
            "DESCRIPTIVE_ONLY"
            if complete
            else "INSUFFICIENT_DATA"
        )

        summary.append({
            "candidate_id": candidate_id,
            "timeframe": timeframe,
            "horizon": horizon,
            "total_events": len(rows),
            "complete_events": len(complete),
            "hit_events": hits,
            "miss_events": misses,
            "incomplete_events": incomplete,
            "hit_rate": (
                "" if hit_rate is None
                else hit_rate
            ),
            "average_max_favorable_return": (
                "" if favorable is None
                else favorable
            ),
            "average_max_adverse_return": (
                "" if adverse is None
                else adverse
            ),
            "status": status,
        })

    return summary


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Frozen candidate OHLCV independent "
            "holdout backtest."
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help="Production project root.",
    )

    parser.add_argument(
        "--signals",
        required=True,
        help="Candidate feature CSV path.",
    )

    parser.add_argument(
        "--rules",
        required=True,
        help="Frozen candidate rules JSON path.",
    )

    parser.add_argument(
        "--research-end",
        required=True,
        help=(
            "Verified research end timestamp "
            "with timezone."
        ),
    )

    parser.add_argument(
        "--holdout-start",
        required=True,
        help=(
            "Holdout start timestamp "
            "with timezone."
        ),
    )

    parser.add_argument(
        "--ohlcv-root",
        default=None,
        help=(
            "OHLCV root containing "
            "h1/h4/d1 directories."
        ),
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory.",
    )

    return parser.parse_args()


# ============================================================
# INPUT / OUTPUT SAFETY
# ============================================================

def verify_hashes(
    snapshots: dict[str, str],
) -> None:
    for name, expected in snapshots.items():
        path = Path(
            name
        )

        if not path.is_file():
            raise FileNotFoundError(
                f"Input disappeared: {path}"
            )

        actual = sha256_file(
            path
        )

        if actual != expected:
            raise RuntimeError(
                f"Input SHA256 changed: {path}"
            )


def prepare_output(
    output_dir: Path,
    protected_paths: list[Path],
) -> None:
    output_dir = output_dir.resolve()

    for protected in protected_paths:
        protected = protected.resolve()

        if (
            output_dir == protected
            or output_dir in protected.parents
            or protected in output_dir.parents
        ):
            raise ValueError(
                "Output directory overlaps protected input: "
                f"{protected}"
            )

    if output_dir.exists():
        if not output_dir.is_dir():
            raise ValueError(
                "Output path is not a directory."
            )

        if any(
            output_dir.iterdir()
        ):
            raise FileExistsError(
                "Output directory is not empty. "
                "Refusing overwrite: "
                f"{output_dir}"
            )

    else:
        output_dir.mkdir(
            parents=True,
            exist_ok=False,
        )


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    args = parse_args()

    log("=" * 70)
    log(PROGRAM)
    log(VERSION)
    log("=" * 70)

    root = Path(
        args.root
    ).expanduser().resolve()

    if not root.is_dir():
        raise FileNotFoundError(
            f"Project root missing: {root}"
        )

    signals_path = Path(
        args.signals
    ).expanduser().resolve()

    rules_path = Path(
        args.rules
    ).expanduser().resolve()

    if args.ohlcv_root:
        ohlcv_root = Path(
            args.ohlcv_root
        ).expanduser().resolve()
    else:
        ohlcv_root = (
            root / "data" / "ohlcv"
        ).resolve()

    if not ohlcv_root.is_dir():
        raise FileNotFoundError(
            f"OHLCV directory missing: {ohlcv_root}"
        )

    if args.output_dir:
        output_dir = Path(
            args.output_dir
        ).expanduser().resolve()
    else:
        output_dir = (
            root / OUTPUT_RELATIVE
        ).resolve()

    research_end = parse_timestamp(
        args.research_end
    )

    holdout_start = parse_timestamp(
        args.holdout_start
    )

    if holdout_start <= research_end:
        raise ValueError(
            "Holdout start must be strictly "
            "after research end."
        )

    checks = []

    add_check(
        checks,
        "holdout_chronology",
        "PASS",
        (
            f"research_end={research_end.isoformat()}, "
            f"holdout_start={holdout_start.isoformat()}"
        ),
    )

    # The chronological separation is checked,
    # but whether the dates accurately describe
    # prior research must be independently audited.

    add_check(
        checks,
        "historical_research_boundary_proof",
        "NOT_VERIFIED",
        (
            "User-supplied research boundary "
            "has not been independently audited."
        ),
    )

    log("[READ] Frozen candidate rules")

    rules = load_rules(
        rules_path
    )

    add_check(
        checks,
        "frozen_rules_schema",
        "PASS",
        f"rules={len(rules)}",
    )

    add_check(
        checks,
        "frozen_rules_provenance",
        "NOT_VERIFIED",
        (
            "The JSON declaration alone does not "
            "prove that thresholds were frozen "
            "before the holdout period."
        ),
    )

    log("[READ] Candidate feature signals")

    signals, signal_stats = load_signals(
        signals_path,
        holdout_start,
    )

    if not signals:
        raise ValueError(
            "No candidate feature rows "
            "in the requested holdout period."
        )

    add_check(
        checks,
        "holdout_signal_rows",
        "PASS",
        json.dumps(
            signal_stats,
            sort_keys=True,
        ),
    )

    selected = select_candidate_signals(
        signals,
        rules,
    )

    if not selected:
        raise ValueError(
            "No holdout signals satisfy "
            "the frozen candidate rules."
        )

    add_check(
        checks,
        "selected_candidate_signals",
        "PASS",
        f"selected={len(selected)}",
    )

    log("[RUN] Historical OHLCV backtest")

    trades, ohlcv_hashes, backtest_stats = (
        run_backtest(
            selected,
            ohlcv_root,
        )
    )

    if not trades:
        raise ValueError(
            "No backtest events were generated."
        )

    summary = build_summary(
        trades
    )

    complete_count = sum(
        1
        for row in trades
        if row["status"] in (
            "HIT",
            "MISS",
        )
    )

    incomplete_count = sum(
        1
        for row in trades
        if row["status"] == "INCOMPLETE"
    )

    add_check(
        checks,
        "backtest_events",
        "PASS",
        (
            f"events={len(trades)}, "
            f"complete={complete_count}, "
            f"incomplete={incomplete_count}"
        ),
    )

    add_check(
        checks,
        "overlap_handling",
        "PASS",
        json.dumps(
            backtest_stats,
            sort_keys=True,
        ),
    )

    add_check(
        checks,
        "future_feature_leakage",
        "NOT_VERIFIED",
        (
            "Feature generation source code and "
            "as-of availability have not been "
            "independently audited."
        ),
    )

    add_check(
        checks,
        "statistical_independence",
        "NOT_VERIFIED",
        (
            "Overlapping signals within each "
            "candidate/market/timeframe are "
            "suppressed, but cross-market and "
            "cross-timeframe dependence remains."
        ),
    )

    add_check(
        checks,
        "execution_realism",
        "NOT_VERIFIED",
        (
            "Next-candle open entry is hypothetical. "
            "Fees, slippage, liquidity, order execution "
            "and intrabar ordering are not modeled."
        ),
    )

    snapshots = {
        str(signals_path): sha256_file(
            signals_path
        ),
        str(rules_path): sha256_file(
            rules_path
        ),
    }

    snapshots.update(
        ohlcv_hashes
    )

    log("[CHECK] Input SHA256")

    verify_hashes(
        snapshots
    )

    add_check(
        checks,
        "input_sha256",
        "PASS",
        (
            f"{len(snapshots)} input files "
            "verified unchanged."
        ),
    )

    # This is a historical descriptive backtest.
    # Genuine independent OOS status requires
    # provenance evidence outside these files.

    result = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "execution_status": "PASS",
        "backtest_status": (
            "DESCRIPTIVE_HOLDOUT_BACKTEST"
        ),
        "independent_oos_proven": False,
        "approved_for_live_trading": False,
        "research_end": (
            research_end.isoformat()
        ),
        "holdout_start": (
            holdout_start.isoformat()
        ),
        "target_return": TARGET_RETURN,
        "horizons": list(HORIZONS),
        "candidate_rules": len(rules),
        "selected_signal_rows": len(selected),
        "backtest_event_rows": len(trades),
        "complete_event_rows": complete_count,
        "incomplete_event_rows": incomplete_count,
        "signal_statistics": signal_stats,
        "backtest_statistics": backtest_stats,
        "input_sha256": snapshots,
        "limitations": [
            "Frozen rule provenance not independently verified.",
            "Research/holdout boundary provenance not independently verified.",
            "Candidate feature leakage not independently audited.",
            "No execution costs or slippage modeled.",
            "No statistical confidence intervals calculated.",
            "Signal observations are not necessarily independent.",
            "No automatic trading approval.",
        ],
    }

    log("[CHECK] Output destination")

    prepare_output(
        output_dir,
        [
            signals_path,
            rules_path,
            ohlcv_root,
        ],
    )

    log("[WRITE] Backtest reports")

    write_csv(
        output_dir
        / "independent_backtest_trades.csv",
        TRADE_FIELDS,
        trades,
    )

    write_csv(
        output_dir
        / "independent_backtest_summary.csv",
        SUMMARY_FIELDS,
        summary,
    )

    write_csv(
        output_dir
        / "independent_backtest_checks.csv",
        CHECK_FIELDS,
        checks,
    )

    write_json(
        output_dir
        / "independent_backtest_result.json",
        result,
    )

    manifest_hashes = {}

    for name in OUTPUT_NAMES:
        if name == (
            "independent_backtest_manifest.json"
        ):
            continue

        manifest_hashes[name] = sha256_file(
            output_dir / name
        )

    manifest = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "input_sha256": snapshots,
        "output_sha256": manifest_hashes,
        "manifest_self_hash": None,
    }

    write_json(
        output_dir
        / "independent_backtest_manifest.json",
        manifest,
    )

    log("[CHECK] Final SHA256")

    verify_hashes(
        snapshots
    )

    for name, expected in manifest_hashes.items():
        actual = sha256_file(
            output_dir / name
        )

        if actual != expected:
            raise RuntimeError(
                f"Output SHA256 mismatch: {name}"
            )

    log("[PASS] Backtest program completed.")
    log("[PASS] Five output files generated.")
    log("[PASS] Input SHA256 unchanged.")
    log("[PASS] Output SHA256 verified.")

    log(
        "[INFO] Independent OOS proven: FALSE"
    )

    log(
        "[INFO] Approved for live trading: FALSE"
    )

    log("=" * 70)
    log("INDEPENDENT CANDIDATE BACKTEST COMPLETE")
    log("=" * 70)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(
            main()
        )

    except Exception as exc:
        log(
            "[FAIL] "
            f"{type(exc).__name__}: {exc}"
        )
        sys.exit(1)
