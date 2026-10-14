
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UPBIT SURGE MONITOR
Pattern 256 + Accumulation
Candidate Independent Backtest
Clean V002

PURPOSE
-------
Recalculate historical candidate outcomes using original OHLCV.

This version is a REPRODUCIBILITY AUDIT, not proof of
a new independent out-of-sample validation.

INPUTS
------
data/reports/pattern_256_accumulation_30pct/
    validation_detail.csv

data/reports/pattern_256_accumulation_candidate_oos/
    candidate_oos_result.json

data/ohlcv/
    h1/
    h4/
    d1/

OUTPUTS
-------
data/reports/
    pattern_256_accumulation_candidate_independent_backtest/
        independent_backtest_trades.csv
        independent_backtest_summary.csv
        independent_backtest_checks.csv
        independent_backtest_result.json
        independent_backtest_manifest.json

RULES
-----
S00_ALL_SIGNALS
S01_PATTERN_60
S02_PATTERN_70
S03_BOTH_PATTERN_60
S04_BOTH_STRICT

SAFETY
------
- Source files are read-only.
- Existing reports are never overwritten.
- Future outcome fields are never used for signal selection.
- Original OOS boundaries are retained by timeframe.
- A PASS does not mean predictive effectiveness.
- Automatic trading is never enabled.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
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

VERSION = "Clean V002"

DETAIL_RELATIVE = Path(
    "data/reports/"
    "pattern_256_accumulation_30pct/"
    "validation_detail.csv"
)

OOS_RELATIVE = Path(
    "data/reports/"
    "pattern_256_accumulation_candidate_oos/"
    "candidate_oos_result.json"
)

OUTPUT_RELATIVE = Path(
    "data/reports/"
    "pattern_256_accumulation_"
    "candidate_independent_backtest"
)

TIMEFRAME_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

HORIZONS = (24, 72, 168)

TARGET_PCT = 30.0

TRADE_FIELDS = [
    "rule_id",
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "max_return_pct",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
    "source_status",
    "source_hit_30pct",
    "status_match",
    "entry_price_match",
]

SUMMARY_FIELDS = [
    "rule_id",
    "timeframe",
    "horizon",
    "events",
    "complete_events",
    "hits",
    "misses",
    "incomplete",
    "hit_rate",
    "status_matches",
    "status_mismatches",
    "entry_price_mismatches",
    "average_max_return_pct",
    "average_max_drawdown_pct",
    "assessment",
]

CHECK_FIELDS = [
    "check",
    "status",
    "detail",
]


def log(message: str) -> None:
    stamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"{stamp} | {message}",
        flush=True,
    )


def parse_time(value: Any) -> datetime:
    text = str(value).strip()

    if not text:
        raise ValueError(
            "Empty timestamp"
        )

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    result = datetime.fromisoformat(
        text
    )

    if result.tzinfo is None:
        raise ValueError(
            f"Timezone missing: {value}"
        )

    return result.astimezone(
        timezone.utc
    )


def number(value: Any) -> float:
    result = float(value)

    if not math.isfinite(result):
        raise ValueError(
            f"Non-finite number: {value}"
        )

    return result


def sha256(path: Path) -> str:
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


def read_json(path: Path) -> dict:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        result = json.load(stream)

    if not isinstance(result, dict):
        raise ValueError(
            f"JSON object expected: {path}"
        )

    return result


def atomic_text(
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
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
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


def atomic_json(
    path: Path,
    payload: dict,
) -> None:
    atomic_text(
        path,
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def check_columns(
    columns: list[str],
    required: list[str],
    source: str,
) -> None:
    missing = [
        name
        for name in required
        if name not in columns
    ]

    if missing:
        raise ValueError(
            f"{source}: missing columns {missing}"
        )


def load_oos_config(
    path: Path,
) -> tuple[
    dict[str, datetime],
    list[dict],
]:
    payload = read_json(
        path
    )

    if payload.get("status") != "PASS":
        raise ValueError(
            "Source OOS execution is not PASS"
        )

    splits = payload.get(
        "splits"
    )

    if not isinstance(splits, dict):
        raise ValueError(
            "OOS splits missing"
        )

    boundaries = {}

    for timeframe in TIMEFRAME_SECONDS:
        item = splits.get(
            timeframe
        )

        if not isinstance(item, dict):
            raise ValueError(
                f"OOS split missing: {timeframe}"
            )

        boundaries[timeframe] = parse_time(
            item["oos_boundary"]
        )

    rules = payload.get(
        "signal_rules"
    )

    if not isinstance(rules, list):
        raise ValueError(
            "OOS signal rules missing"
        )

    expected_ids = {
        "S00_ALL_SIGNALS",
        "S01_PATTERN_60",
        "S02_PATTERN_70",
        "S03_BOTH_PATTERN_60",
        "S04_BOTH_STRICT",
    }

    found_ids = {
        item.get("rule_id")
        for item in rules
    }

    if found_ids != expected_ids:
        raise ValueError(
            "Unexpected candidate rule set"
        )

    for rule in rules:
        for field in (
            "rule_id",
            "min_pattern_256_score",
            "min_accumulation_score",
            "require_both",
        ):
            if field not in rule:
                raise ValueError(
                    f"Rule field missing: {field}"
                )

        if not isinstance(
            rule["require_both"],
            bool,
        ):
            raise ValueError(
                "require_both must be boolean"
            )

    return boundaries, rules


def rule_matches(
    row: dict,
    rule: dict,
) -> bool:
    pattern_score = number(
        row["pattern_256_score"]
    )

    accumulation_score = number(
        row["accumulation_score"]
    )

    if pattern_score < number(
        rule["min_pattern_256_score"]
    ):
        return False

    if accumulation_score < number(
        rule["min_accumulation_score"]
    ):
        return False

    if rule["require_both"]:
        if row["classification"] != "BOTH":
            return False

    return True


def find_ohlcv_file(
    root: Path,
    timeframe: str,
    market: str,
) -> Path:
    directory = (
        root / timeframe
    )

    candidates = [
        directory / f"{market}.csv",
        directory / (
            market.replace("-", "_") + ".csv"
        ),
    ]

    found = [
        path
        for path in candidates
        if path.is_file()
    ]

    if len(found) != 1:
        raise FileNotFoundError(
            "Expected exactly one OHLCV file for "
            f"{timeframe}/{market}: {candidates}"
        )

    return found[0]


def find_column(
    columns: list[str],
    names: tuple[str, ...],
) -> str:
    for name in names:
        if name in columns:
            return name

    raise ValueError(
        f"Missing OHLCV column: {names}"
    )


def load_candles(
    path: Path,
) -> tuple[
    list[dict],
    dict[datetime, int],
]:
    candles = []
    positions = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(
            stream
        )

        columns = (
            reader.fieldnames or []
        )

        time_key = find_column(
            columns,
            (
                "candle_date_time_utc",
                "timestamp",
            ),
        )

        open_key = find_column(
            columns,
            (
                "opening_price",
                "open",
            ),
        )

        high_key = find_column(
            columns,
            (
                "high_price",
                "high",
            ),
        )

        low_key = find_column(
            columns,
            (
                "low_price",
                "low",
            ),
        )

        close_key = find_column(
            columns,
            (
                "trade_price",
                "close",
            ),
        )

        previous = None

        for row in reader:
            timestamp = parse_time(
                row[time_key]
            )

            opening = number(
                row[open_key]
            )

            high = number(
                row[high_key]
            )

            low = number(
                row[low_key]
            )

            close = number(
                row[close_key]
            )

            if min(
                opening,
                high,
                low,
                close,
            ) <= 0:
                raise ValueError(
                    f"Invalid OHLCV price: {path}"
                )

            if (
                high < max(
                    opening,
                    low,
                    close,
                )
                or low > min(
                    opening,
                    high,
                    close,
                )
            ):
                raise ValueError(
                    f"Invalid OHLCV candle: {path}"
                )

            if (
                previous is not None
                and timestamp <= previous
            ):
                raise ValueError(
                    "OHLCV timestamps are not "
                    f"strictly increasing: {path}"
                )

            positions[timestamp] = len(
                candles
            )

            candles.append({
                "timestamp": timestamp,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
            })

            previous = timestamp

    if not candles:
        raise ValueError(
            f"Empty OHLCV: {path}"
        )

    return candles, positions


def calculate_event(
    row: dict,
    candles: list[dict],
    positions: dict[datetime, int],
) -> dict:
    timeframe = row[
        "timeframe"
    ]

    signal_time = parse_time(
        row["signal_timestamp"]
    )

    entry_time = parse_time(
        row["entry_timestamp"]
    )

    horizon = int(
        row["horizon"]
    )

    if horizon not in HORIZONS:
        raise ValueError(
            f"Unexpected horizon: {horizon}"
        )

    seconds = TIMEFRAME_SECONDS[
        timeframe
    ]

    expected_entry = (
        signal_time.timestamp()
        + seconds
    )

    if entry_time.timestamp() != expected_entry:
        raise ValueError(
            "Source entry is not the next "
            f"expected candle: {timeframe}, "
            f"{row['market']}, {signal_time}"
        )

    if entry_time not in positions:
        raise ValueError(
            "Entry timestamp missing in OHLCV: "
            f"{timeframe}/{row['market']}/{entry_time}"
        )

    entry_index = positions[
        entry_time
    ]

    entry_price = candles[
        entry_index
    ]["open"]

    source_entry = number(
        row["entry_price"]
    )

    entry_match = math.isclose(
        entry_price,
        source_entry,
        rel_tol=1e-9,
        abs_tol=1e-9,
    )

    target_price = (
        entry_price
        * (1.0 + TARGET_PCT / 100.0)
    )

    highest = entry_price
    lowest = entry_price

    first_hit = None
    observed = 0

    for offset in range(
        horizon
    ):
        index = (
            entry_index + offset
        )

        if index >= len(candles):
            break

        candle = candles[index]

        expected = (
            entry_time.timestamp()
            + offset * seconds
        )

        if (
            candle["timestamp"].timestamp()
            != expected
        ):
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
            first_hit is None
            and candle["high"] >= target_price
        ):
            first_hit = offset + 1

    if observed < horizon:
        status = "INCOMPLETE"
        hit_value = ""
    elif first_hit is not None:
        status = "HIT"
        hit_value = 1
    else:
        status = "MISS"
        hit_value = 0

    max_return = (
        (highest / entry_price - 1.0)
        * 100.0
    )

    max_drawdown = (
        (lowest / entry_price - 1.0)
        * 100.0
    )

    source_status = str(
        row["status"]
    ).strip().upper()

    status_match = (
        status == source_status
    )

    if status != "INCOMPLETE":
        source_hit = str(
            row["hit_30pct"]
        ).strip()

        if source_hit not in ("0", "1"):
            raise ValueError(
                "Invalid source HIT/MISS value"
            )

        if int(source_hit) != hit_value:
            status_match = False

    return {
        "timeframe": timeframe,
        "market": row["market"],
        "signal_timestamp": (
            signal_time.isoformat()
        ),
        "entry_timestamp": (
            entry_time.isoformat()
        ),
        "entry_price": entry_price,
        "horizon": horizon,
        "status": status,
        "hit_30pct": hit_value,
        "first_hit_bars": (
            first_hit
            if first_hit is not None
            else ""
        ),
        "max_return_pct": max_return,
        "max_drawdown_pct": max_drawdown,
        "observed_bars": observed,
        "required_bars": horizon,
        "source_status": source_status,
        "source_hit_30pct": (
            row["hit_30pct"]
        ),
        "status_match": status_match,
        "entry_price_match": entry_match,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 candidate OHLCV "
            "reproducibility backtest"
        )
    )

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--detail",
        default=None,
    )

    parser.add_argument(
        "--oos-result",
        default=None,
    )

    parser.add_argument(
        "--ohlcv-root",
        default=None,
    )

    parser.add_argument(
        "--output-dir",
        default=None,
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help=(
            "Smoke test only. "
            "Zero means no limit."
        ),
    )

    return parser.parse_args()


def safe_output_directory(
    output_dir: Path,
    protected: list[Path],
) -> None:
    output_dir = output_dir.resolve()

    for path in protected:
        path = path.resolve()

        if (
            output_dir == path
            or output_dir in path.parents
            or path in output_dir.parents
        ):
            raise ValueError(
                "Output overlaps protected input: "
                f"{path}"
            )

    if output_dir.exists():
        if not output_dir.is_dir():
            raise ValueError(
                "Output path is not a directory"
            )

        if any(output_dir.iterdir()):
            raise FileExistsError(
                "Output already contains files. "
                f"Refusing overwrite: {output_dir}"
            )

    else:
        output_dir.mkdir(
            parents=True,
            exist_ok=False,
        )


def main() -> int:
    args = parse_args()

    root = Path(
        args.root
    ).expanduser().resolve()

    if not root.is_dir():
        raise FileNotFoundError(
            f"Root missing: {root}"
        )

    detail_path = (
        Path(args.detail).expanduser().resolve()
        if args.detail
        else root / DETAIL_RELATIVE
    )

    oos_path = (
        Path(args.oos_result).expanduser().resolve()
        if args.oos_result
        else root / OOS_RELATIVE
    )

    ohlcv_root = (
        Path(args.ohlcv_root).expanduser().resolve()
        if args.ohlcv_root
        else root / "data" / "ohlcv"
    )

    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else root / OUTPUT_RELATIVE
    )

    for path in (
        detail_path,
        oos_path,
    ):
        if not path.is_file():
            raise FileNotFoundError(
                f"Input missing: {path}"
            )

    if not ohlcv_root.is_dir():
        raise FileNotFoundError(
            f"OHLCV root missing: {ohlcv_root}"
        )

    if args.max_events < 0:
        raise ValueError(
            "max-events cannot be negative"
        )

    log("=" * 70)
    log(f"{PROGRAM} | {VERSION}")
    log("=" * 70)

    log("[READ] OOS configuration")

    boundaries, rules = load_oos_config(
        oos_path
    )

    for timeframe, boundary in boundaries.items():
        log(
            f"[OOS] {timeframe}: "
            f"{boundary.isoformat()}"
        )

    log("[READ] Validation detail header")

    with detail_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(
            stream
        )

        columns = (
            reader.fieldnames or []
        )

    check_columns(
        columns,
        [
            "timeframe",
            "market",
            "signal_timestamp",
            "classification",
            "pattern_256_score",
            "accumulation_score",
            "entry_timestamp",
            "entry_price",
            "horizon",
            "status",
            "hit_30pct",
        ],
        str(detail_path),
    )

    safe_output_directory(
        output_dir,
        [
            detail_path,
            oos_path,
            ohlcv_root,
        ],
    )

    trades_path = (
        output_dir
        / "independent_backtest_trades.csv"
    )

    summary_path = (
        output_dir
        / "independent_backtest_summary.csv"
    )

    checks_path = (
        output_dir
        / "independent_backtest_checks.csv"
    )

    result_path = (
        output_dir
        / "independent_backtest_result.json"
    )

    manifest_path = (
        output_dir
        / "independent_backtest_manifest.json"
    )

    # SHA256 is captured before processing.
    # All files actually read are checked again
    # after processing.

    input_hashes = {
        str(detail_path.resolve()): sha256(
            detail_path
        ),
        str(oos_path.resolve()): sha256(
            oos_path
        ),
    }

    summary_groups = defaultdict(
        lambda: {
            "events": 0,
            "complete": 0,
            "hits": 0,
            "misses": 0,
            "incomplete": 0,
            "status_matches": 0,
            "status_mismatches": 0,
            "entry_mismatches": 0,
            "sum_return": 0.0,
            "sum_drawdown": 0.0,
        }
    )

    # Cache only the currently accessed
    # market/timeframe pair. This avoids
    # retaining the entire OHLCV market
    # in memory.

    cached_key = None
    cached_candles = None
    cached_positions = None

    source_rows = 0
    oos_rows = 0
    matched_rows = 0
    written_rows = 0
    missing_entry_rows = 0

    # Every horizon row is treated as a
    # separate evaluation record, not as
    # a separate independent signal.

    with (
        detail_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as source,
        trades_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as destination,
    ):
        reader = csv.DictReader(
            source
        )

        writer = csv.DictWriter(
            destination,
            fieldnames=TRADE_FIELDS,
            lineterminator="\n",
        )

        writer.writeheader()

        for row in reader:
            source_rows += 1

            timeframe = str(
                row["timeframe"]
            ).strip().lower()

            if timeframe not in boundaries:
                raise ValueError(
                    "Unexpected timeframe: "
                    f"{timeframe}"
                )

            row["timeframe"] = timeframe

            signal_time = parse_time(
                row["signal_timestamp"]
            )

            if signal_time < boundaries[timeframe]:
                continue

            oos_rows += 1

            matched_rules = [
                rule
                for rule in rules
                if rule_matches(
                    row,
                    rule,
                )
            ]

            if not matched_rules:
                continue

            matched_rows += 1

            if (
                not str(
                    row["entry_timestamp"]
                ).strip()
            ):
                missing_entry_rows += 1
                continue

            market = str(
                row["market"]
            ).strip().upper()

            if not market.startswith("KRW-"):
                raise ValueError(
                    f"Invalid market: {market}"
                )

            row["market"] = market

            cache_key = (
                timeframe,
                market,
            )

            if cache_key != cached_key:
                path = find_ohlcv_file(
                    ohlcv_root,
                    timeframe,
                    market,
                )

                resolved = str(
                    path.resolve()
                )

                current_hash = sha256(
                    path
                )

                if resolved in input_hashes:
                    if input_hashes[resolved] != current_hash:
                        raise RuntimeError(
                            "OHLCV changed during execution: "
                            f"{path}"
                        )
                else:
                    input_hashes[resolved] = current_hash

                cached_candles, cached_positions = (
                    load_candles(
                        path
                    )
                )

                cached_key = cache_key

                log(
                    "[OHLCV] "
                    f"{timeframe}/{market} "
                    f"candles={len(cached_candles)}"
                )

            event = calculate_event(
                row,
                cached_candles,
                cached_positions,
            )

            for rule in matched_rules:
                trade = {
                    "rule_id": rule["rule_id"],
                    **event,
                }

                writer.writerow(
                    trade
                )

                written_rows += 1

                key = (
                    rule["rule_id"],
                    timeframe,
                    int(
                        row["horizon"]
                    ),
                )

                group = summary_groups[
                    key
                ]

                group["events"] += 1

                if event["status"] == "HIT":
                    group["hits"] += 1
                    group["complete"] += 1

                elif event["status"] == "MISS":
                    group["misses"] += 1
                    group["complete"] += 1

                else:
                    group["incomplete"] += 1

                if event["status_match"]:
                    group["status_matches"] += 1
                else:
                    group["status_mismatches"] += 1

                if not event["entry_price_match"]:
                    group["entry_mismatches"] += 1

                if event["status"] != "INCOMPLETE":
                    group["sum_return"] += event[
                        "max_return_pct"
                    ]

                    group["sum_drawdown"] += event[
                        "max_drawdown_pct"
                    ]

            if (
                source_rows % 100000 == 0
            ):
                log(
                    "[PROGRESS] "
                    f"source={source_rows:,} "
                    f"oos={oos_rows:,} "
                    f"output={written_rows:,}"
                )

            if (
                args.max_events > 0
                and matched_rows >= args.max_events
            ):
                log(
                    "[SMOKE TEST] Event limit reached"
                )
                break

    log("[WRITE] Summary")

    summary_rows = []

    total_status_mismatches = 0
    total_entry_mismatches = 0

    for key, group in sorted(
        summary_groups.items()
    ):
        rule_id, timeframe, horizon = key

        complete = group[
            "complete"
        ]

        total_status_mismatches += group[
            "status_mismatches"
        ]

        total_entry_mismatches += group[
            "entry_mismatches"
        ]

        if complete:
            hit_rate = (
                group["hits"] / complete
            )

            avg_return = (
                group["sum_return"] / complete
            )

            avg_drawdown = (
                group["sum_drawdown"] / complete
            )

        else:
            hit_rate = ""
            avg_return = ""
            avg_drawdown = ""

        if (
            group["status_mismatches"] > 0
            or group["entry_mismatches"] > 0
        ):
            assessment = "MISMATCH"

        elif complete == 0:
            assessment = "INSUFFICIENT_DATA"

        else:
            assessment = "REPRODUCED_DESCRIPTIVE_ONLY"

        summary_rows.append({
            "rule_id": rule_id,
            "timeframe": timeframe,
            "horizon": horizon,
            "events": group["events"],
            "complete_events": complete,
            "hits": group["hits"],
            "misses": group["misses"],
            "incomplete": group["incomplete"],
            "hit_rate": hit_rate,
            "status_matches": group[
                "status_matches"
            ],
            "status_mismatches": group[
                "status_mismatches"
            ],
            "entry_price_mismatches": group[
                "entry_mismatches"
            ],
            "average_max_return_pct": avg_return,
            "average_max_drawdown_pct": avg_drawdown,
            "assessment": assessment,
        })

    with summary_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=SUMMARY_FIELDS,
            lineterminator="\n",
        )

        writer.writeheader()
        writer.writerows(
            summary_rows
        )

    # This is an audit of existing outcomes.
    # Reusing previously evaluated periods
    # cannot prove a pristine holdout.

    is_smoke = (
        args.max_events > 0
    )

    if (
        total_status_mismatches > 0
        or total_entry_mismatches > 0
    ):
        execution_status = "FAIL"

    elif written_rows == 0:
        execution_status = "FAIL"

    elif is_smoke:
        execution_status = "SMOKE_TEST_PASS"

    else:
        execution_status = "PASS"

    checks = [
        {
            "check": "source_schema",
            "status": "PASS",
            "detail": (
                "Required validation_detail columns found"
            ),
        },
        {
            "check": "oos_boundaries",
            "status": "PASS",
            "detail": (
                "Original timeframe-specific "
                "OOS boundaries used"
            ),
        },
        {
            "check": "candidate_rules",
            "status": "PASS",
            "detail": (
                "Original S00-S04 rules used"
            ),
        },
        {
            "check": "future_outcomes_as_features",
            "status": "PASS",
            "detail": (
                "Outcome fields excluded from "
                "signal selection"
            ),
        },
        {
            "check": "status_reproduction",
            "status": (
                "PASS"
                if total_status_mismatches == 0
                else "FAIL"
            ),
            "detail": (
                f"mismatches={total_status_mismatches}"
            ),
        },
        {
            "check": "entry_price_reproduction",
            "status": (
                "PASS"
                if total_entry_mismatches == 0
                else "FAIL"
            ),
            "detail": (
                f"mismatches={total_entry_mismatches}"
            ),
        },
        {
            "check": "independent_oos_proven",
            "status": "NOT_VERIFIED",
            "detail": (
                "Existing OOS data are reused. "
                "New independent holdout not proven."
            ),
        },
        {
            "check": "feature_causality",
            "status": "NOT_VERIFIED",
            "detail": (
                "Signal score generation and "
                "as-of availability not audited"
            ),
        },
        {
            "check": "statistical_independence",
            "status": "NOT_VERIFIED",
            "detail": (
                "Market and timeframe events "
                "may be dependent"
            ),
        },
        {
            "check": "execution_realism",
            "status": "NOT_VERIFIED",
            "detail": (
                "Fees, slippage, liquidity and "
                "intrabar ordering not modeled"
            ),
        },
    ]

    with checks_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=CHECK_FIELDS,
            lineterminator="\n",
        )

        writer.writeheader()
        writer.writerows(
            checks
        )

    log("[VERIFY] Input SHA256")

    for path_text, expected in input_hashes.items():
        path = Path(
            path_text
        )

        if not path.is_file():
            raise RuntimeError(
                f"Input disappeared: {path}"
            )

        actual = sha256(
            path
        )

        if actual != expected:
            raise RuntimeError(
                f"Input SHA256 changed: {path}"
            )

    result = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution_status": execution_status,
        "assessment": (
            "EXISTING_OOS_REPRODUCIBILITY_ONLY"
        ),
        "independent_oos_proven": False,
        "approved_for_live_trading": False,
        "automatic_trading_enabled": False,
        "smoke_test": is_smoke,
        "max_events": args.max_events,
        "target_return_pct": TARGET_PCT,
        "horizons": list(HORIZONS),
        "source_rows_read": source_rows,
        "oos_rows_read": oos_rows,
        "matched_source_rows": matched_rows,
        "missing_entry_rows_excluded": (
            missing_entry_rows
        ),
        "output_rows": written_rows,
        "summary_rows": len(summary_rows),
        "status_mismatches": (
            total_status_mismatches
        ),
        "entry_price_mismatches": (
            total_entry_mismatches
        ),
        "oos_boundaries": {
            key: value.isoformat()
            for key, value in boundaries.items()
        },
        "rule_ids": [
            rule["rule_id"]
            for rule in rules
        ],
        "input_sha256": input_hashes,
        "limitations": [
            "This reuses previously evaluated OOS data.",
            "No new independent holdout is proven.",
            "Feature causality is not independently audited.",
            "Signals are not necessarily independent.",
            "Intrabar ordering is not modeled.",
            "Fees and slippage are not modeled.",
            "No live trading approval.",
        ],
    }

    atomic_json(
        result_path,
        result,
    )

    output_hashes = {}

    for path in (
        trades_path,
        summary_path,
        checks_path,
        result_path,
    ):
        output_hashes[
            path.name
        ] = sha256(
            path
        )

    manifest = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    log("=" * 70)
    log(
        f"EXECUTION STATUS: {execution_status}"
    )
    log(
        f"SOURCE ROWS: {source_rows:,}"
    )
    log(
        f"OOS ROWS: {oos_rows:,}"
    )
    log(
        f"MATCHED SOURCE ROWS: {matched_rows:,}"
    )
    log(
        f"OUTPUT ROWS: {written_rows:,}"
    )
    log(
        "STATUS MISMATCHES: "
        f"{total_status_mismatches:,}"
    )
    log(
        "ENTRY PRICE MISMATCHES: "
        f"{total_entry_mismatches:,}"
    )
    log(
        "INDEPENDENT OOS PROVEN: FALSE"
    )
    log(
        "LIVE TRADING APPROVED: FALSE"
    )
    log("=" * 70)

    if execution_status == "FAIL":
        return 1

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
