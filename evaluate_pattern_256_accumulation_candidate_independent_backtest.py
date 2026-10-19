
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_accumulation_candidate_independent_backtest.py

Clean V001

Purpose
-------
Evaluate the existing independent candidate backtest without modifying
production inputs or historical OHLCV data.

Evaluation:
    - 30% target hit rate
    - Performance by timeframe
    - Performance by classification
    - Performance by horizon
    - Maximum return
    - Maximum drawdown
    - Sample size and Wilson confidence intervals
    - OOS evidence and data-quality checks
    - Candidate decision: REVIEW / HOLD / REJECT

Safety:
    - Read-only input handling
    - No automatic trading
    - No live trading approval
    - No future information used to generate signals
    - No automatic deletion of existing output
    - SHA256 input/output audit
    - Fail-closed on missing or ambiguous required fields
    - Atomic output writing
    - Resume-safe completed-output verification

Python:
    Standard library only.

Usage:
    py evaluate_pattern_256_accumulation_candidate_independent_backtest.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor"

    py evaluate_pattern_256_accumulation_candidate_independent_backtest.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor" --smoke-test

    py evaluate_pattern_256_accumulation_candidate_independent_backtest.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor" --output "C:\\temp\\candidate_evaluation"
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import traceback

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VERSION = "Clean V002"

PROGRAM_NAME = (
    "evaluate_pattern_256_accumulation_"
    "candidate_independent_backtest.py"
)

DEFAULT_INPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest"
)

DEFAULT_OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest_evaluation"
)

OUTPUT_FILES = (
    "independent_evaluation_summary.csv",
    "independent_evaluation_comparison.csv",
    "independent_evaluation_decision.csv",
    "independent_evaluation_result.json",
    "independent_evaluation_manifest.json",
)

TRADE_FILENAME_CANDIDATES = (
    "backtest_trades.csv",
    "independent_backtest_trades.csv",
    "independent_backtest_detail.csv",
    "backtest_detail.csv",
    "trade_detail.csv",
    "trades.csv",
)

RESULT_FILENAME_CANDIDATES = (
    "backtest_result.json",
    "independent_backtest_result.json",
    "independent_result.json",
)

COLUMN_ALIASES = {
    "timeframe": (
        "timeframe",
    ),
    "market": (
        "market",
    ),
    "signal_timestamp": (
        "signal_timestamp",
    ),
    "entry_timestamp": (
        "entry_timestamp",
    ),
    "classification": (
        "classification",
    ),
    "horizon": (
        "horizon",
        "required_bars",
    ),
    "status": (
        "status",
    ),
    "hit": (
        "hit_30pct",
    ),
    "max_return": (
        "max_return_pct",
    ),
    "max_drawdown": (
        "max_drawdown_pct",
    ),
    "observed_bars": (
        "observed_bars",
    ),
    "target_return": (
        "target_return_pct",
    ),
    "entry_price": (
        "entry_price",
    ),
    "first_hit_bars": (
        "first_hit_bars",
    ),
}

SUPPORTED_CLASSIFICATIONS = {
    "PATTERN_256_ONLY",
    "ACCUMULATION_ONLY",
    "BOTH",
    "NEITHER",
}

SUPPORTED_TIMEFRAMES = {
    "h1",
    "h4",
    "d1",
}

COMPLETE_STATUSES = {
    "HIT",
    "MISS",
}

INCOMPLETE_STATUSES = {
    "INCOMPLETE",
    "PENDING",
    "INSUFFICIENT_DATA",
    "CENSORED",
    "UNKNOWN",
    "SKIP",
    "SKIPPED",
}

Z_95 = 1.959963984540054

# Research review thresholds, not profitability guarantees.
MIN_REVIEW_SAMPLE = 100
MIN_COMPARISON_SAMPLE = 30


def log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(f"{timestamp} | {message}", flush=True)


def fail(message: str) -> None:
    raise RuntimeError(message)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def atomic_write_json(path: Path, data: dict) -> None:
    atomic_write_text(
        path,
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def atomic_write_csv(
    path: Path,
    fieldnames: list[str],
    rows: list[dict],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def read_json(path: Path) -> dict:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        value = json.load(handle)

    if not isinstance(value, dict):
        fail(f"JSON root must be an object: {path}")

    return value


def normalize_column(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def build_column_map(fieldnames: list[str]) -> dict:
    normalized = {}

    for original in fieldnames:
        key = normalize_column(original)

        if key in normalized:
            fail(f"Duplicate normalized CSV column: {key}")

        normalized[key] = original

    result = {}

    for canonical, aliases in COLUMN_ALIASES.items():

        # horizon과 required_bars는 우선순위로 처리한다.
        # 두 컬럼이 동시에 존재해도 중복 오류로 판단하지 않는다.
        if canonical == "horizon":
            if "horizon" in normalized:
                result["horizon"] = normalized["horizon"]
            elif "required_bars" in normalized:
                result["horizon"] = normalized["required_bars"]
            continue

        matches = [
            normalized[alias]
            for alias in aliases
            if alias in normalized
        ]

        if len(matches) > 1:
            fail(
                f"Ambiguous columns for {canonical}: {matches}"
            )

        if matches:
            result[canonical] = matches[0]

    return result


def required_columns() -> tuple[str, ...]:
    return (
        "timeframe",
        "market",
        "signal_timestamp",
        "entry_timestamp",
        "classification",
        "horizon",
        "status",
        "hit",
        "max_return",
        "max_drawdown",
        "observed_bars",
        "target_return",
        "entry_price",
    )


def get_value(
    row: dict,
    columns: dict,
    canonical: str,
) -> str:
    original = columns.get(canonical)

    if original is None:
        return ""

    value = row.get(original)

    if value is None:
        return ""

    return str(value).strip()


def parse_float(
    value: str,
    field: str,
    line_number: int,
    allow_blank: bool = False,
) -> float | None:
    if value == "":
        if allow_blank:
            return None

        fail(
            f"Missing {field} at CSV line {line_number}"
        )

    try:
        number = float(value)

    except ValueError:
        fail(
            f"Invalid {field} at CSV line "
            f"{line_number}: {value}"
        )

    if not math.isfinite(number):
        fail(
            f"Non-finite {field} at CSV line {line_number}"
        )

    return number


def parse_int(
    value: str,
    field: str,
    line_number: int,
    allow_blank: bool = False,
) -> int | None:
    if value == "":
        if allow_blank:
            return None

        fail(
            f"Missing {field} at CSV line {line_number}"
        )

    try:
        number = int(value)

    except ValueError:
        fail(
            f"Invalid {field} at CSV line "
            f"{line_number}: {value}"
        )

    return number


def parse_timestamp(
    value: str,
    field: str,
    line_number: int,
) -> datetime:
    if not value:
        fail(
            f"Missing {field} at CSV line {line_number}"
        )

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

    except ValueError:
        fail(
            f"Invalid {field} at CSV line "
            f"{line_number}: {value}"
        )

    if parsed.tzinfo is None:
        fail(
            f"Timezone missing from {field} "
            f"at CSV line {line_number}"
        )

    return parsed.astimezone(timezone.utc)


def parse_hit(
    value: str,
    line_number: int,
) -> int:
    if value in ("1", "1.0", "true", "True", "TRUE"):
        return 1

    if value in ("0", "0.0", "false", "False", "FALSE"):
        return 0

    fail(
        f"Invalid hit_30pct at CSV line "
        f"{line_number}: {value}"
    )


def safe_rate(
    numerator: int,
    denominator: int,
) -> float | None:
    if denominator <= 0:
        return None

    return numerator / denominator * 100.0


def wilson_interval(
    hits: int,
    total: int,
) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None

    p = hits / total
    z2 = Z_95 * Z_95

    denominator = 1.0 + z2 / total

    center = (
        p + z2 / (2.0 * total)
    ) / denominator

    margin = (
        Z_95
        * math.sqrt(
            p * (1.0 - p) / total
            + z2 / (4.0 * total * total)
        )
        / denominator
    )

    return (
        max(0.0, center - margin) * 100.0,
        min(1.0, center + margin) * 100.0,
    )


def round_optional(
    value: float | None,
    digits: int = 6,
) -> float | None:
    if value is None:
        return None

    return round(value, digits)


class Aggregate:
    def __init__(self) -> None:
        self.total_rows = 0
        self.completed = 0
        self.hits = 0
        self.misses = 0
        self.incomplete = 0

        self.return_sum = 0.0
        self.drawdown_sum = 0.0

        self.max_return = None
        self.worst_drawdown = None

        self.unique_markets = set()
        self.unique_events = set()

    def add(self, event: dict) -> None:
        self.total_rows += 1

        self.unique_markets.add(event["market"])
        self.unique_events.add(event["event_key"])

        if not event["completed"]:
            self.incomplete += 1
            return

        self.completed += 1
        self.hits += event["hit"]
        self.misses += 1 - event["hit"]

        maximum = event["max_return"]
        drawdown = event["max_drawdown"]

        self.return_sum += maximum
        self.drawdown_sum += drawdown

        if (
            self.max_return is None
            or maximum > self.max_return
        ):
            self.max_return = maximum

        if (
            self.worst_drawdown is None
            or drawdown < self.worst_drawdown
        ):
            self.worst_drawdown = drawdown

    def summary(
        self,
        timeframe: str,
        classification: str,
        horizon: str,
    ) -> dict:
        lower, upper = wilson_interval(
            self.hits,
            self.completed,
        )

        return {
            "timeframe": timeframe,
            "classification": classification,
            "horizon": horizon,
            "total_rows": self.total_rows,
            "completed": self.completed,
            "hits": self.hits,
            "misses": self.misses,
            "incomplete": self.incomplete,
            "hit_rate_pct": round_optional(
                safe_rate(self.hits, self.completed)
            ),
            "wilson_95_lower_pct": round_optional(lower),
            "wilson_95_upper_pct": round_optional(upper),
            "avg_max_return_pct": round_optional(
                self.return_sum / self.completed
                if self.completed else None
            ),
            "avg_max_drawdown_pct": round_optional(
                self.drawdown_sum / self.completed
                if self.completed else None
            ),
            "best_max_return_pct": round_optional(
                self.max_return
            ),
            "worst_max_drawdown_pct": round_optional(
                self.worst_drawdown
            ),
            "unique_markets": len(self.unique_markets),
            "unique_signal_events": len(self.unique_events),
        }


def select_trade_csv(input_dir: Path) -> Path:
    for filename in TRADE_FILENAME_CANDIDATES:
        candidate = input_dir / filename

        if candidate.is_file():
            log(f"[INPUT] Selected trade CSV: {candidate}")
            return candidate

    candidates = []

    for path in sorted(input_dir.glob("*.csv")):
        if not path.is_file():
            continue

        try:
            with path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as handle:
                reader = csv.reader(handle)
                header = next(reader, [])

            columns = build_column_map(header)

            if all(
                name in columns
                for name in required_columns()
            ):
                candidates.append(path)

        except (OSError, UnicodeError, csv.Error):
            continue

    if len(candidates) == 1:
        log(f"[INPUT] Selected trade CSV: {candidates[0]}")
        return candidates[0]

    if not candidates:
        fail(
            "No compatible trade-level CSV found. "
            "Inspect the backtest output filenames and headers."
        )

    fail(
        "Multiple compatible trade CSV files found. "
        "Specify --trades explicitly: "
        + ", ".join(str(path) for path in candidates)
    )


def select_result_json(input_dir: Path) -> Path | None:
    found = [
        input_dir / filename
        for filename in RESULT_FILENAME_CANDIDATES
        if (input_dir / filename).is_file()
    ]

    if len(found) > 1:
        fail(
            "Multiple candidate result JSON files found. "
            "Specify --result explicitly."
        )

    if found:
        return found[0]

    return None


def check_result_metadata(
    result_path: Path | None,
) -> dict:
    if result_path is None:
        return {
            "result_json_found": False,
            "execution_status": None,
            "independent_oos_proven": False,
            "approved_for_live_trading": False,
            "result_metadata_verified": False,
        }

    result = read_json(result_path)

    status = result.get(
        "execution_status",
        result.get("status"),
    )

    if status is None:
        fail(
            "Backtest result JSON has no execution status. "
            "Cannot independently verify successful execution."
        )

    status = str(status).strip().upper()

    if status not in ("PASS", "SUCCESS", "COMPLETED"):
        fail(
            f"Backtest result is not successful: {status}"
        )

    if result.get("approved_for_live_trading") is True:
        fail(
            "Unexpected live-trading approval in source result."
        )

    # OOS proof is not inferred from a successful run.
    proven = result.get(
        "independent_oos_proven"
    ) is True

    return {
        "result_json_found": True,
        "execution_status": status,
        "independent_oos_proven": proven,
        "approved_for_live_trading": False,
        "result_metadata_verified": True,
    }


def validate_event(
    row: dict,
    columns: dict,
    line_number: int,
) -> dict:
    timeframe = get_value(
        row, columns, "timeframe"
    ).lower()

    market = get_value(
        row, columns, "market"
    ).upper()

    classification = get_value(
        row, columns, "classification"
    ).upper()

    status = get_value(
        row, columns, "status"
    ).upper()

    if timeframe not in SUPPORTED_TIMEFRAMES:
        fail(
            f"Unsupported timeframe at line "
            f"{line_number}: {timeframe}"
        )

    if not re.fullmatch(
        r"KRW-[A-Z0-9]+",
        market,
    ):
        fail(
            f"Invalid market at line "
            f"{line_number}: {market}"
        )

    if classification not in SUPPORTED_CLASSIFICATIONS:
        fail(
            f"Unsupported classification at line "
            f"{line_number}: {classification}"
        )

    if (
        status not in COMPLETE_STATUSES
        and status not in INCOMPLETE_STATUSES
    ):
        fail(
            f"Unknown status at line "
            f"{line_number}: {status}"
        )

    horizon = parse_int(
        get_value(row, columns, "horizon"),
        "horizon",
        line_number,
    )

    if horizon <= 0:
        fail(
            f"Invalid horizon at line {line_number}"
        )

    signal_time = parse_timestamp(
        get_value(row, columns, "signal_timestamp"),
        "signal_timestamp",
        line_number,
    )

    entry_time = parse_timestamp(
        get_value(row, columns, "entry_timestamp"),
        "entry_timestamp",
        line_number,
    )

    if entry_time <= signal_time:
        fail(
            f"Entry timestamp is not after signal "
            f"at line {line_number}"
        )

    entry_price = parse_float(
        get_value(row, columns, "entry_price"),
        "entry_price",
        line_number,
    )

    if entry_price <= 0:
        fail(
            f"Non-positive entry price at line {line_number}"
        )

    target_return = parse_float(
        get_value(row, columns, "target_return"),
        "target_return_pct",
        line_number,
    )

    if not math.isclose(
        target_return,
        30.0,
        abs_tol=1e-8,
    ):
        fail(
            f"Target is not 30% at line {line_number}: "
            f"{target_return}"
        )

    observed_bars = parse_int(
        get_value(row, columns, "observed_bars"),
        "observed_bars",
        line_number,
    )

    if observed_bars < 0:
        fail(
            f"Negative observed_bars at line {line_number}"
        )

    if observed_bars > horizon:
        fail(
            f"Observed bars exceed horizon "
            f"at line {line_number}"
        )

    completed = status in COMPLETE_STATUSES

    if completed and observed_bars != horizon:
        fail(
            f"Completed event has insufficient observed bars "
            f"at line {line_number}"
        )

    hit_value = get_value(
        row, columns, "hit"
    )

    if completed:
        hit = parse_hit(
            hit_value,
            line_number,
        )

        expected_hit = (
            1 if status == "HIT" else 0
        )

        if hit != expected_hit:
            fail(
                f"HIT/status mismatch at line {line_number}"
            )

        max_return = parse_float(
            get_value(row, columns, "max_return"),
            "max_return_pct",
            line_number,
        )

        max_drawdown = parse_float(
            get_value(row, columns, "max_drawdown"),
            "max_drawdown_pct",
            line_number,
        )

        if max_return < -100.0:
            fail(
                f"Impossible max return at line {line_number}"
            )

        if max_drawdown < -100.0:
            fail(
                f"Impossible max drawdown at line {line_number}"
            )

        if max_drawdown > 0.000001:
            fail(
                f"Positive max drawdown at line {line_number}"
            )

        if hit == 1 and max_return < 30.0 - 1e-7:
            fail(
                f"HIT without 30% maximum return "
                f"at line {line_number}"
            )

        if hit == 0 and max_return >= 30.0 - 1e-7:
            fail(
                f"MISS despite 30% maximum return "
                f"at line {line_number}"
            )

    else:
        if hit_value not in (
            "",
            "0",
            "0.0",
            "false",
            "False",
            "FALSE",
        ):
            fail(
                f"Incomplete event has positive hit "
                f"at line {line_number}"
            )

        hit = 0
        max_return = None
        max_drawdown = None

    first_hit_raw = get_value(
        row, columns, "first_hit_bars"
    )

    if "first_hit_bars" in columns:
        first_hit = parse_int(
            first_hit_raw,
            "first_hit_bars",
            line_number,
            allow_blank=True,
        )

        if completed and hit == 1:
            if first_hit is None:
                fail(
                    f"HIT missing first_hit_bars "
                    f"at line {line_number}"
                )

            if first_hit < 1 or first_hit > horizon:
                fail(
                    f"Invalid first_hit_bars "
                    f"at line {line_number}"
                )

        if completed and hit == 0 and first_hit is not None:
            fail(
                f"MISS has first_hit_bars "
                f"at line {line_number}"
            )

    event_key = (
        timeframe,
        market,
        signal_time.isoformat(),
        entry_time.isoformat(),
    )

    return {
        "timeframe": timeframe,
        "market": market,
        "classification": classification,
        "horizon": horizon,
        "status": status,
        "completed": completed,
        "hit": hit,
        "max_return": max_return,
        "max_drawdown": max_drawdown,
        "event_key": event_key,
    }


def analyze_trades(
    trades_path: Path,
    smoke_test: bool,
    limit_rows: int,
) -> tuple[list[dict], dict]:
    aggregates = defaultdict(Aggregate)

    total_rows = 0
    duplicate_keys = set()
    seen_keys = set()

    classification_by_event = {}

    horizons_seen = set()
    timeframes_seen = set()

    with trades_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            fail("Trade CSV has no header.")

        columns = build_column_map(
            reader.fieldnames
        )

        missing = [
            field
            for field in required_columns()
            if field not in columns
        ]

        if missing:
            fail(
                "Missing required trade CSV columns: "
                + ", ".join(missing)
            )

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            if smoke_test and total_rows >= limit_rows:
                break

            if None in row:
                fail(
                    f"Malformed CSV row at line {line_number}"
                )

            event = validate_event(
                row,
                columns,
                line_number,
            )

            total_rows += 1

            timeframe = event["timeframe"]
            classification = event["classification"]
            horizon = event["horizon"]
            event_key = event["event_key"]

            timeframes_seen.add(timeframe)
            horizons_seen.add(horizon)

            previous_classification = (
                classification_by_event.get(event_key)
            )

            if previous_classification is None:
                classification_by_event[event_key] = classification

            elif previous_classification != classification:
                fail(
                    "Same signal event has conflicting "
                    f"classifications at line {line_number}"
                )

            unique_row_key = (
                event_key,
                horizon,
            )

            if unique_row_key in seen_keys:
                duplicate_keys.add(unique_row_key)

            else:
                seen_keys.add(unique_row_key)

            group_keys = (
                (
                    timeframe,
                    classification,
                    str(horizon),
                ),
                (
                    timeframe,
                    classification,
                    "ALL",
                ),
                (
                    timeframe,
                    "ALL",
                    str(horizon),
                ),
                (
                    timeframe,
                    "ALL",
                    "ALL",
                ),
                (
                    "ALL",
                    classification,
                    str(horizon),
                ),
                (
                    "ALL",
                    classification,
                    "ALL",
                ),
                (
                    "ALL",
                    "ALL",
                    str(horizon),
                ),
                (
                    "ALL",
                    "ALL",
                    "ALL",
                ),
            )

            for key in group_keys:
                aggregates[key].add(event)

            if total_rows % 100000 == 0:
                log(
                    "[PROGRESS] "
                    f"rows={total_rows:,} "
                    f"duplicate_keys={len(duplicate_keys):,}"
                )

    if total_rows == 0:
        fail("No trade rows found.")

    if duplicate_keys:
        fail(
            "Duplicate signal/timeframe/horizon rows found: "
            f"{len(duplicate_keys):,}. "
            "Evaluation stopped to prevent double counting."
        )

    summaries = []

    for key in sorted(aggregates):
        timeframe, classification, horizon = key

        summaries.append(
            aggregates[key].summary(
                timeframe,
                classification,
                horizon,
            )
        )

    overall = aggregates[
        ("ALL", "ALL", "ALL")
    ]

    diagnostics = {
        "source_rows": total_rows,
        "completed_rows": overall.completed,
        "incomplete_rows": overall.incomplete,
        "unique_signal_events": len(
            classification_by_event
        ),
        "duplicate_event_horizon_rows": len(
            duplicate_keys
        ),
        "timeframes": sorted(timeframes_seen),
        "horizons": sorted(horizons_seen),
        "smoke_test": smoke_test,
        "full_dataset_evaluated": not smoke_test,
    }

    return summaries, diagnostics


def find_summary(
    summaries: list[dict],
    timeframe: str,
    classification: str,
    horizon: str,
) -> dict | None:
    for row in summaries:
        if (
            row["timeframe"] == timeframe
            and row["classification"] == classification
            and row["horizon"] == horizon
        ):
            return row

    return None


def compare_patterns(
    summaries: list[dict],
) -> list[dict]:
    results = []

    timeframes = sorted({
        row["timeframe"]
        for row in summaries
        if row["timeframe"] != "ALL"
    })

    horizons = sorted({
        row["horizon"]
        for row in summaries
        if row["horizon"] != "ALL"
    }, key=lambda value: int(value))

    for timeframe in timeframes:
        for horizon in horizons:
            baseline = find_summary(
                summaries,
                timeframe,
                "PATTERN_256_ONLY",
                horizon,
            )

            combined = find_summary(
                summaries,
                timeframe,
                "BOTH",
                horizon,
            )

            if baseline is None or combined is None:
                results.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "baseline_completed": (
                        baseline["completed"]
                        if baseline else 0
                    ),
                    "combined_completed": (
                        combined["completed"]
                        if combined else 0
                    ),
                    "baseline_hit_rate_pct": (
                        baseline["hit_rate_pct"]
                        if baseline else None
                    ),
                    "combined_hit_rate_pct": (
                        combined["hit_rate_pct"]
                        if combined else None
                    ),
                    "hit_rate_difference_pp": None,
                    "comparison_status": "INSUFFICIENT_GROUPS",
                })
                continue

            baseline_rate = baseline["hit_rate_pct"]
            combined_rate = combined["hit_rate_pct"]

            if (
                baseline_rate is None
                or combined_rate is None
            ):
                difference = None

            else:
                difference = round(
                    combined_rate - baseline_rate,
                    6,
                )

            if (
                baseline["completed"] < MIN_COMPARISON_SAMPLE
                or combined["completed"] < MIN_COMPARISON_SAMPLE
            ):
                comparison_status = "SMALL_SAMPLE"

            elif difference is None:
                comparison_status = "NOT_EVALUABLE"

            elif difference > 0:
                comparison_status = "OBSERVED_IMPROVEMENT"

            elif difference < 0:
                comparison_status = "OBSERVED_DETERIORATION"

            else:
                comparison_status = "OBSERVED_TIE"

            results.append({
                "timeframe": timeframe,
                "horizon": horizon,
                "baseline_completed": baseline["completed"],
                "combined_completed": combined["completed"],
                "baseline_hit_rate_pct": baseline_rate,
                "combined_hit_rate_pct": combined_rate,
                "hit_rate_difference_pp": difference,
                "comparison_status": comparison_status,
            })

    return results


def build_decisions(
    summaries: list[dict],
    comparisons: list[dict],
    metadata: dict,
    diagnostics: dict,
) -> list[dict]:
    decisions = []

    for comparison in comparisons:
        timeframe = comparison["timeframe"]
        horizon = comparison["horizon"]

        combined = find_summary(
            summaries,
            timeframe,
            "BOTH",
            horizon,
        )

        sample = (
            combined["completed"]
            if combined else 0
        )

        if diagnostics["smoke_test"]:
            decision = "HOLD"
            reason = "SMOKE_TEST_ONLY"

        elif sample < MIN_REVIEW_SAMPLE:
            decision = "HOLD"
            reason = "INSUFFICIENT_SAMPLE"

        elif not metadata["independent_oos_proven"]:
            decision = "HOLD"
            reason = "INDEPENDENT_OOS_NOT_PROVEN"

        elif (
            comparison["comparison_status"]
            == "OBSERVED_DETERIORATION"
        ):
            decision = "REJECT"
            reason = "COMBINED_PATTERN_UNDERPERFORMED"

        elif (
            comparison["comparison_status"]
            == "OBSERVED_IMPROVEMENT"
        ):
            decision = "REVIEW"
            reason = (
                "OBSERVED_IMPROVEMENT_REQUIRES_"
                "INDEPENDENT_STATISTICAL_REVIEW"
            )

        else:
            decision = "HOLD"
            reason = (
                comparison["comparison_status"]
            )

        decisions.append({
            "timeframe": timeframe,
            "horizon": horizon,
            "combined_completed": sample,
            "comparison_status": (
                comparison["comparison_status"]
            ),
            "decision": decision,
            "reason": reason,
            "approved_for_live_trading": False,
        })

    return decisions


def verify_existing_output(
    output_dir: Path,
    current_inputs: dict,
) -> bool:
    manifest_path = (
        output_dir / "independent_evaluation_manifest.json"
    )

    result_path = (
        output_dir / "independent_evaluation_result.json"
    )

    if not manifest_path.exists():
        return False

    if not result_path.exists():
        fail(
            "Existing manifest found but result JSON is missing. "
            "Refusing to overwrite incomplete output."
        )

    manifest = read_json(manifest_path)
    result = read_json(result_path)

    if manifest.get("version") != VERSION:
        fail(
            "Existing evaluation version differs. "
            "Use a separate output directory."
        )

    if manifest.get("input_sha256") != current_inputs:
        fail(
            "Existing evaluation input provenance mismatch. "
            "Refusing unsafe resume."
        )

    output_hashes = manifest.get(
        "output_sha256",
        {},
    )

    if not isinstance(output_hashes, dict):
        fail("Invalid output manifest structure.")

    expected = set(OUTPUT_FILES) - {
        "independent_evaluation_manifest.json"
    }

    if set(output_hashes) != expected:
        fail(
            "Existing output manifest does not list "
            "all expected files."
        )

    for filename, expected_hash in output_hashes.items():
        path = output_dir / filename

        if not path.is_file():
            fail(
                f"Existing output missing: {path}"
            )

        if sha256_file(path) != expected_hash:
            fail(
                f"Existing output SHA256 mismatch: {path}"
            )

    if result.get("execution_status") != "PASS":
        fail(
            "Existing evaluation result is not PASS."
        )

    if result.get("approved_for_live_trading") is not False:
        fail(
            "Existing evaluation contains unexpected "
            "live-trading approval."
        )

    log("[RESUME] Existing completed output verified.")
    log("[RESUME] All SHA256 checks PASS.")

    return True


def snapshot_inputs(
    paths: list[Path],
) -> dict:
    return {
        str(path.resolve()): sha256_file(path)
        for path in paths
    }


def verify_inputs_unchanged(
    snapshot: dict,
) -> None:
    for filename, expected_hash in snapshot.items():
        path = Path(filename)

        if not path.is_file():
            fail(
                f"Production input disappeared: {filename}"
            )

        current_hash = sha256_file(path)

        if current_hash != expected_hash:
            fail(
                f"Production input SHA256 changed: {filename}"
            )

        log(f"[SHA256 PASS] {filename}")


def save_evaluation(
    output_dir: Path,
    summaries: list[dict],
    comparisons: list[dict],
    decisions: list[dict],
    diagnostics: dict,
    metadata: dict,
    input_hashes: dict,
) -> None:
    summary_columns = [
        "timeframe",
        "classification",
        "horizon",
        "total_rows",
        "completed",
        "hits",
        "misses",
        "incomplete",
        "hit_rate_pct",
        "wilson_95_lower_pct",
        "wilson_95_upper_pct",
        "avg_max_return_pct",
        "avg_max_drawdown_pct",
        "best_max_return_pct",
        "worst_max_drawdown_pct",
        "unique_markets",
        "unique_signal_events",
    ]

    comparison_columns = [
        "timeframe",
        "horizon",
        "baseline_completed",
        "combined_completed",
        "baseline_hit_rate_pct",
        "combined_hit_rate_pct",
        "hit_rate_difference_pp",
        "comparison_status",
    ]

    decision_columns = [
        "timeframe",
        "horizon",
        "combined_completed",
        "comparison_status",
        "decision",
        "reason",
        "approved_for_live_trading",
    ]

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    atomic_write_csv(
        output_dir / "independent_evaluation_summary.csv",
        summary_columns,
        summaries,
    )

    atomic_write_csv(
        output_dir / "independent_evaluation_comparison.csv",
        comparison_columns,
        comparisons,
    )

    atomic_write_csv(
        output_dir / "independent_evaluation_decision.csv",
        decision_columns,
        decisions,
    )

    result = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "execution_status": "PASS",
        "diagnostics": diagnostics,
        "source_result": metadata,
        "summary_groups": len(summaries),
        "comparison_groups": len(comparisons),
        "decision_groups": len(decisions),
        "decisions": dict(
            Counter(
                row["decision"]
                for row in decisions
            )
        ),
        "independent_oos_proven": (
            metadata["independent_oos_proven"]
        ),
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "notes": [
            "Execution PASS does not establish predictive profitability.",
            "Wilson intervals assume independent Bernoulli observations.",
            "Overlapping signals can invalidate naive confidence intervals.",
            "H1, H4 and D1 horizons have different elapsed durations.",
            "Observed differences do not prove statistical significance.",
            "Trading fees, slippage and execution risks are not verified.",
            "No automated trading is permitted.",
        ],
    }

    atomic_write_json(
        output_dir / "independent_evaluation_result.json",
        result,
    )

    output_hashes = {}

    for filename in OUTPUT_FILES:
        if filename == "independent_evaluation_manifest.json":
            continue

        path = output_dir / filename

        output_hashes[filename] = sha256_file(path)

    manifest = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "approved_for_live_trading": False,
    }

    # Manifest is written last to mark a completed evaluation.
    atomic_write_json(
        output_dir / "independent_evaluation_manifest.json",
        manifest,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Independent candidate backtest evaluation "
            f"({VERSION})"
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help="Production project root directory.",
    )

    parser.add_argument(
        "--input",
        default=None,
        help="Independent backtest output directory.",
    )

    parser.add_argument(
        "--output",
        default=None,
        help="Evaluation output directory.",
    )

    parser.add_argument(
        "--trades",
        default=None,
        help="Explicit trade-level CSV path.",
    )

    parser.add_argument(
        "--result",
        default=None,
        help="Explicit backtest result JSON path.",
    )

    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Evaluate a limited sample for workflow testing.",
    )

    parser.add_argument(
        "--limit-rows",
        type=int,
        default=1000,
        help="Maximum CSV rows in smoke-test mode.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Permit a new evaluation only when the output "
            "directory contains no existing files."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log("=" * 70)
    log(f"{PROGRAM_NAME} | {VERSION}")
    log("=" * 70)

    root = Path(args.root).expanduser().resolve()

    if not root.is_dir():
        fail(f"Production root missing: {root}")

    input_dir = (
        Path(args.input).expanduser().resolve()
        if args.input
        else root / DEFAULT_INPUT_RELATIVE
    )

    output_dir = (
        Path(args.output).expanduser().resolve()
        if args.output
        else root / DEFAULT_OUTPUT_RELATIVE
    )

    if not input_dir.is_dir():
        fail(
            f"Independent backtest input directory missing: "
            f"{input_dir}"
        )

    if input_dir == output_dir:
        fail(
            "Input and output directories must differ."
        )

    if input_dir in output_dir.parents:
        fail(
            "Output directory must not be inside input directory."
        )

    if output_dir in input_dir.parents:
        fail(
            "Input directory must not be inside output directory."
        )

    if args.limit_rows <= 0:
        fail("--limit-rows must be positive.")

    trades_path = (
        Path(args.trades).expanduser().resolve()
        if args.trades
        else select_trade_csv(input_dir)
    )

    result_path = (
        Path(args.result).expanduser().resolve()
        if args.result
        else select_result_json(input_dir)
    )

    if not trades_path.is_file():
        fail(
            f"Trade CSV missing: {trades_path}"
        )

    if result_path is not None and not result_path.is_file():
        fail(
            f"Backtest result JSON missing: {result_path}"
        )

    input_paths = [trades_path]

    if result_path is not None:
        input_paths.append(result_path)

    # Never permit evaluation output to overlap source files.
    for path in input_paths:
        if output_dir == path.parent:
            fail(
                "Evaluation output cannot overwrite source files."
            )

    log(f"[ROOT] {root}")
    log(f"[INPUT] {input_dir}")
    log(f"[TRADES] {trades_path}")
    log(f"[RESULT] {result_path}")
    log(f"[OUTPUT] {output_dir}")
    log(f"[SMOKE TEST] {args.smoke_test}")

    log("[SNAPSHOT] Production input SHA256")

    input_hashes = snapshot_inputs(input_paths)

    for filename, digest in input_hashes.items():
        log(f"[INPUT SHA256] {filename} = {digest}")

    if output_dir.exists():
        existing_files = [
            path
            for path in output_dir.rglob("*")
            if path.is_file()
        ]

        if existing_files:
            if verify_existing_output(
                output_dir,
                input_hashes,
            ):
                log("[STATUS] PASS - Verified existing output")
                return 0

            fail(
                "Output directory already contains files "
                "without a verified completed manifest. "
                "No files were overwritten."
            )

    log("[READ] Backtest result metadata")

    metadata = check_result_metadata(
        result_path
    )

    log("[ANALYZE] Trade-level backtest records")

    summaries, diagnostics = analyze_trades(
        trades_path=trades_path,
        smoke_test=args.smoke_test,
        limit_rows=args.limit_rows,
    )

    log(
        "[ROWS] "
        f"source={diagnostics['source_rows']:,} "
        f"completed={diagnostics['completed_rows']:,} "
        f"incomplete={diagnostics['incomplete_rows']:,}"
    )

    log(
        "[UNIQUE SIGNAL EVENTS] "
        f"{diagnostics['unique_signal_events']:,}"
    )

    log("[COMPARE] Pattern 256 vs combined pattern")

    comparisons = compare_patterns(
        summaries
    )

    log("[DECIDE] Candidate research decisions")

    decisions = build_decisions(
        summaries,
        comparisons,
        metadata,
        diagnostics,
    )

    log("[VERIFY] Production inputs unchanged")

    verify_inputs_unchanged(
        input_hashes
    )

    log("[WRITE] Evaluation reports")

    save_evaluation(
        output_dir=output_dir,
        summaries=summaries,
        comparisons=comparisons,
        decisions=decisions,
        diagnostics=diagnostics,
        metadata=metadata,
        input_hashes=input_hashes,
    )

    log("[VERIFY] Completed evaluation manifest")

    if not verify_existing_output(
        output_dir,
        input_hashes,
    ):
        fail(
            "Completed output verification failed."
        )

    log("=" * 70)
    log("EXECUTION STATUS: PASS")
    log(f"SOURCE ROWS: {diagnostics['source_rows']:,}")
    log(f"SUMMARY GROUPS: {len(summaries)}")
    log(f"COMPARISON GROUPS: {len(comparisons)}")
    log(f"DECISION GROUPS: {len(decisions)}")
    log("INDEPENDENT OOS PROVEN: "
        f"{metadata['independent_oos_proven']}")
    log("LIVE TRADING APPROVED: FALSE")
    log("=" * 70)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log("[INTERRUPTED] Evaluation stopped.")
        sys.exit(130)

    except Exception as exc:
        log(
            f"[FAIL] {type(exc).__name__}: {exc}"
        )
        traceback.print_exc()
        sys.exit(1)
