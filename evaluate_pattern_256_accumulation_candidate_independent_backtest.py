
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_accumulation_candidate_independent_backtest.py
Clean V003

Independent backtest evaluation.
Standard library only.

Safety:
- Production inputs are read-only.
- Fixed research target: +30%.
- No automatic trading.
- No live-trading approval.
- No profitability or statistical-independence claims.
- SHA256 input/output audit.
- Resume only after complete integrity verification.
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
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V003"
TARGET_PCT = 30.0

INPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest"
)
OUTPUT_RELATIVE = (
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

TRADE_NAMES = (
    "backtest_trades.csv",
    "independent_backtest_trades.csv",
    "independent_backtest_detail.csv",
    "backtest_detail.csv",
    "trade_detail.csv",
    "trades.csv",
)

RESULT_NAMES = (
    "backtest_result.json",
    "independent_backtest_result.json",
    "independent_result.json",
)

ALIASES = {
    "timeframe": ("timeframe",),
    "market": ("market",),
    "signal_timestamp": ("signal_timestamp",),
    "entry_timestamp": ("entry_timestamp",),
    "classification": ("classification",),
    "horizon": ("horizon", "required_bars"),
    "status": ("status",),
    "hit": ("hit_30pct",),
    "max_return": ("max_return_pct",),
    "max_drawdown": ("max_drawdown_pct",),
    "observed_bars": ("observed_bars",),
    "target_return": (
        "target_return_pct",
        "target_return",
    ),
    "entry_price": ("entry_price",),
    "first_hit_bars": ("first_hit_bars",),
}

REQUIRED = (
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
    "entry_price",
)

CLASSIFICATIONS = {
    "PATTERN_256_ONLY",
    "ACCUMULATION_ONLY",
    "BOTH",
    "NEITHER",
}

TIMEFRAMES = {"h1", "h4", "d1"}
COMPLETE = {"HIT", "MISS"}
INCOMPLETE = {
    "INCOMPLETE",
    "PENDING",
    "INSUFFICIENT_DATA",
    "CENSORED",
    "UNKNOWN",
    "SKIP",
    "SKIPPED",
}

SUMMARY_COLUMNS = [
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

COMPARISON_COLUMNS = [
    "timeframe",
    "horizon",
    "baseline_completed",
    "combined_completed",
    "baseline_hit_rate_pct",
    "combined_hit_rate_pct",
    "hit_rate_difference_pp",
    "comparison_status",
]

DECISION_COLUMNS = [
    "timeframe",
    "horizon",
    "combined_completed",
    "comparison_status",
    "decision",
    "reason",
    "approved_for_live_trading",
]


def log(message):
    now = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(f"{now} | {message}", flush=True)


def fail(message):
    raise RuntimeError(message)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024), b""
        ):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path, writer_function):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary = Path(name)
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as handle:
            writer_function(handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_json(path, value):
    def write(handle):
        json.dump(
            value,
            handle,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        handle.write("\n")

    atomic_write(path, write)


def write_csv(path, columns, rows):
    def write(handle):
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

    atomic_write(path, write)


def read_json(path):
    with Path(path).open(
        "r", encoding="utf-8-sig"
    ) as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        fail(f"Invalid JSON object: {path}")
    return value


def normalize(value):
    return str(value).strip().lower().replace(" ", "_")


def column_map(headers):
    normalized = {}
    for header in headers:
        key = normalize(header)
        if key in normalized:
            fail(f"Duplicate CSV column: {key}")
        normalized[key] = header

    mapped = {}
    for canonical, aliases in ALIASES.items():
        matches = [
            normalized[alias]
            for alias in aliases
            if alias in normalized
        ]

        # Two equivalent column names can coexist.
        # Prefer the primary name, but verify values per row.
        if matches:
            mapped[canonical] = matches[0]
            if len(matches) > 1:
                mapped[f"{canonical}__alternates"] = matches[1:]

    return mapped


def get(row, columns, field):
    name = columns.get(field)
    if name is None:
        return ""

    value = row.get(name)
    value = "" if value is None else str(value).strip()

    for alternate in columns.get(
        f"{field}__alternates", []
    ):
        other = row.get(alternate)
        other = "" if other is None else str(other).strip()
        if value and other and value != other:
            if field == "horizon":
                try:
                    equivalent = (
                        float(value) == float(other)
                    )
                except ValueError:
                    equivalent = False
                if not equivalent:
                    fail(
                        f"Conflicting {field}: "
                        f"{value} vs {other}"
                    )
            elif field == "target_return":
                try:
                    equivalent = math.isclose(
                        float(value), float(other),
                        abs_tol=1e-8,
                    )
                except ValueError:
                    equivalent = False
                if not equivalent:
                    fail(
                        f"Conflicting {field}: "
                        f"{value} vs {other}"
                    )
            else:
                fail(
                    f"Conflicting {field}: "
                    f"{value} vs {other}"
                )

        if not value:
            value = other

    return value


def number(value, field, line, blank=False):
    if value == "":
        if blank:
            return None
        fail(f"Missing {field} at line {line}")

    try:
        result = float(value)
    except (TypeError, ValueError):
        fail(f"Invalid {field} at line {line}: {value}")

    if not math.isfinite(result):
        fail(f"Non-finite {field} at line {line}")

    return result


def integer(value, field, line, blank=False):
    parsed = number(value, field, line, blank)
    if parsed is None:
        return None
    if not parsed.is_integer():
        fail(f"Non-integer {field} at line {line}")
    return int(parsed)


def timestamp(value, field, line):
    if not value:
        fail(f"Missing {field} at line {line}")

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError:
        fail(
            f"Invalid {field} at line {line}: {value}"
        )

    if parsed.tzinfo is None:
        fail(
            f"Timezone missing in {field} at line {line}"
        )

    return parsed.astimezone(timezone.utc)


def parse_hit(value, line):
    if value.lower() in ("1", "1.0", "true"):
        return 1
    if value.lower() in ("0", "0.0", "false"):
        return 0
    fail(f"Invalid hit_30pct at line {line}: {value}")


def wilson(hits, total):
    if total == 0:
        return None, None

    z = 1.959963984540054
    p = hits / total
    denominator = 1 + z * z / total
    center = (
        p + z * z / (2 * total)
    ) / denominator
    margin = (
        z
        * math.sqrt(
            p * (1 - p) / total
            + z * z / (4 * total * total)
        )
        / denominator
    )

    return (
        round(max(0, center - margin) * 100, 6),
        round(min(1, center + margin) * 100, 6),
    )


def rounded(value):
    return None if value is None else round(value, 6)


class Aggregate:
    def __init__(self):
        self.total_rows = 0
        self.completed = 0
        self.hits = 0
        self.incomplete = 0
        self.return_sum = 0.0
        self.drawdown_sum = 0.0
        self.best_return = None
        self.worst_drawdown = None
        self.markets = set()
        self.events = set()

    def add(self, event):
        self.total_rows += 1
        self.markets.add(event["market"])
        self.events.add(event["event_key"])

        if not event["completed"]:
            self.incomplete += 1
            return

        self.completed += 1
        self.hits += event["hit"]

        ret = event["max_return"]
        dd = event["max_drawdown"]
        self.return_sum += ret
        self.drawdown_sum += dd

        if (
            self.best_return is None
            or ret > self.best_return
        ):
            self.best_return = ret

        if (
            self.worst_drawdown is None
            or dd < self.worst_drawdown
        ):
            self.worst_drawdown = dd

    def summary(self, key):
        timeframe, classification, horizon = key
        lower, upper = wilson(
            self.hits, self.completed
        )

        return {
            "timeframe": timeframe,
            "classification": classification,
            "horizon": horizon,
            "total_rows": self.total_rows,
            "completed": self.completed,
            "hits": self.hits,
            "misses": self.completed - self.hits,
            "incomplete": self.incomplete,
            "hit_rate_pct": rounded(
                100 * self.hits / self.completed
                if self.completed else None
            ),
            "wilson_95_lower_pct": lower,
            "wilson_95_upper_pct": upper,
            "avg_max_return_pct": rounded(
                self.return_sum / self.completed
                if self.completed else None
            ),
            "avg_max_drawdown_pct": rounded(
                self.drawdown_sum / self.completed
                if self.completed else None
            ),
            "best_max_return_pct": rounded(
                self.best_return
            ),
            "worst_max_drawdown_pct": rounded(
                self.worst_drawdown
            ),
            "unique_markets": len(self.markets),
            "unique_signal_events": len(self.events),
        }


def choose_trades(directory, explicit=None):
    if explicit:
        path = Path(explicit).resolve()
        if not path.is_file():
            fail(f"Trade CSV missing: {path}")
        return path

    for name in TRADE_NAMES:
        path = directory / name
        if path.is_file():
            return path

    matches = []
    for path in sorted(directory.glob("*.csv")):
        with path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            header = next(csv.reader(handle), [])
        columns = column_map(header)
        if all(field in columns for field in REQUIRED):
            matches.append(path)

    if len(matches) != 1:
        fail(
            "Cannot uniquely identify trade CSV: "
            + ", ".join(map(str, matches))
        )
    return matches[0]


def choose_result(directory, explicit=None):
    if explicit:
        path = Path(explicit).resolve()
        if not path.is_file():
            fail(f"Result JSON missing: {path}")
        return path

    matches = [
        directory / name
        for name in RESULT_NAMES
        if (directory / name).is_file()
    ]
    if len(matches) > 1:
        fail("Multiple backtest result JSON files")
    return matches[0] if matches else None


def result_metadata(path):
    if path is None:
        return {
            "result_json_found": False,
            "execution_status": None,
            "independent_oos_proven": False,
            "approved_for_live_trading": False,
            "result_metadata_verified": False,
        }

    source = read_json(path)
    status = str(
        source.get(
            "execution_status",
            source.get("status", ""),
        )
    ).upper()

    if status not in ("PASS", "SUCCESS", "COMPLETED"):
        fail(f"Backtest result not successful: {status}")

    if source.get("approved_for_live_trading") is True:
        fail("Source unexpectedly approves live trading")

    return {
        "result_json_found": True,
        "execution_status": status,
        "independent_oos_proven": (
            source.get("independent_oos_proven") is True
        ),
        "approved_for_live_trading": False,
        "result_metadata_verified": True,
    }


def validate_row(row, columns, line):
    timeframe = get(
        row, columns, "timeframe"
    ).lower()
    market = get(
        row, columns, "market"
    ).upper()
    classification = get(
        row, columns, "classification"
    ).upper()
    status = get(
        row, columns, "status"
    ).upper()

    if timeframe not in TIMEFRAMES:
        fail(f"Invalid timeframe at line {line}")
    if not re.fullmatch(r"KRW-[A-Z0-9]+", market):
        fail(f"Invalid market at line {line}")
    if classification not in CLASSIFICATIONS:
        fail(f"Invalid classification at line {line}")
    if status not in COMPLETE | INCOMPLETE:
        fail(f"Invalid status at line {line}")

    horizon = integer(
        get(row, columns, "horizon"),
        "horizon", line,
    )
    observed = integer(
        get(row, columns, "observed_bars"),
        "observed_bars", line,
    )

    if horizon <= 0:
        fail(f"Invalid horizon at line {line}")
    if not 0 <= observed <= horizon:
        fail(f"Invalid observed_bars at line {line}")

    signal = timestamp(
        get(row, columns, "signal_timestamp"),
        "signal_timestamp", line,
    )
    entry = timestamp(
        get(row, columns, "entry_timestamp"),
        "entry_timestamp", line,
    )

    if entry <= signal:
        fail(f"Entry not after signal at line {line}")

    price = number(
        get(row, columns, "entry_price"),
        "entry_price", line,
    )
    if price <= 0:
        fail(f"Invalid entry_price at line {line}")

    # The source CSV may omit target_return_pct.
    # This evaluator has an explicit fixed +30% target.
    target_text = get(
        row, columns, "target_return"
    )
    if target_text:
        target = number(
            target_text, "target_return_pct", line
        )
        if not math.isclose(
            target, TARGET_PCT, abs_tol=1e-8
        ):
            fail(
                f"Target is not 30% at line {line}: "
                f"{target}"
            )

    completed = status in COMPLETE
    hit_text = get(row, columns, "hit")

    if completed:
        if observed != horizon:
            fail(
                f"Completed event has insufficient "
                f"bars at line {line}"
            )

        hit = parse_hit(hit_text, line)
        if hit != int(status == "HIT"):
            fail(f"Status/hit mismatch at line {line}")

        ret = number(
            get(row, columns, "max_return"),
            "max_return_pct", line,
        )
        dd = number(
            get(row, columns, "max_drawdown"),
            "max_drawdown_pct", line,
        )

        if ret < -100 or dd < -100 or dd > 0.000001:
            fail(f"Invalid return/drawdown at line {line}")

        if hit and ret < TARGET_PCT - 1e-7:
            fail(f"HIT below 30% at line {line}")

        if not hit and ret >= TARGET_PCT - 1e-7:
            fail(f"MISS reached 30% at line {line}")

    else:
        if hit_text.lower() not in (
            "", "0", "0.0", "false"
        ):
            fail(
                f"Incomplete event has hit at line {line}"
            )
        hit = 0
        ret = None
        dd = None

    if "first_hit_bars" in columns:
        first = integer(
            get(row, columns, "first_hit_bars"),
            "first_hit_bars",
            line,
            blank=True,
        )
        if completed and hit:
            if first is None or not 1 <= first <= horizon:
                fail(
                    f"Invalid first_hit_bars at line {line}"
                )
        elif completed and first is not None:
            fail(
                f"Unexpected first_hit_bars at line {line}"
            )

    event_key = (
        timeframe,
        market,
        signal.isoformat(),
        entry.isoformat(),
    )

    return {
        "timeframe": timeframe,
        "market": market,
        "classification": classification,
        "horizon": horizon,
        "completed": completed,
        "hit": hit,
        "max_return": ret,
        "max_drawdown": dd,
        "event_key": event_key,
    }


def analyze_trades(path, smoke_test, limit_rows):
    aggregates = defaultdict(Aggregate)
    seen = set()
    classification_by_event = {}
    horizons = set()
    timeframes = set()
    count = 0

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            fail("Trade CSV header missing")

        columns = column_map(reader.fieldnames)
        missing = [
            field for field in REQUIRED
            if field not in columns
        ]
        if missing:
            fail(
                "Missing required trade CSV columns: "
                + ", ".join(missing)
            )

        for line, row in enumerate(reader, 2):
            if smoke_test and count >= limit_rows:
                break

            if None in row:
                fail(f"Malformed CSV line {line}")

            event = validate_row(row, columns, line)
            count += 1

            key = event["event_key"]
            horizon = event["horizon"]
            classification = event["classification"]
            timeframe = event["timeframe"]

            old = classification_by_event.get(key)
            if old is not None and old != classification:
                fail(
                    f"Conflicting classification at line {line}"
                )
            classification_by_event[key] = classification

            unique = (key, horizon)
            if unique in seen:
                fail(
                    f"Duplicate event/horizon at line {line}"
                )
            seen.add(unique)

            horizons.add(horizon)
            timeframes.add(timeframe)

            for group in (
                (timeframe, classification, str(horizon)),
                (timeframe, classification, "ALL"),
                (timeframe, "ALL", str(horizon)),
                (timeframe, "ALL", "ALL"),
                ("ALL", classification, str(horizon)),
                ("ALL", classification, "ALL"),
                ("ALL", "ALL", str(horizon)),
                ("ALL", "ALL", "ALL"),
            ):
                aggregates[group].add(event)

            if count % 100000 == 0:
                log(f"[PROGRESS] rows={count:,}")

    if count == 0:
        fail("Trade CSV has no evaluated rows")

    summaries = [
        aggregates[key].summary(key)
        for key in sorted(aggregates)
    ]
    overall = aggregates[("ALL", "ALL", "ALL")]

    diagnostics = {
        "source_rows": count,
        "completed_rows": overall.completed,
        "incomplete_rows": overall.incomplete,
        "unique_signal_events": len(
            classification_by_event
        ),
        "duplicate_event_horizon_rows": 0,
        "timeframes": sorted(timeframes),
        "horizons": sorted(horizons),
        "smoke_test": smoke_test,
        "full_dataset_evaluated": not smoke_test,
        "target_return_pct": TARGET_PCT,
        "target_return_source": (
            "FIXED_30PCT_WITH_OPTIONAL_CSV_VALIDATION"
        ),
    }

    return summaries, diagnostics


def find_summary(rows, timeframe, classification, horizon):
    return next(
        (
            row for row in rows
            if row["timeframe"] == timeframe
            and row["classification"] == classification
            and row["horizon"] == horizon
        ),
        None,
    )


def comparisons_from(summaries):
    results = []
    timeframes = sorted({
        row["timeframe"]
        for row in summaries
        if row["timeframe"] != "ALL"
    })
    horizons = sorted(
        {
            row["horizon"]
            for row in summaries
            if row["horizon"] != "ALL"
        },
        key=int,
    )

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

            base_n = (
                baseline["completed"] if baseline else 0
            )
            both_n = (
                combined["completed"] if combined else 0
            )
            base_rate = (
                baseline["hit_rate_pct"]
                if baseline else None
            )
            both_rate = (
                combined["hit_rate_pct"]
                if combined else None
            )

            difference = (
                rounded(both_rate - base_rate)
                if base_rate is not None
                and both_rate is not None
                else None
            )

            if not baseline or not combined:
                status = "INSUFFICIENT_GROUPS"
            elif min(base_n, both_n) < 30:
                status = "SMALL_SAMPLE"
            elif difference is None:
                status = "NOT_EVALUABLE"
            elif difference > 0:
                status = "OBSERVED_IMPROVEMENT"
            elif difference < 0:
                status = "OBSERVED_DETERIORATION"
            else:
                status = "OBSERVED_TIE"

            results.append({
                "timeframe": timeframe,
                "horizon": horizon,
                "baseline_completed": base_n,
                "combined_completed": both_n,
                "baseline_hit_rate_pct": base_rate,
                "combined_hit_rate_pct": both_rate,
                "hit_rate_difference_pp": difference,
                "comparison_status": status,
            })

    return results


def decisions_from(summaries, comparisons, metadata, diagnostics):
    results = []

    for item in comparisons:
        combined = find_summary(
            summaries,
            item["timeframe"],
            "BOTH",
            item["horizon"],
        )
        sample = (
            combined["completed"] if combined else 0
        )
        status = item["comparison_status"]

        if diagnostics["smoke_test"]:
            decision, reason = (
                "HOLD", "SMOKE_TEST_ONLY"
            )
        elif sample < 100:
            decision, reason = (
                "HOLD", "INSUFFICIENT_SAMPLE"
            )
        elif not metadata["independent_oos_proven"]:
            decision, reason = (
                "HOLD", "INDEPENDENT_OOS_NOT_PROVEN"
            )
        elif status == "OBSERVED_DETERIORATION":
            decision, reason = (
                "REJECT",
                "COMBINED_PATTERN_UNDERPERFORMED",
            )
        elif status == "OBSERVED_IMPROVEMENT":
            decision, reason = (
                "REVIEW",
                "OBSERVED_IMPROVEMENT_REQUIRES_"
                "INDEPENDENT_STATISTICAL_REVIEW",
            )
        else:
            decision, reason = "HOLD", status

        results.append({
            "timeframe": item["timeframe"],
            "horizon": item["horizon"],
            "combined_completed": sample,
            "comparison_status": status,
            "decision": decision,
            "reason": reason,
            "approved_for_live_trading": False,
        })

    return results


def input_snapshot(paths):
    return {
        str(path.resolve()): sha256(path)
        for path in paths
    }


def verify_input_snapshot(snapshot):
    for name, expected in snapshot.items():
        path = Path(name)
        if not path.is_file():
            fail(f"Input disappeared: {name}")
        if sha256(path) != expected:
            fail(f"Input SHA256 changed: {name}")
        log(f"[INPUT SHA256 PASS] {name}")


def verify_resume(output, snapshot):
    manifest_path = (
        output / "independent_evaluation_manifest.json"
    )
    result_path = (
        output / "independent_evaluation_result.json"
    )

    if not manifest_path.is_file():
        return False

    if not result_path.is_file():
        fail("Existing manifest without result JSON")

    manifest = read_json(manifest_path)
    result = read_json(result_path)

    if manifest.get("version") != VERSION:
        fail("Existing output version mismatch")

    if manifest.get("input_sha256") != snapshot:
        fail("Existing input SHA256 mismatch")

    expected = set(OUTPUT_FILES) - {
        "independent_evaluation_manifest.json"
    }
    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        fail("Invalid existing output manifest")

    if set(hashes) != expected:
        fail("Existing output file list mismatch")

    for name, expected_hash in hashes.items():
        path = output / name
        if not path.is_file():
            fail(f"Existing output missing: {name}")
        if sha256(path) != expected_hash:
            fail(f"Existing output SHA256 mismatch: {name}")

    if result.get("execution_status") != "PASS":
        fail("Existing result is not PASS")

    if result.get("approved_for_live_trading") is not False:
        fail("Existing result safety flag invalid")

    if manifest.get("approved_for_live_trading") is not False:
        fail("Existing manifest safety flag invalid")

    log("[RESUME] Completed output verified")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", required=True
    )
    parser.add_argument("--input")
    parser.add_argument("--trades")
    parser.add_argument("--result")
    parser.add_argument("--output")
    parser.add_argument(
        "--smoke-test", action="store_true"
    )
    parser.add_argument(
        "--limit-rows", type=int, default=1000
    )
    args = parser.parse_args()

    if args.limit_rows <= 0:
        fail("--limit-rows must be positive")

    root = Path(args.root).resolve()
    if not root.is_dir():
        fail(f"Production root missing: {root}")

    input_dir = (
        Path(args.input).resolve()
        if args.input
        else root / INPUT_RELATIVE
    )
    output_dir = (
        Path(args.output).resolve()
        if args.output
        else root / OUTPUT_RELATIVE
    )

    if not input_dir.is_dir():
        fail(f"Input directory missing: {input_dir}")

    if output_dir == input_dir:
        fail("Input and output directories must differ")

    if output_dir == root:
        fail("Output cannot be production root")

    if input_dir in output_dir.parents:
        fail("Output cannot be inside input directory")

    if output_dir in input_dir.parents:
        fail("Output cannot contain input directory")

    trades = choose_trades(input_dir, args.trades)
    result_path = choose_result(input_dir, args.result)

    source_paths = [trades]
    if result_path is not None:
        source_paths.append(result_path)

    snapshot = input_snapshot(source_paths)

    log("=" * 64)
    log(f"{VERSION} | Independent Backtest Evaluation")
    log(f"[ROOT] {root}")
    log(f"[INPUT] {input_dir}")
    log(f"[TRADES] {trades}")
    log(f"[RESULT] {result_path}")
    log(f"[OUTPUT] {output_dir}")
    log(f"[SMOKE TEST] {args.smoke_test}")
    log(f"[TARGET] {TARGET_PCT}%")

    for name, digest in snapshot.items():
        log(f"[INPUT SHA256] {name} = {digest}")

    if verify_resume(output_dir, snapshot):
        verify_input_snapshot(snapshot)
        return 0

    if output_dir.exists() and any(output_dir.iterdir()):
        fail(
            "Output directory already contains files. "
            "Refusing to overwrite."
        )

    metadata = result_metadata(result_path)
    summaries, diagnostics = analyze_trades(
        trades,
        args.smoke_test,
        args.limit_rows,
    )

    comparisons = comparisons_from(summaries)
    decisions = decisions_from(
        summaries,
        comparisons,
        metadata,
        diagnostics,
    )

    verify_input_snapshot(snapshot)

    output_dir.mkdir(parents=True, exist_ok=True)

    write_csv(
        output_dir / OUTPUT_FILES[0],
        SUMMARY_COLUMNS,
        summaries,
    )
    write_csv(
        output_dir / OUTPUT_FILES[1],
        COMPARISON_COLUMNS,
        comparisons,
    )
    write_csv(
        output_dir / OUTPUT_FILES[2],
        DECISION_COLUMNS,
        decisions,
    )

    result = {
        "program": Path(__file__).name,
        "version": VERSION,
        "execution_status": "PASS",
        "target_return_pct": TARGET_PCT,
        "summary_groups": len(summaries),
        "comparison_groups": len(comparisons),
        "decision_groups": len(decisions),
        "diagnostics": diagnostics,
        "source_metadata": metadata,
        "approved_for_live_trading": False,
        "profitability_proven": False,
        "statistical_independence_proven": False,
        "independent_oos_proven": (
            metadata["independent_oos_proven"]
        ),
        "evaluation_scope": (
            "SMOKE_TEST"
            if args.smoke_test
            else "FULL_DATASET"
        ),
    }

    write_json(
        output_dir / OUTPUT_FILES[3],
        result,
    )

    hashes = {
        name: sha256(output_dir / name)
        for name in OUTPUT_FILES[:-1]
    }

    manifest = {
        "program": Path(__file__).name,
        "version": VERSION,
        "execution_status": "PASS",
        "input_sha256": snapshot,
        "output_sha256": hashes,
        "approved_for_live_trading": False,
        "profitability_proven": False,
        "statistical_independence_proven": False,
    }

    write_json(
        output_dir / OUTPUT_FILES[4],
        manifest,
    )

    verify_input_snapshot(snapshot)

    if not verify_resume(output_dir, snapshot):
        fail("Final output verification failed")

    log(f"[PASS] Source rows: {diagnostics['source_rows']:,}")
    log(f"[PASS] Summary groups: {len(summaries)}")
    log(f"[PASS] Comparison groups: {len(comparisons)}")
    log(f"[PASS] Decision groups: {len(decisions)}")
    log("[PASS] Input SHA256 unchanged")
    log("[PASS] Output SHA256 verified")
    log("[PASS] Live trading approval: FALSE")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log(f"[FAIL] {type(exc).__name__}: {exc}")
        raise
