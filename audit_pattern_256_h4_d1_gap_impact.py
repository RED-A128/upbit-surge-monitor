
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H4/D1 Gap Impact Audit
Clean V001

Read-only audit of missing candle intervals and
signal_trigger adjacency.

No missing candles are generated.
No source files are changed.
No OOS or profitability claims.
"""

import argparse
import csv
import hashlib
import json
import math
import sys

from collections import Counter
from datetime import datetime, timezone
from itertools import zip_longest
from pathlib import Path


VERSION = "Clean V001"

TIMEFRAMES = {
    "h4": 14400,
    "d1": 86400,
}

TIME_COLUMNS = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "time",
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_h4_d1_gap_impact"
)


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def save_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def parse_time(value):
    text = str(value or "").strip()

    if not text:
        raise ValueError("Empty timestamp")

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    result = datetime.fromisoformat(text)

    if result.tzinfo is None:
        raise ValueError(
            "Timestamp without timezone"
        )

    return result.astimezone(timezone.utc)


def time_column(fieldnames):
    for column in TIME_COLUMNS:
        if column in fieldnames:
            return column

    raise ValueError(
        "Missing UTC timestamp column"
    )


def parse_trigger(value):
    text = str(value or "").strip().lower()

    if text in ("true", "1"):
        return True

    if text in ("false", "0", ""):
        return False

    try:
        number = float(text)

        if (
            math.isfinite(number)
            and number in (0.0, 1.0)
        ):
            return number == 1.0
    except ValueError:
        pass

    raise ValueError(
        f"Invalid signal_trigger: {value}"
    )


def inspect_pair(
    timeframe,
    market,
    ohlcv_path,
    signal_path,
    radius,
):
    expected = TIMEFRAMES[timeframe]

    result = {
        "timeframe": timeframe,
        "market": market,
        "status": "PASS",
        "rows": 0,
        "trigger_rows": 0,
        "gap_events": 0,
        "missing_slots": 0,
        "max_missing_slots": 0,
        "signals_near_gaps": 0,
        "gap_details": [],
        "errors": [],
    }

    # Keep recent triggers to identify signals
    # immediately preceding a missing interval.
    recent_triggers = []
    pending_after = []

    try:
        with ohlcv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as left_handle, signal_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as right_handle:

            left = csv.DictReader(left_handle)
            right = csv.DictReader(right_handle)

            left_time = time_column(
                left.fieldnames or []
            )

            right_time = time_column(
                right.fieldnames or []
            )

            if "signal_trigger" not in (
                right.fieldnames or []
            ):
                raise ValueError(
                    "Missing signal_trigger column"
                )

            previous_time = None
            previous_row = 0

            for index, (price, signal) in enumerate(
                zip_longest(left, right),
                start=1,
            ):
                if price is None or signal is None:
                    raise ValueError(
                        f"Row count mismatch at {index}"
                    )

                timestamp = parse_time(
                    price.get(left_time)
                )

                signal_time = parse_time(
                    signal.get(right_time)
                )

                if timestamp != signal_time:
                    raise ValueError(
                        f"Timestamp mismatch at {index}"
                    )

                triggered = parse_trigger(
                    signal.get("signal_trigger")
                )

                result["rows"] += 1

                if triggered:
                    result["trigger_rows"] += 1

                if previous_time is not None:
                    elapsed = (
                        timestamp - previous_time
                    ).total_seconds()

                    if elapsed <= 0:
                        raise ValueError(
                            f"Non-increasing timestamp "
                            f"at {index}"
                        )

                    ratio = elapsed / expected
                    rounded = round(ratio)

                    if abs(ratio - rounded) > 1e-8:
                        raise ValueError(
                            f"Irregular interval "
                            f"at {index}: {elapsed}"
                        )

                    if rounded > 1:
                        missing = rounded - 1

                        result["gap_events"] += 1
                        result["missing_slots"] += missing

                        result["max_missing_slots"] = max(
                            result["max_missing_slots"],
                            missing,
                        )

                        before = [
                            row_number
                            for row_number
                            in recent_triggers
                            if previous_row - row_number
                            < radius
                        ]

                        detail = {
                            "before_utc": (
                                previous_time.isoformat()
                            ),
                            "after_utc": (
                                timestamp.isoformat()
                            ),
                            "missing_slots": missing,
                            "before_row": previous_row,
                            "after_row": index,
                            "trigger_before_rows": before,
                            "trigger_after_rows": [],
                        }

                        result["gap_details"].append(
                            detail
                        )

                        pending_after.append(
                            (index + radius - 1, detail)
                        )

                if triggered:
                    for _, detail in pending_after:
                        detail[
                            "trigger_after_rows"
                        ].append(index)

                    recent_triggers.append(index)

                # Remove expired trigger history.
                recent_triggers = [
                    row_number
                    for row_number in recent_triggers
                    if index - row_number < radius
                ]

                # Remove gaps after the proximity
                # window has been completely checked.
                pending_after = [
                    (end_row, detail)
                    for end_row, detail in pending_after
                    if end_row > index
                ]

                previous_time = timestamp
                previous_row = index

        adjacent_rows = set()

        for detail in result["gap_details"]:
            adjacent_rows.update(
                detail["trigger_before_rows"]
            )

            adjacent_rows.update(
                detail["trigger_after_rows"]
            )

        result["signals_near_gaps"] = len(
            adjacent_rows
        )

        if result["gap_events"] > 0:
            result["status"] = "GAP_REVIEW"

    except Exception as exc:
        result["status"] = "ERROR"

        result["errors"].append(
            f"{type(exc).__name__}: {exc}"
        )

    return result


def run(root, radius):
    records = []
    summary = {}
    issue_counts = Counter()

    for timeframe in TIMEFRAMES:
        ohlcv_dir = (
            root / "data" / "ohlcv" / timeframe
        )

        signal_dir = (
            root / "data" / "signals"
            / "256" / timeframe
        )

        if not ohlcv_dir.is_dir():
            raise FileNotFoundError(
                str(ohlcv_dir)
            )

        if not signal_dir.is_dir():
            raise FileNotFoundError(
                str(signal_dir)
            )

        price_files = {
            path.stem: path
            for path in ohlcv_dir.glob("KRW-*.csv")
        }

        signal_files = {
            path.stem: path
            for path in signal_dir.glob("KRW-*.csv")
        }

        missing_signals = sorted(
            set(price_files) - set(signal_files)
        )

        missing_prices = sorted(
            set(signal_files) - set(price_files)
        )

        if missing_signals or missing_prices:
            raise ValueError(
                f"{timeframe}: unmatched source files; "
                f"missing_signals={missing_signals}, "
                f"missing_prices={missing_prices}"
            )

        markets = sorted(price_files)

        print(
            f"[{timeframe}] markets={len(markets)}",
            flush=True,
        )

        for index, market in enumerate(
            markets,
            start=1,
        ):
            record = inspect_pair(
                timeframe,
                market,
                price_files[market],
                signal_files[market],
                radius,
            )

            records.append(record)

            if record["status"] == "ERROR":
                issue_counts["PAIR_ERROR"] += 1

            if index % 25 == 0:
                print(
                    f"[{timeframe}] "
                    f"{index}/{len(markets)}",
                    flush=True,
                )

        subset = [
            item for item in records
            if item["timeframe"] == timeframe
        ]

        summary[timeframe] = {
            "markets": len(subset),
            "clean_markets": sum(
                x["status"] == "PASS"
                for x in subset
            ),
            "gap_review_markets": sum(
                x["status"] == "GAP_REVIEW"
                for x in subset
            ),
            "error_markets": sum(
                x["status"] == "ERROR"
                for x in subset
            ),
            "rows": sum(
                x["rows"] for x in subset
            ),
            "trigger_rows": sum(
                x["trigger_rows"]
                for x in subset
            ),
            "gap_events": sum(
                x["gap_events"]
                for x in subset
            ),
            "missing_slots": sum(
                x["missing_slots"]
                for x in subset
            ),
            "max_missing_slots": max(
                (
                    x["max_missing_slots"]
                    for x in subset
                ),
                default=0,
            ),
            "signals_near_gaps_sum": sum(
                x["signals_near_gaps"]
                for x in subset
            ),
        }

    has_errors = (
        issue_counts["PAIR_ERROR"] > 0
    )

    return {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": (
            "FAIL" if has_errors else "PASS"
        ),
        "classification": (
            "INPUT_ERROR"
            if has_errors
            else "GAP_IMPACT_REVIEW_COMPLETED"
        ),
        "proximity_radius_rows": radius,
        "summary": summary,
        "issue_counts": dict(issue_counts),
        "records": records,
        "gap_cause_proven": False,
        "future_outcome_gap_safety_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Gap causes cannot be inferred from CSV",
            "No missing candles were synthesized",
            "Near-gap signals are descriptive only",
            "Future outcome windows not evaluated",
            "No independent OOS or cost evaluation",
        ],
    }


def make_report(result):
    lines = [
        "# Pattern 256 H4/D1 Gap Impact Audit",
        "",
        f"Version: {VERSION}",
        f"Execution: {result['execution']}",
        (
            "Classification: "
            + result["classification"]
        ),
        "",
        "## Summary",
        "",
        (
            "| TF | Markets | Gap review | "
            "Errors | Gap events | Missing slots | "
            "Max gap | Near-gap signals |"
        ),
        (
            "|---|---:|---:|---:|---:|"
            "---:|---:|---:|"
        ),
    ]

    for tf, row in result["summary"].items():
        lines.append(
            f"| {tf} "
            f"| {row['markets']} "
            f"| {row['gap_review_markets']} "
            f"| {row['error_markets']} "
            f"| {row['gap_events']} "
            f"| {row['missing_slots']} "
            f"| {row['max_missing_slots']} "
            f"| {row['signals_near_gaps_sum']} |"
        )

    lines.extend([
        "",
        "## Research decision",
        "",
        "- Decision: HOLD",
        "- Gap cause: NOT PROVEN",
        "- Future outcome safety: NOT PROVEN",
        "- Independent OOS: NOT PROVEN",
        "- Profitability: NOT PROVEN",
        "- Training: NOT APPROVED",
        "- Live trading: NOT APPROVED",
        "",
        (
            "Near-gap signals count distinct "
            "trigger rows within each market."
        ),
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--radius",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    if args.radius < 1 or args.radius > 100:
        raise ValueError(
            "radius must be between 1 and 100"
        )

    root = Path(args.root).resolve()

    if not root.is_dir():
        raise FileNotFoundError(str(root))

    result = run(root, args.radius)

    output_dir = root / OUTPUT_DIR

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output_dir / "gap_impact_result.json"
    )

    report_path = (
        output_dir / "gap_impact_report.md"
    )

    manifest_path = (
        output_dir / "gap_impact_manifest.json"
    )

    save_json(result_path, result)

    report_path.write_text(
        make_report(result),
        encoding="utf-8",
    )

    manifest = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "output_sha256": {
            result_path.name: sha256(result_path),
            report_path.name: sha256(report_path),
        },
    }

    save_json(manifest_path, manifest)

    print("=================================")
    print("Pattern 256 H4/D1 Gap Impact")
    print("=================================")

    print(
        "Execution:",
        result["execution"],
    )

    print(
        "Classification:",
        result["classification"],
    )

    for tf, row in result["summary"].items():
        print(
            f"{tf}: "
            f"gaps={row['gap_events']}, "
            f"missing={row['missing_slots']}, "
            f"near_signals="
            f"{row['signals_near_gaps_sum']}"
        )

    print("Research decision: HOLD")

    if result["execution"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
