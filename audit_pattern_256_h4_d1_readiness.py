
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H4/D1 Readiness Audit
Clean V001

Read-only, streaming CSV inspection.

Inputs:
  data/ohlcv/{h4,d1}/KRW-*.csv
  data/signals/256/{h4,d1}/KRW-*.csv

Outputs:
  data/reports/pattern_256_h4_d1_readiness/

No modification of source files.
No strategy training or trading.
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

TIMEFRAMES = {
    "h4": 4 * 3600,
    "d1": 24 * 3600,
}

TIME_COLUMNS = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "time",
)

TRIGGER_COLUMN = "signal_trigger"

OUTPUT_REL = Path(
    "data/reports/pattern_256_h4_d1_readiness"
)


def digest_file(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def write_json(path, data):
    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def parse_timestamp(value):
    value = str(value or "").strip()

    if not value:
        raise ValueError("EMPTY_TIMESTAMP")

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        raise ValueError("TIMEZONE_MISSING")

    return dt.astimezone(timezone.utc)


def detect_time_column(fields):
    for name in TIME_COLUMNS:
        if name in fields:
            return name

    raise ValueError(
        "TIMESTAMP_COLUMN_MISSING"
    )


def parse_trigger(value):
    text = str(value or "").strip().lower()

    if text in ("1", "true"):
        return True

    if text in ("0", "false", ""):
        return False

    try:
        number = float(text)

        if not math.isfinite(number):
            raise ValueError()

        if number in (0.0, 1.0):
            return number == 1.0
    except ValueError:
        pass

    raise ValueError(
        "INVALID_TRIGGER_VALUE"
    )


def inspect_csv(path, timeframe, is_signal):
    expected_seconds = TIMEFRAMES[timeframe]

    result = {
        "file": path.name,
        "rows": 0,
        "first_utc": None,
        "last_utc": None,
        "duplicate_timestamps": 0,
        "reverse_timestamps": 0,
        "gap_events": 0,
        "missing_slots": 0,
        "irregular_intervals": 0,
        "trigger_rows": 0,
        "errors": [],
    }

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:

            reader = csv.DictReader(handle)

            fields = reader.fieldnames or []
            time_column = detect_time_column(fields)

            if is_signal and TRIGGER_COLUMN not in fields:
                raise ValueError(
                    "TRIGGER_COLUMN_MISSING"
                )

            previous = None
            first = None
            last = None

            for row in reader:
                result["rows"] += 1

                dt = parse_timestamp(
                    row.get(time_column)
                )

                if is_signal and parse_trigger(
                    row.get(TRIGGER_COLUMN)
                ):
                    result["trigger_rows"] += 1

                if first is None:
                    first = dt

                if previous is not None:
                    seconds = (
                        dt - previous
                    ).total_seconds()

                    if seconds == 0:
                        result[
                            "duplicate_timestamps"
                        ] += 1

                    elif seconds < 0:
                        result[
                            "reverse_timestamps"
                        ] += 1

                    elif seconds > expected_seconds:
                        ratio = (
                            seconds / expected_seconds
                        )

                        rounded = round(ratio)

                        if abs(ratio - rounded) < 1e-8:
                            result["gap_events"] += 1
                            result["missing_slots"] += (
                                rounded - 1
                            )
                        else:
                            result[
                                "irregular_intervals"
                            ] += 1

                    elif seconds != expected_seconds:
                        result[
                            "irregular_intervals"
                        ] += 1

                previous = dt
                last = dt

            if first is not None:
                result["first_utc"] = first.isoformat()

            if last is not None:
                result["last_utc"] = last.isoformat()

    except Exception as exc:
        result["errors"].append(
            f"{type(exc).__name__}: {exc}"
        )

    return result


def compare_timestamp_streams(ohlcv, signal):
    """
    Streaming equality comparison of timestamps.
    Never loads full CSV files into memory.
    """

    result = {
        "timestamp_match": False,
        "first_mismatch_row": None,
        "error": None,
    }

    try:
        with ohlcv.open(
            "r", encoding="utf-8-sig", newline=""
        ) as left_handle, signal.open(
            "r", encoding="utf-8-sig", newline=""
        ) as right_handle:

            left = csv.DictReader(left_handle)
            right = csv.DictReader(right_handle)

            left_col = detect_time_column(
                left.fieldnames or []
            )

            right_col = detect_time_column(
                right.fieldnames or []
            )

            left_iterator = iter(left)
            right_iterator = iter(right)

            index = 0

            while True:
                left_row = next(
                    left_iterator, None
                )

                right_row = next(
                    right_iterator, None
                )

                if left_row is None and right_row is None:
                    result["timestamp_match"] = True
                    break

                index += 1

                if left_row is None or right_row is None:
                    result["first_mismatch_row"] = index
                    break

                left_time = parse_timestamp(
                    left_row.get(left_col)
                )

                right_time = parse_timestamp(
                    right_row.get(right_col)
                )

                if left_time != right_time:
                    result["first_mismatch_row"] = index
                    break

    except Exception as exc:
        result["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    return result


def inspect_pair(
    market,
    timeframe,
    ohlcv_path,
    signal_path,
):
    ohlcv = inspect_csv(
        ohlcv_path,
        timeframe,
        False,
    )

    signal = inspect_csv(
        signal_path,
        timeframe,
        True,
    )

    matching = compare_timestamp_streams(
        ohlcv_path,
        signal_path,
    )

    problems = []

    if ohlcv["errors"]:
        problems.append("OHLCV_PARSE_ERROR")

    if signal["errors"]:
        problems.append("SIGNAL_PARSE_ERROR")

    if ohlcv["rows"] != signal["rows"]:
        problems.append("ROW_COUNT_MISMATCH")

    if not matching["timestamp_match"]:
        problems.append("TIMESTAMP_MISMATCH")

    for prefix, data in (
        ("OHLCV", ohlcv),
        ("SIGNAL", signal),
    ):
        if data["duplicate_timestamps"]:
            problems.append(
                prefix + "_DUPLICATE_TIMESTAMP"
            )

        if data["reverse_timestamps"]:
            problems.append(
                prefix + "_REVERSE_TIMESTAMP"
            )

        if data["irregular_intervals"]:
            problems.append(
                prefix + "_IRREGULAR_INTERVAL"
            )

    # A missing candle may be a genuine no-trade
    # interval. Report it, do not fabricate candles.
    if ohlcv["gap_events"]:
        problems.append(
            "OHLCV_GAPS_OBSERVED"
        )

    return {
        "timeframe": timeframe,
        "market": market,
        "classification": (
            "PASS" if not problems else "REVIEW"
        ),
        "problems": problems,
        "ohlcv": ohlcv,
        "signal": signal,
        "alignment": matching,
    }


def run_audit(root):
    pairs = []
    inventory = {}
    problems = Counter()

    for timeframe in TIMEFRAMES:
        ohlcv_folder = (
            root / "data" / "ohlcv" / timeframe
        )

        signal_folder = (
            root / "data" / "signals"
            / "256" / timeframe
        )

        if not ohlcv_folder.is_dir():
            raise FileNotFoundError(
                str(ohlcv_folder)
            )

        if not signal_folder.is_dir():
            raise FileNotFoundError(
                str(signal_folder)
            )

        ohlcv_files = {
            path.stem: path
            for path in ohlcv_folder.glob("KRW-*.csv")
        }

        signal_files = {
            path.stem: path
            for path in signal_folder.glob("KRW-*.csv")
        }

        common = sorted(
            set(ohlcv_files) & set(signal_files)
        )

        unmatched_ohlcv = sorted(
            set(ohlcv_files) - set(signal_files)
        )

        unmatched_signal = sorted(
            set(signal_files) - set(ohlcv_files)
        )

        inventory[timeframe] = {
            "ohlcv_files": len(ohlcv_files),
            "signal_files": len(signal_files),
            "matched_files": len(common),
            "unmatched_ohlcv": unmatched_ohlcv,
            "unmatched_signal": unmatched_signal,
        }

        print(
            f"[{timeframe}] "
            f"matched={len(common)}",
            flush=True,
        )

        for index, market in enumerate(common, 1):
            pair = inspect_pair(
                market,
                timeframe,
                ohlcv_files[market],
                signal_files[market],
            )

            pairs.append(pair)

            for problem in pair["problems"]:
                problems[problem] += 1

            if index % 25 == 0:
                print(
                    f"[{timeframe}] "
                    f"{index}/{len(common)}",
                    flush=True,
                )

    summaries = {}

    for timeframe in TIMEFRAMES:
        subset = [
            p for p in pairs
            if p["timeframe"] == timeframe
        ]

        summaries[timeframe] = {
            "matched_pairs": len(subset),
            "passed_pairs": sum(
                p["classification"] == "PASS"
                for p in subset
            ),
            "review_pairs": sum(
                p["classification"] == "REVIEW"
                for p in subset
            ),
            "ohlcv_rows": sum(
                p["ohlcv"]["rows"]
                for p in subset
            ),
            "signal_rows": sum(
                p["signal"]["rows"]
                for p in subset
            ),
            "trigger_rows": sum(
                p["signal"]["trigger_rows"]
                for p in subset
            ),
            "ohlcv_gap_events": sum(
                p["ohlcv"]["gap_events"]
                for p in subset
            ),
            "ohlcv_missing_slots": sum(
                p["ohlcv"]["missing_slots"]
                for p in subset
            ),
        }

    has_review = (
        any(
            v["review_pairs"] > 0
            for v in summaries.values()
        )
        or any(
            inventory[tf]["unmatched_ohlcv"]
            or inventory[tf]["unmatched_signal"]
            for tf in TIMEFRAMES
        )
    )

    return {
        "version": VERSION,
        "execution": "PASS",
        "classification": (
            "INPUT_REVIEW_REQUIRED"
            if has_review
            else "INPUT_ALIGNMENT_PASS"
        ),
        "inventory": inventory,
        "summary": summaries,
        "problem_counts": dict(
            sorted(problems.items())
        ),
        "pairs": pairs,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "notes": [
            "Read-only source inspection",
            "Missing candles are not synthesized",
            "Observed gaps require policy review",
            "No independent OOS validation",
            "No profitability validation",
        ],
    }


def build_report(result):
    lines = [
        "# Pattern 256 H4/D1 Readiness Audit",
        "",
        f"Version: {VERSION}",
        f"Execution: {result['execution']}",
        f"Classification: {result['classification']}",
        "",
        "## Input inventory",
        "",
        "| TF | OHLCV | Signals | Matched |",
        "|---|---:|---:|---:|",
    ]

    for tf in TIMEFRAMES:
        row = result["inventory"][tf]

        lines.append(
            f"| {tf} | {row['ohlcv_files']} "
            f"| {row['signal_files']} "
            f"| {row['matched_files']} |"
        )

    lines.extend([
        "",
        "## Full-row summary",
        "",
        "| TF | Pairs | Pass | Review | "
        "OHLCV rows | Trigger rows | Gaps |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ])

    for tf in TIMEFRAMES:
        row = result["summary"][tf]

        lines.append(
            f"| {tf} "
            f"| {row['matched_pairs']} "
            f"| {row['passed_pairs']} "
            f"| {row['review_pairs']} "
            f"| {row['ohlcv_rows']} "
            f"| {row['trigger_rows']} "
            f"| {row['ohlcv_gap_events']} |"
        )

    lines.extend([
        "",
        "## Problem counts",
        "",
    ])

    if result["problem_counts"]:
        for name, count in result["problem_counts"].items():
            lines.append(
                f"- {name}: {count}"
            )
    else:
        lines.append("- None detected")

    lines.extend([
        "",
        "## Decision",
        "",
        "- Research decision: HOLD",
        "- Independent OOS: NOT PROVEN",
        "- Profitability: NOT PROVEN",
        "- Training: NOT APPROVED",
        "- Live trading: NOT APPROVED",
        "",
        "This is input readiness inspection only.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    if not root.is_dir():
        raise FileNotFoundError(str(root))

    result = run_audit(root)

    destination = root / OUTPUT_REL
    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        destination / "readiness_result.json"
    )

    report_path = (
        destination / "readiness_report.md"
    )

    manifest_path = (
        destination / "readiness_manifest.json"
    )

    write_json(result_path, result)

    report_path.write_text(
        build_report(result),
        encoding="utf-8",
    )

    manifest = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "output_sha256": {
            result_path.name: digest_file(result_path),
            report_path.name: digest_file(report_path),
        },
    }

    write_json(manifest_path, manifest)

    print("==================================")
    print("Pattern 256 H4/D1 Readiness")
    print("==================================")
    print("Execution: PASS")
    print(
        "Classification:",
        result["classification"],
    )

    for tf, row in result["summary"].items():
        print(
            f"{tf}: "
            f"pairs={row['matched_pairs']}, "
            f"review={row['review_pairs']}, "
            f"triggers={row['trigger_rows']}"
        )

    print("Research decision: HOLD")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
