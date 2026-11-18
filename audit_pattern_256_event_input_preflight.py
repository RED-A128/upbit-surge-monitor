
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Input Preflight - Clean V001

Purpose:
- Inspect actual OHLCV and signal CSV schemas.
- Verify market-file correspondence.
- Inspect timestamp and signal examples.
- Detect schema and sample-level anomalies.
- Prepare a safe event-level audit specification.

Does not modify source data.
Does not claim independent OOS or profitability.
"""

import argparse
import csv
import hashlib
import json
import os
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path

VERSION = "Clean V001"
TIMEFRAMES = ("h1", "h4", "d1")
OUTPUT_FOLDER = "pattern_256_event_input_preflight"

TIMESTAMP_CANDIDATES = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "date",
    "time",
)

OHLCV_REQUIRED = (
    "opening_price",
    "high_price",
    "low_price",
    "trade_price",
)

SIGNAL_REQUIRED = (
    "market",
    "signal_setup",
    "signal_ready",
    "signal_trigger",
    "signal_stage",
)

SIGNAL_STAGES = (
    "NONE",
    "SETUP",
    "READY",
    "TRIGGER",
)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)
    return digest.hexdigest()


def write_atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=".preflight_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, data):
    write_atomic(
        path,
        json.dumps(
            data,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def find_timestamp_column(columns):
    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in columns:
            return candidate
    return None


def inspect_file(path, max_rows):
    result = {
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "columns": [],
        "timestamp_column": None,
        "sample_rows": 0,
        "sample_trigger_rows": 0,
        "sample_invalid_timestamps": 0,
        "sample_duplicate_timestamps": 0,
        "sample_unsorted_timestamps": 0,
        "sample_market_mismatches": 0,
        "sample_stage_counts": {},
        "sample_signal_values": [],
        "error": None,
    }

    try:
        with open(
            path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)
            columns = reader.fieldnames

            if not columns:
                raise ValueError("CSV header missing")

            if len(columns) != len(set(columns)):
                raise ValueError("Duplicate CSV columns")

            result["columns"] = columns
            timestamp_column = find_timestamp_column(columns)
            result["timestamp_column"] = timestamp_column

            seen = set()
            previous = None
            stages = Counter()

            for row in reader:
                if result["sample_rows"] >= max_rows:
                    break

                result["sample_rows"] += 1

                if "market" in columns:
                    market = str(row.get("market") or "").strip()
                    expected = path.stem

                    if market and market != expected:
                        result["sample_market_mismatches"] += 1

                if timestamp_column:
                    raw = str(
                        row.get(timestamp_column) or ""
                    ).strip()

                    try:
                        parsed = datetime.fromisoformat(
                            raw.replace("Z", "+00:00")
                        )
                        normalized = parsed.isoformat()

                        if normalized in seen:
                            result[
                                "sample_duplicate_timestamps"
                            ] += 1

                        seen.add(normalized)

                        if (
                            previous is not None
                            and parsed <= previous
                        ):
                            result[
                                "sample_unsorted_timestamps"
                            ] += 1

                        previous = parsed

                    except (ValueError, TypeError):
                        result[
                            "sample_invalid_timestamps"
                        ] += 1

                if "signal_stage" in columns:
                    stage = str(
                        row.get("signal_stage") or ""
                    ).strip()
                    stages[stage] += 1

                if "signal_trigger" in columns:
                    trigger = str(
                        row.get("signal_trigger") or ""
                    ).strip().lower()

                    if trigger in ("1", "true"):
                        result["sample_trigger_rows"] += 1

                        if len(
                            result["sample_signal_values"]
                        ) < 3:
                            result[
                                "sample_signal_values"
                            ].append({
                                "timestamp": row.get(
                                    timestamp_column
                                ) if timestamp_column else None,
                                "signal_setup": row.get(
                                    "signal_setup"
                                ),
                                "signal_ready": row.get(
                                    "signal_ready"
                                ),
                                "signal_trigger": row.get(
                                    "signal_trigger"
                                ),
                                "signal_stage": row.get(
                                    "signal_stage"
                                ),
                            })

            result["sample_stage_counts"] = dict(stages)

    except Exception as exc:
        result["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    return result


def inspect_timeframe(root, timeframe, max_rows):
    ohlcv_folder = (
        root / "data" / "ohlcv" / timeframe
    )
    signal_folder = (
        root / "data" / "signals" / "256" / timeframe
    )

    if not ohlcv_folder.is_dir():
        raise FileNotFoundError(ohlcv_folder)

    if not signal_folder.is_dir():
        raise FileNotFoundError(signal_folder)

    ohlcv_files = {
        path.name: path
        for path in ohlcv_folder.glob("KRW-*.csv")
        if path.is_file()
    }

    signal_files = {
        path.name: path
        for path in signal_folder.glob("KRW-*.csv")
        if path.is_file()
    }

    matching = sorted(
        set(ohlcv_files) & set(signal_files)
    )

    missing_signals = sorted(
        set(ohlcv_files) - set(signal_files)
    )

    missing_ohlcv = sorted(
        set(signal_files) - set(ohlcv_files)
    )

    if not matching:
        raise ValueError(
            f"No matching files for {timeframe}"
        )

    column_signatures = {
        "ohlcv": Counter(),
        "signals": Counter(),
    }

    timestamp_columns = {
        "ohlcv": Counter(),
        "signals": Counter(),
    }

    examples = []
    anomalies = []
    total_sampled = 0
    total_sampled_triggers = 0

    for filename in matching:
        inspected = {}

        for kind, path in (
            ("ohlcv", ohlcv_files[filename]),
            ("signals", signal_files[filename]),
        ):
            item = inspect_file(path, max_rows)
            inspected[kind] = item

            signature = tuple(item["columns"])
            column_signatures[kind][signature] += 1

            timestamp_columns[kind][
                str(item["timestamp_column"])
            ] += 1

            total_sampled += item["sample_rows"]

            if kind == "signals":
                total_sampled_triggers += (
                    item["sample_trigger_rows"]
                )

            required = (
                OHLCV_REQUIRED
                if kind == "ohlcv"
                else SIGNAL_REQUIRED
            )

            missing = sorted(
                set(required) - set(item["columns"])
            )

            problems = []

            if item["error"]:
                problems.append(item["error"])

            if missing:
                problems.append(
                    "Missing columns: " + ", ".join(missing)
                )

            if not item["timestamp_column"]:
                problems.append("Timestamp column missing")

            for field in (
                "sample_invalid_timestamps",
                "sample_duplicate_timestamps",
                "sample_unsorted_timestamps",
                "sample_market_mismatches",
            ):
                if item[field] > 0:
                    problems.append(
                        f"{field}={item[field]}"
                    )

            if problems:
                anomalies.append({
                    "filename": filename,
                    "kind": kind,
                    "problems": problems,
                })

        if len(examples) < 5:
            examples.append({
                "filename": filename,
                "ohlcv": inspected["ohlcv"],
                "signals": inspected["signals"],
            })

    signatures = {}

    for kind in ("ohlcv", "signals"):
        signatures[kind] = [
            {
                "columns": list(columns),
                "file_count": count,
            }
            for columns, count in sorted(
                column_signatures[kind].items(),
                key=lambda item: (
                    -item[1],
                    item[0],
                ),
            )
        ]

    return {
        "timeframe": timeframe,
        "ohlcv_files": len(ohlcv_files),
        "signal_files": len(signal_files),
        "matching_files": len(matching),
        "missing_signal_files": missing_signals,
        "missing_ohlcv_files": missing_ohlcv,
        "sampled_rows": total_sampled,
        "sampled_trigger_rows": total_sampled_triggers,
        "timestamp_columns": {
            kind: dict(timestamp_columns[kind])
            for kind in ("ohlcv", "signals")
        },
        "schema_signatures": signatures,
        "anomaly_count": len(anomalies),
        "anomalies": anomalies,
        "examples": examples,
    }


def build_report(result):
    lines = [
        "# Pattern 256 Event Input Preflight",
        "",
        f"Version: {VERSION}",
        f"Execution: {result['execution']}",
        f"Classification: {result['classification']}",
        "Research decision: HOLD",
        "",
        "## File inventory",
        "",
        "| TF | OHLCV | Signals | Matched | "
        "Anomalies | Sample triggers |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for item in result["timeframes"]:
        lines.append(
            f"| {item['timeframe']} "
            f"| {item['ohlcv_files']} "
            f"| {item['signal_files']} "
            f"| {item['matching_files']} "
            f"| {item['anomaly_count']} "
            f"| {item['sampled_trigger_rows']} |"
        )

    lines.extend([
        "",
        "## Schema signatures",
        "",
    ])

    for item in result["timeframes"]:
        lines.append(
            f"### {item['timeframe']}"
        )

        for kind in ("ohlcv", "signals"):
            lines.append(
                f"#### {kind}"
            )

            for signature in item[
                "schema_signatures"
            ][kind]:
                columns = ", ".join(
                    signature["columns"]
                )
                lines.append(
                    f"- {signature['file_count']} files: "
                    f"{columns}"
                )

    lines.extend([
        "",
        "## Audit limitations",
        "",
        "- Only the first N rows per file are sampled.",
        "- Full timestamp alignment is not verified.",
        "- Full OHLCV integrity is not verified.",
        "- Event-level outcome calculations are not performed.",
        "- No market clustering or volatility matching.",
        "- No independent out-of-sample validation.",
        "- No point-in-time provenance verification.",
        "- No executable profitability calculation.",
        "- Training and live trading are not approved.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--sample-rows",
        type=int,
        default=200,
    )

    args = parser.parse_args()

    if not 1 <= args.sample_rows <= 10000:
        raise ValueError(
            "sample-rows must be between 1 and 10000"
        )

    root = Path(args.root).resolve()

    if not root.is_dir():
        raise FileNotFoundError(root)

    timeframes = [
        inspect_timeframe(
            root,
            timeframe,
            args.sample_rows,
        )
        for timeframe in TIMEFRAMES
    ]

    anomaly_count = sum(
        item["anomaly_count"]
        for item in timeframes
    )

    unmatched_count = sum(
        len(item["missing_signal_files"])
        + len(item["missing_ohlcv_files"])
        for item in timeframes
    )

    if anomaly_count or unmatched_count:
        classification = (
            "INPUT_REVIEW_REQUIRED"
        )
    else:
        classification = (
            "SAMPLED_SCHEMA_PREFLIGHT_COMPLETE"
        )

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification": classification,
        "sample_rows_per_file": args.sample_rows,
        "timeframes": timeframes,
        "total_matched_files": sum(
            item["matching_files"]
            for item in timeframes
        ),
        "total_anomalies": anomaly_count,
        "total_unmatched_files": unmatched_count,
        "event_level_audit_complete": False,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    output = (
        root
        / "data"
        / "reports"
        / OUTPUT_FOLDER
    )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_json(
        output / "preflight_result.json",
        result,
    )

    write_atomic(
        output / "preflight_report.md",
        build_report(result),
    )

    files = (
        "preflight_result.json",
        "preflight_report.md",
    )

    write_json(
        output / "preflight_manifest.json",
        {
            "version": VERSION,
            "files": {
                name: sha256(output / name)
                for name in files
            },
        },
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "classification": classification,
                "total_matched_files":
                    result["total_matched_files"],
                "total_anomalies":
                    anomaly_count,
                "total_unmatched_files":
                    unmatched_count,
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
