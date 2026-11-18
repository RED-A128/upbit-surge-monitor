
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Input Integrity Audit
Clean V002

Full-row audit for OHLCV and Pattern 256 signals.

Checks:
- Actual normalized OHLCV schema
- All rows, not sampled rows
- Market identity
- Timestamp validity and strict ordering
- Duplicate timestamps
- OHLCV numeric validity
- OHLC consistency
- Signal flag validity
- OHLCV/signal timestamp alignment
- OHLCV/signal price consistency
- Source SHA256 fingerprints
- Atomic report writing

Read-only source inspection.
No trading, training, or OOS approval.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V002"
TIMEFRAMES = ("h1", "h4", "d1")
REPORT_DIR = "pattern_256_event_input_integrity"

PRICE_COLUMNS = ("open", "high", "low", "close")
OHLCV_COLUMNS = (
    "market",
    "candle_date_time_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

SIGNAL_COLUMNS = (
    "market",
    "candle_date_time_utc",
    "open",
    "high",
    "low",
    "close",
    "signal_setup",
    "signal_ready",
    "signal_trigger",
    "signal_stage",
)

FLAG_VALUES = {"0", "1", "true", "false"}
STAGE_VALUES = {"NONE", "SETUP", "READY", "TRIGGER"}


def file_hash(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024), b""
        ):
            digest.update(block)
    return digest.hexdigest()


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary = tempfile.mkstemp(
        prefix=".integrity_",
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


def atomic_json(path, data):
    atomic_text(
        path,
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def parse_time(value):
    value = str(value or "").strip()

    if not value:
        raise ValueError("empty timestamp")

    parsed = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def number(value):
    result = float(value)

    if not math.isfinite(result):
        raise ValueError("non-finite number")

    return result


def same_number(a, b):
    return math.isclose(
        a, b,
        rel_tol=1e-9,
        abs_tol=1e-12,
    )


def inspect_csv(path, market, is_signal):
    required = (
        SIGNAL_COLUMNS if is_signal else OHLCV_COLUMNS
    )

    issues = Counter()
    examples = []
    row_count = 0
    trigger_count = 0
    previous_time = None
    first_time = None
    last_time = None

    def record(code, row_number, detail=""):
        issues[code] += 1

        if len(examples) < 12:
            examples.append({
                "code": code,
                "row": row_number,
                "detail": str(detail)[:200],
            })

    try:
        with open(
            path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)
            columns = reader.fieldnames or []

            if len(columns) != len(set(columns)):
                record("DUPLICATE_COLUMNS", 1)

            missing = sorted(set(required) - set(columns))

            if missing:
                record(
                    "MISSING_COLUMNS",
                    1,
                    ",".join(missing),
                )

                return {
                    "file": path.name,
                    "rows": 0,
                    "triggers": 0,
                    "first_timestamp": None,
                    "last_timestamp": None,
                    "issues": dict(issues),
                    "examples": examples,
                    "sha256": file_hash(path),
                }

            for row_number, row in enumerate(
                reader, start=2
            ):
                row_count += 1

                if None in row:
                    record("EXTRA_FIELDS", row_number)

                if any(
                    row.get(col) is None
                    for col in required
                ):
                    record("SHORT_ROW", row_number)
                    continue

                if str(row["market"]).strip() != market:
                    record(
                        "MARKET_MISMATCH",
                        row_number,
                        row.get("market"),
                    )

                try:
                    timestamp = parse_time(
                        row["candle_date_time_utc"]
                    )
                except (ValueError, TypeError) as exc:
                    record(
                        "INVALID_TIMESTAMP",
                        row_number,
                        exc,
                    )
                    continue

                if first_time is None:
                    first_time = timestamp

                last_time = timestamp

                if previous_time is not None:
                    if timestamp == previous_time:
                        record(
                            "DUPLICATE_TIMESTAMP",
                            row_number,
                        )
                    elif timestamp < previous_time:
                        record(
                            "UNSORTED_TIMESTAMP",
                            row_number,
                        )

                previous_time = timestamp

                try:
                    values = {
                        col: number(row[col])
                        for col in PRICE_COLUMNS
                    }

                    if not is_signal:
                        values["volume"] = number(
                            row["volume"]
                        )

                except (ValueError, TypeError) as exc:
                    record(
                        "INVALID_NUMERIC",
                        row_number,
                        exc,
                    )
                    continue

                o = values["open"]
                h = values["high"]
                l = values["low"]
                c = values["close"]

                if min(o, h, l, c) <= 0:
                    record(
                        "NONPOSITIVE_PRICE",
                        row_number,
                    )

                if h < max(o, c, l):
                    record(
                        "INVALID_HIGH",
                        row_number,
                    )

                if l > min(o, c, h):
                    record(
                        "INVALID_LOW",
                        row_number,
                    )

                if not is_signal:
                    if values["volume"] < 0:
                        record(
                            "NEGATIVE_VOLUME",
                            row_number,
                        )

                else:
                    for flag in (
                        "signal_setup",
                        "signal_ready",
                        "signal_trigger",
                    ):
                        value = str(
                            row[flag]
                        ).strip().lower()

                        if value not in FLAG_VALUES:
                            record(
                                "INVALID_SIGNAL_FLAG",
                                row_number,
                                f"{flag}={value}",
                            )

                    trigger = str(
                        row["signal_trigger"]
                    ).strip().lower()

                    if trigger in ("1", "true"):
                        trigger_count += 1

                    stage = str(
                        row["signal_stage"]
                    ).strip().upper()

                    if stage not in STAGE_VALUES:
                        record(
                            "INVALID_SIGNAL_STAGE",
                            row_number,
                            stage,
                        )

    except Exception as exc:
        record(
            "FILE_READ_ERROR",
            row_count + 2,
            f"{type(exc).__name__}: {exc}",
        )

    if row_count == 0:
        record("EMPTY_DATA", 1)

    return {
        "file": path.name,
        "rows": row_count,
        "triggers": trigger_count,
        "first_timestamp": (
            first_time.isoformat()
            if first_time else None
        ),
        "last_timestamp": (
            last_time.isoformat()
            if last_time else None
        ),
        "issues": dict(issues),
        "examples": examples,
        "sha256": file_hash(path),
    }


def aligned_rows(ohlcv_path, signal_path):
    """
    Compare full streams by row position.

    If timestamps differ, record mismatch.
    No automatic forward filling or index matching.
    """

    issues = Counter()
    examples = []

    try:
        with open(
            ohlcv_path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as left, open(
            signal_path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as right:

            ohlcv = csv.DictReader(left)
            signals = csv.DictReader(right)

            for index, (a, b) in enumerate(
                zip_longest_rows(ohlcv, signals),
                start=1,
            ):
                if a is None or b is None:
                    issues["ROW_COUNT_MISMATCH"] += 1
                    continue

                try:
                    ta = parse_time(
                        a["candle_date_time_utc"]
                    )
                    tb = parse_time(
                        b["candle_date_time_utc"]
                    )

                    if ta != tb:
                        issues[
                            "TIMESTAMP_ALIGNMENT_MISMATCH"
                        ] += 1

                        if len(examples) < 8:
                            examples.append({
                                "row": index,
                                "ohlcv_time": ta.isoformat(),
                                "signal_time": tb.isoformat(),
                            })

                    for col in PRICE_COLUMNS:
                        va = number(a[col])
                        vb = number(b[col])

                        if not same_number(va, vb):
                            issues[
                                "PRICE_ALIGNMENT_MISMATCH"
                            ] += 1

                            if len(examples) < 8:
                                examples.append({
                                    "row": index,
                                    "column": col,
                                    "ohlcv": va,
                                    "signal": vb,
                                })

                except (
                    ValueError,
                    TypeError,
                    KeyError,
                ) as exc:
                    issues[
                        "ALIGNMENT_PARSE_ERROR"
                    ] += 1

                    if len(examples) < 8:
                        examples.append({
                            "row": index,
                            "error": str(exc),
                        })

    except Exception as exc:
        issues["ALIGNMENT_READ_ERROR"] += 1
        examples.append({
            "error": (
                f"{type(exc).__name__}: {exc}"
            )
        })

    return {
        "issues": dict(issues),
        "examples": examples,
    }


def zip_longest_rows(left, right):
    sentinel = object()
    a = iter(left)
    b = iter(right)

    while True:
        x = next(a, sentinel)
        y = next(b, sentinel)

        if x is sentinel and y is sentinel:
            return

        yield (
            None if x is sentinel else x,
            None if y is sentinel else y,
        )


def run_audit(root):
    entries = []
    totals = Counter()

    for timeframe in TIMEFRAMES:
        ohlcv_dir = (
            root / "data" / "ohlcv" / timeframe
        )
        signal_dir = (
            root / "data" / "signals"
            / "256" / timeframe
        )

        if not ohlcv_dir.is_dir():
            raise FileNotFoundError(ohlcv_dir)

        if not signal_dir.is_dir():
            raise FileNotFoundError(signal_dir)

        ohlcv_files = {
            p.name: p
            for p in ohlcv_dir.glob("KRW-*.csv")
            if p.is_file()
        }

        signal_files = {
            p.name: p
            for p in signal_dir.glob("KRW-*.csv")
            if p.is_file()
        }

        all_names = sorted(
            set(ohlcv_files) | set(signal_files)
        )

        if not all_names:
            raise ValueError(
                f"No input files: {timeframe}"
            )

        for index, filename in enumerate(
            all_names, start=1
        ):
            market = Path(filename).stem

            entry = {
                "timeframe": timeframe,
                "market": market,
                "file": filename,
                "status": "PASS",
                "ohlcv": None,
                "signal": None,
                "alignment": None,
                "issues": {},
            }

            combined = Counter()

            if filename not in ohlcv_files:
                combined["MISSING_OHLCV_FILE"] += 1

            if filename not in signal_files:
                combined["MISSING_SIGNAL_FILE"] += 1

            if (
                filename in ohlcv_files
                and filename in signal_files
            ):
                entry["ohlcv"] = inspect_csv(
                    ohlcv_files[filename],
                    market,
                    False,
                )

                entry["signal"] = inspect_csv(
                    signal_files[filename],
                    market,
                    True,
                )

                entry["alignment"] = aligned_rows(
                    ohlcv_files[filename],
                    signal_files[filename],
                )

                for section in (
                    "ohlcv",
                    "signal",
                    "alignment",
                ):
                    combined.update(
                        entry[section]["issues"]
                    )

                totals["ohlcv_rows"] += (
                    entry["ohlcv"]["rows"]
                )
                totals["signal_rows"] += (
                    entry["signal"]["rows"]
                )
                totals["signal_triggers"] += (
                    entry["signal"]["triggers"]
                )

            entry["issues"] = dict(combined)

            if combined:
                entry["status"] = "REVIEW_REQUIRED"
                totals["review_files"] += 1
            else:
                totals["pass_files"] += 1

            totals["file_pairs"] += 1
            entries.append(entry)

            if index % 25 == 0:
                print(
                    f"[{timeframe}] {index}/{len(all_names)}",
                    flush=True,
                )

    classification = (
        "FULL_ROW_INPUT_INTEGRITY_PASS"
        if totals["review_files"] == 0
        else "FULL_ROW_INPUT_REVIEW_REQUIRED"
    )

    return {
        "version": VERSION,
        "execution": "PASS",
        "classification": classification,
        "audit_scope": "FULL_ROWS",
        "timeframes": list(TIMEFRAMES),
        "totals": dict(totals),
        "entries": entries,
        "full_row_input_integrity_proven": (
            totals["review_files"] == 0
        ),
        "event_level_outcomes_verified": False,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }


def build_report(result):
    totals = result["totals"]

    lines = [
        "# Pattern 256 Event Input Integrity",
        "",
        f"Version: {VERSION}",
        f"Execution: {result['execution']}",
        f"Classification: {result['classification']}",
        "Research decision: HOLD",
        "",
        "## Full-row totals",
        "",
        f"- File pairs: {totals.get('file_pairs', 0)}",
        f"- Passed pairs: {totals.get('pass_files', 0)}",
        f"- Review pairs: {totals.get('review_files', 0)}",
        f"- OHLCV rows: {totals.get('ohlcv_rows', 0)}",
        f"- Signal rows: {totals.get('signal_rows', 0)}",
        f"- Trigger rows: {totals.get('signal_triggers', 0)}",
        "",
        "## Issue categories",
        "",
    ]

    issues = Counter()

    for entry in result["entries"]:
        issues.update(entry["issues"])

    if issues:
        for name, count in sorted(issues.items()):
            lines.append(f"- {name}: {count}")
    else:
        lines.append("- No detected input issues")

    lines.extend([
        "",
        "## Research limitations",
        "",
        "- This is an input integrity audit.",
        "- Historical signal correctness is not re-proven.",
        "- Signal candle-close availability is not proven.",
        "- Independent OOS is not proven.",
        "- Event-level outcomes are not evaluated.",
        "- No simultaneous market-matched controls.",
        "- No fees, slippage or execution simulation.",
        "- Training and live trading remain blocked.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()

    if not root.is_dir():
        raise FileNotFoundError(root)

    result = run_audit(root)

    output = (
        root / "data" / "reports" / REPORT_DIR
    )
    output.mkdir(parents=True, exist_ok=True)

    result_path = output / "integrity_result.json"
    report_path = output / "integrity_report.md"
    manifest_path = output / "integrity_manifest.json"

    atomic_json(result_path, result)
    atomic_text(
        report_path,
        build_report(result),
    )

    atomic_json(
        manifest_path,
        {
            "version": VERSION,
            "files": {
                result_path.name: file_hash(result_path),
                report_path.name: file_hash(report_path),
            },
        },
    )

    print(
        json.dumps(
            {
                "execution": result["execution"],
                "classification": result["classification"],
                "totals": result["totals"],
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
