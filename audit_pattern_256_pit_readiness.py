
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Point-in-Time Data Readiness Audit
Clean V005

Read-only audit of D1 source, feature and signal CSV files.

This is a schema and data-readiness audit, NOT proof of:
- Point-in-time correctness
- Absence of look-ahead leakage
- Independent out-of-sample performance
- Statistical significance
- Trading profitability

Source data is never modified.
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

VERSION = "Clean V005"

INPUT_FOLDERS = {
    "ohlcv": "data/ohlcv/d1",
    "features": "data/features/d1",
    "signals": "data/signals/256/d1",
}

OUTPUT_FOLDER = (
    "data/reports/pattern_256_pit_readiness"
)

SOURCE_REPORT = (
    "data/reports/"
    "pattern_256_candidate_sensitivity/result.json"
)

SOURCE_MANIFEST = (
    "data/reports/"
    "pattern_256_candidate_sensitivity/manifest.json"
)

TIME_COLUMNS = (
    "timestamp",
    "candle_date_time_utc",
    "datetime",
    "date",
    "time",
    "open_time",
    "candle_date_time_kst",
)

SIGNAL_COLUMNS = (
    "signal_256",
    "pattern_256",
    "is_256",
    "signal",
    "detected",
)

REQUIRED_OHLCV = (
    "open",
    "high",
    "low",
    "close",
    "volume",
)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r", encoding="utf-8-sig"
    ) as stream:
        return json.load(stream)


def write_json(path, data):
    with path.open(
        "w", encoding="utf-8", newline="\n"
    ) as stream:
        json.dump(
            data,
            stream,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        stream.write("\n")


def write_text(path, content):
    with path.open(
        "w", encoding="utf-8", newline="\n"
    ) as stream:
        stream.write(content)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def normalize_market(path):
    return path.stem.upper()


def detect_column(columns, candidates):
    lookup = {}

    for column in columns:
        key = column.strip().lower()
        lookup.setdefault(key, []).append(column)

    matches = []

    for candidate in candidates:
        matches.extend(lookup.get(candidate, []))

    # Ambiguous time columns are not silently accepted.
    if len(matches) != 1:
        return None, matches

    return matches[0], matches


def parse_timestamp(value):
    value = value.strip()

    if not value:
        return None

    try:
        number = float(value)
        if math.isfinite(number):
            if number > 1e14:
                number /= 1_000_000
            elif number > 1e11:
                number /= 1_000

            if 0 < number < 4102444800:
                return datetime.fromtimestamp(
                    number, timezone.utc
                ).isoformat()
    except (ValueError, OverflowError, OSError):
        pass

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            # Naive values retain their wall-clock value.
            # They are NOT asserted to be UTC.
            return parsed.isoformat()

        return parsed.astimezone(
            timezone.utc
        ).isoformat()

    except ValueError:
        return None


def scan_csv(path, category):
    result = {
        "file": str(path),
        "market": normalize_market(path),
        "category": category,
        "size_bytes": path.stat().st_size,
        "sha256": sha256(path),
        "columns": [],
        "row_count": 0,
        "timestamp_column": None,
        "timestamp_candidates": [],
        "timestamp_first": None,
        "timestamp_last": None,
        "timestamp_min": None,
        "timestamp_max": None,
        "timestamp_parse_errors": 0,
        "timestamp_empty": 0,
        "timestamp_duplicates": 0,
        "timestamp_out_of_order": 0,
        "signal_column": None,
        "signal_candidates": [],
        "issues": [],
    }

    seen = set()
    previous = None
    minimum = None
    maximum = None

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)

            columns = reader.fieldnames

            if not columns:
                result["issues"].append(
                    "MISSING_HEADER"
                )
                return result

            result["columns"] = columns

            if len(columns) != len(set(columns)):
                result["issues"].append(
                    "DUPLICATE_HEADER"
                )

            time_col, time_candidates = detect_column(
                columns, TIME_COLUMNS
            )

            result["timestamp_column"] = time_col
            result["timestamp_candidates"] = (
                time_candidates
            )

            if time_col is None:
                result["issues"].append(
                    "TIMESTAMP_COLUMN_UNRESOLVED"
                )

            if category == "ohlcv":
                lower = {
                    c.strip().lower()
                    for c in columns
                }

                missing = [
                    c for c in REQUIRED_OHLCV
                    if c not in lower
                ]

                if missing:
                    result["issues"].append(
                        "OHLCV_COLUMNS_UNRESOLVED:"
                        + ",".join(missing)
                    )

            if category == "signals":
                signal_col, signal_candidates = (
                    detect_column(
                        columns, SIGNAL_COLUMNS
                    )
                )

                result["signal_column"] = signal_col
                result["signal_candidates"] = (
                    signal_candidates
                )

                if signal_col is None:
                    result["issues"].append(
                        "SIGNAL_COLUMN_UNRESOLVED"
                    )

            for row in reader:
                result["row_count"] += 1

                if None in row:
                    if "MALFORMED_ROW" not in result["issues"]:
                        result["issues"].append(
                            "MALFORMED_ROW"
                        )

                if time_col is None:
                    continue

                raw = row.get(time_col)

                if raw is None or not raw.strip():
                    result["timestamp_empty"] += 1
                    continue

                stamp = parse_timestamp(raw)

                if stamp is None:
                    result["timestamp_parse_errors"] += 1
                    continue

                if result["timestamp_first"] is None:
                    result["timestamp_first"] = stamp

                result["timestamp_last"] = stamp

                if stamp in seen:
                    result["timestamp_duplicates"] += 1

                seen.add(stamp)

                if previous is not None and stamp < previous:
                    result["timestamp_out_of_order"] += 1

                previous = stamp

                if minimum is None or stamp < minimum:
                    minimum = stamp

                if maximum is None or stamp > maximum:
                    maximum = stamp

    except (
        OSError,
        UnicodeError,
        csv.Error,
        ValueError,
    ) as exc:
        result["issues"].append(
            "CSV_READ_ERROR:"
            + type(exc).__name__
            + ":"
            + str(exc)
        )

    result["timestamp_min"] = minimum
    result["timestamp_max"] = maximum

    if result["row_count"] == 0:
        result["issues"].append("EMPTY_CSV")

    if result["timestamp_empty"]:
        result["issues"].append(
            "EMPTY_TIMESTAMPS"
        )

    if result["timestamp_parse_errors"]:
        result["issues"].append(
            "UNPARSEABLE_TIMESTAMPS"
        )

    if result["timestamp_duplicates"]:
        result["issues"].append(
            "DUPLICATE_TIMESTAMPS"
        )

    if result["timestamp_out_of_order"]:
        result["issues"].append(
            "OUT_OF_ORDER_TIMESTAMPS"
        )

    return result


def verify_previous_stage(root):
    source_path = root / SOURCE_REPORT
    manifest_path = root / SOURCE_MANIFEST

    require(
        source_path.is_file(),
        "Clean V004 result.json missing",
    )
    require(
        manifest_path.is_file(),
        "Clean V004 manifest.json missing",
    )

    source = read_json(source_path)
    manifest = read_json(manifest_path)

    require(
        source.get("version") == "Clean V004",
        "Unexpected source version",
    )
    require(
        source.get("execution") == "PASS",
        "Source execution not PASS",
    )
    require(
        source.get("research_decision") == "HOLD",
        "Source decision must be HOLD",
    )
    require(
        source.get("original_candidate")
        == "d1|DOWN|LOW",
        "Unexpected candidate",
    )

    expected = manifest.get(
        "sha256", {}
    ).get("result.json")

    require(
        isinstance(expected, str)
        and sha256(source_path) == expected.lower(),
        "Clean V004 SHA256 mismatch",
    )

    return {
        "source_version": "Clean V004",
        "source_result_sha256": sha256(
            source_path
        ),
    }


def discover_files(folder):
    return sorted(
        (
            path for path in folder.glob("*.csv")
            if path.is_file()
        ),
        key=lambda path: path.name.lower(),
    )


def make_report(result):
    lines = [
        "# Pattern 256 PIT Data Readiness Audit",
        "",
        f"Version: {VERSION}",
        "",
        f"Execution: {result['execution']}",
        "",
        f"Data readiness: {result['data_readiness']}",
        "",
        "Research decision: HOLD",
        "",
        "## Inventory",
        "",
        "| Category | Files | Rows | Issues |",
        "|---|---:|---:|---:|",
    ]

    for name, info in result["categories"].items():
        lines.append(
            f"| {name} | {info['file_count']} "
            f"| {info['row_count']} "
            f"| {info['issue_count']} |"
        )

    lines.extend([
        "",
        "## Market coverage",
        "",
        f"OHLCV markets: {result['market_counts']['ohlcv']}",
        "",
        f"Feature markets: {result['market_counts']['features']}",
        "",
        f"Signal markets: {result['market_counts']['signals']}",
        "",
        f"Common markets: {result['common_market_count']}",
        "",
        "## Issue counts",
        "",
    ])

    for issue, count in sorted(
        result["issue_counts"].items()
    ):
        lines.append(
            f"- {issue}: {count}"
        )

    lines.extend([
        "",
        "## Important limitations",
        "",
        "- Schema checks do not prove causal timing.",
        "- No feature formulas are replayed here.",
        "- No future-information leakage proof.",
        "- No independent OOS performance proof.",
        "- No statistical significance proof.",
        "- No multiple-testing correction proof.",
        "- No realized profitability proof.",
        "- Training and trading remain unapproved.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
        help="Persistent project root",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()

    require(
        root.is_dir(),
        "Project root does not exist",
    )

    upstream = verify_previous_stage(root)

    files_by_category = {}
    categories = {}
    market_sets = {}
    all_results = []
    issue_counts = Counter()

    for category, relative in INPUT_FOLDERS.items():
        folder = root / relative

        require(
            folder.is_dir(),
            "Required directory missing: " + str(folder),
        )

        paths = discover_files(folder)

        require(
            len(paths) > 0,
            "No CSV files in: " + str(folder),
        )

        files_by_category[category] = paths

        market_names = [
            normalize_market(path)
            for path in paths
        ]

        markets = set(market_names)
        market_sets[category] = markets

        if len(markets) != len(market_names):
            raise ValueError(
                "Duplicate market filenames: "
                + category
            )

        total_rows = 0
        total_issues = 0

        for path in paths:
            entry = scan_csv(path, category)
            all_results.append(entry)

            total_rows += entry["row_count"]
            total_issues += len(entry["issues"])

            for issue in entry["issues"]:
                issue_counts[
                    issue.split(":")[0]
                ] += 1

        categories[category] = {
            "folder": str(folder),
            "file_count": len(paths),
            "row_count": total_rows,
            "issue_count": total_issues,
        }

    common = set.intersection(
        *market_sets.values()
    )

    union = set.union(
        *market_sets.values()
    )

    missing_by_category = {
        category: sorted(union - markets)
        for category, markets in market_sets.items()
    }

    for category, missing in (
        missing_by_category.items()
    ):
        if missing:
            issue_counts[
                "MISSING_MARKET_" + category.upper()
            ] += len(missing)

    readiness = (
        "READY_FOR_DEEPER_AUDIT"
        if not issue_counts
        else "BLOCKED"
    )

    # READY_FOR_DEEPER_AUDIT is NOT PIT verification.
    result = {
        "version": VERSION,
        "created_utc": now_utc(),
        "execution": "PASS",
        "source": upstream,
        "candidate": "d1|DOWN|LOW",
        "categories": categories,
        "market_counts": {
            key: len(value)
            for key, value in market_sets.items()
        },
        "common_market_count": len(common),
        "common_markets": sorted(common),
        "missing_by_category": missing_by_category,
        "issue_counts": dict(issue_counts),
        "data_readiness": readiness,
        "files": all_results,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "formal_significance_proven": False,
        "multiple_testing_adjusted": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    output = root / OUTPUT_FOLDER

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = output / "result.json"
    report_path = output / "report.md"
    manifest_path = output / "manifest.json"

    write_json(result_path, result)
    write_text(
        report_path,
        make_report(result),
    )

    manifest = {
        "version": VERSION,
        "created_utc": now_utc(),
        "source_result_sha256": upstream[
            "source_result_sha256"
        ],
        "sha256": {
            "result.json": sha256(result_path),
            "report.md": sha256(report_path),
        },
    }

    write_json(manifest_path, manifest)

    print("[PASS] PIT readiness inventory")
    print("Version:", VERSION)
    print("Data readiness:", readiness)
    print("Common markets:", len(common))
    print("Issue counts:", dict(issue_counts))
    print("Research decision: HOLD")
    print("Output:", output)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
