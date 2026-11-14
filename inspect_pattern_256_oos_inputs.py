
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 OOS Input Inspection
Clean V001

Read-only inspection of OHLCV and signal CSV inputs.

Does NOT:
- calculate OOS performance
- change original data
- infer unknown signal semantics
- approve training or trading
"""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_oos_input_inspection"

POLICY_DIR = Path(
    "data/reports/pattern_256_oos_policy_freeze"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_oos_input_inspection"
)

TIMEFRAMES = ("h1", "h4", "d1")

TIME_COLUMNS = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "date",
    "time",
    "signal_time",
    "signal_timestamp",
    "candle_time",
    "candle_timestamp",
    "timestamp_utc",
)

SIGNAL_HINTS = (
    "signal",
    "detected",
    "trigger",
    "pattern",
    "entry",
    "candidate",
    "flag",
    "score",
    "is_256",
)

MAX_EXAMPLES = 3


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def write_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def check_policy(root):
    folder = root / POLICY_DIR

    manifest_path = (
        folder / "oos_policy_freeze_manifest.json"
    )

    if not manifest_path.is_file():
        raise RuntimeError(
            "Frozen policy manifest missing"
        )

    manifest = load_json(manifest_path)

    if manifest.get("audit") != (
        "pattern_256_oos_policy_freeze"
    ):
        raise RuntimeError("Wrong policy manifest")

    expected = {
        "oos_policy.json",
        "oos_policy_freeze_result.json",
        "oos_policy_freeze_report.md",
    }

    outputs = manifest.get("outputs", {})

    if set(outputs) != expected:
        raise RuntimeError(
            "Unexpected policy manifest outputs"
        )

    for name, expected_hash in outputs.items():
        path = folder / name

        if not path.is_file():
            raise RuntimeError(
                f"Policy file missing: {name}"
            )

        if sha256(path).lower() != str(
            expected_hash
        ).lower():
            raise RuntimeError(
                f"Policy SHA256 mismatch: {name}"
            )

    policy = load_json(
        folder / "oos_policy.json"
    )

    result = load_json(
        folder / "oos_policy_freeze_result.json"
    )

    if policy.get("policy_id") != (
        "pattern_256_oos_conservative_v001"
    ):
        raise RuntimeError("Wrong policy ID")

    if result.get("execution") != "PASS":
        raise RuntimeError(
            "Policy freeze did not pass"
        )

    if result.get("research_decision") != "HOLD":
        raise RuntimeError(
            "Unexpected policy research decision"
        )

    return {
        "policy_id": policy["policy_id"],
        "policy_manifest_sha256": sha256(
            manifest_path
        ),
        "policy_file_sha256": sha256(
            folder / "oos_policy.json"
        ),
    }


def normalize_time(value):
    value = str(value or "").strip()

    if not value:
        return None

    try:
        if value.isdigit():
            number = int(value)

            if number > 10**12:
                number /= 1000

            if number > 10**9:
                return datetime.fromtimestamp(
                    number,
                    tz=timezone.utc,
                ).isoformat()

        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            return parsed.isoformat()

        return parsed.astimezone(
            timezone.utc
        ).isoformat()

    except (ValueError, OverflowError, OSError):
        return None


def read_csv_info(path):
    result = {
        "file": path.name,
        "exists": True,
        "size_bytes": path.stat().st_size,
        "columns": [],
        "row_count": 0,
        "time_column": None,
        "first_time": None,
        "last_time": None,
        "min_time": None,
        "max_time": None,
        "invalid_time_rows": 0,
        "empty_rows": 0,
        "sample_rows": [],
        "signal_column_candidates": [],
        "error": None,
    }

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)

            if not reader.fieldnames:
                raise ValueError(
                    "CSV header missing"
                )

            columns = list(reader.fieldnames)

            if any(
                column is None or not column.strip()
                for column in columns
            ):
                raise ValueError(
                    "Empty CSV column name"
                )

            if len(columns) != len(set(columns)):
                raise ValueError(
                    "Duplicate CSV column names"
                )

            result["columns"] = columns

            lower_columns = {
                column.lower(): column
                for column in columns
            }

            for candidate in TIME_COLUMNS:
                if candidate in lower_columns:
                    result["time_column"] = (
                        lower_columns[candidate]
                    )
                    break

            result["signal_column_candidates"] = [
                column
                for column in columns
                if any(
                    hint in column.lower()
                    for hint in SIGNAL_HINTS
                )
            ]

            for row in reader:
                result["row_count"] += 1

                if None in row:
                    raise ValueError(
                        "Malformed CSV row: extra fields"
                    )

                if all(
                    not str(value or "").strip()
                    for value in row.values()
                ):
                    result["empty_rows"] += 1

                if len(result["sample_rows"]) < (
                    MAX_EXAMPLES
                ):
                    result["sample_rows"].append(
                        dict(row)
                    )

                column = result["time_column"]

                if column:
                    normalized = normalize_time(
                        row.get(column)
                    )

                    if normalized is None:
                        result[
                            "invalid_time_rows"
                        ] += 1
                    else:
                        if result["first_time"] is None:
                            result["first_time"] = (
                                normalized
                            )

                        result["last_time"] = normalized

                        if (
                            result["min_time"] is None
                            or normalized
                            < result["min_time"]
                        ):
                            result["min_time"] = (
                                normalized
                            )

                        if (
                            result["max_time"] is None
                            or normalized
                            > result["max_time"]
                        ):
                            result["max_time"] = (
                                normalized
                            )

    except Exception as exc:
        result["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    return result


def inspect_group(root, category, timeframe):
    if category == "ohlcv":
        folder = (
            root / "data" / "ohlcv" / timeframe
        )
    else:
        folder = (
            root / "data" / "signals"
            / "256" / timeframe
        )

    result = {
        "category": category,
        "timeframe": timeframe,
        "folder": str(folder),
        "folder_exists": folder.is_dir(),
        "file_count": 0,
        "total_rows": 0,
        "schema_counts": {},
        "time_column_counts": {},
        "files": [],
        "errors": [],
    }

    if not folder.is_dir():
        result["errors"].append(
            "DIRECTORY_MISSING"
        )
        return result

    files = sorted(
        folder.glob("KRW-*.csv")
    )

    schema_counter = Counter()
    time_counter = Counter()

    for index, path in enumerate(files, 1):
        info = read_csv_info(path)

        result["files"].append(info)
        result["total_rows"] += (
            info["row_count"]
        )

        schema_counter[
            "|".join(info["columns"])
        ] += 1

        time_counter[
            str(info["time_column"])
        ] += 1

        if info["error"]:
            result["errors"].append(
                f"{path.name}: {info['error']}"
            )

        if index % 50 == 0:
            print(
                f"[PROGRESS] {category}/{timeframe}"
                f" {index}/{len(files)}",
                flush=True,
            )

    result["file_count"] = len(files)

    result["schema_counts"] = dict(
        schema_counter
    )

    result["time_column_counts"] = dict(
        time_counter
    )

    if not files:
        result["errors"].append(
            "NO_MARKET_CSV_FILES"
        )

    return result


def write_inventory_csv(path, groups):
    columns = [
        "category",
        "timeframe",
        "market_file",
        "rows",
        "size_bytes",
        "time_column",
        "min_time",
        "max_time",
        "invalid_time_rows",
        "columns",
        "signal_column_candidates",
        "error",
    ]

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
        )

        writer.writeheader()

        for group in groups:
            for item in group["files"]:
                writer.writerow({
                    "category": group["category"],
                    "timeframe": group["timeframe"],
                    "market_file": item["file"],
                    "rows": item["row_count"],
                    "size_bytes": item["size_bytes"],
                    "time_column": (
                        item["time_column"] or ""
                    ),
                    "min_time": (
                        item["min_time"] or ""
                    ),
                    "max_time": (
                        item["max_time"] or ""
                    ),
                    "invalid_time_rows": (
                        item["invalid_time_rows"]
                    ),
                    "columns": "|".join(
                        item["columns"]
                    ),
                    "signal_column_candidates": (
                        "|".join(
                            item[
                                "signal_column_candidates"
                            ]
                        )
                    ),
                    "error": item["error"] or "",
                })


def build_summary(groups):
    result = {}

    for timeframe in TIMEFRAMES:
        ohlcv = next(
            group for group in groups
            if group["category"] == "ohlcv"
            and group["timeframe"] == timeframe
        )

        signals = next(
            group for group in groups
            if group["category"] == "signals"
            and group["timeframe"] == timeframe
        )

        ohlcv_files = {
            item["file"]
            for item in ohlcv["files"]
        }

        signal_files = {
            item["file"]
            for item in signals["files"]
        }

        result[timeframe] = {
            "ohlcv_files": len(ohlcv_files),
            "signal_files": len(signal_files),
            "matching_files": len(
                ohlcv_files & signal_files
            ),
            "ohlcv_without_signals": len(
                ohlcv_files - signal_files
            ),
            "signals_without_ohlcv": len(
                signal_files - ohlcv_files
            ),
            "ohlcv_rows": ohlcv["total_rows"],
            "signal_rows": signals["total_rows"],
            "ohlcv_schema_variants": len(
                ohlcv["schema_counts"]
            ),
            "signal_schema_variants": len(
                signals["schema_counts"]
            ),
            "ohlcv_errors": len(
                ohlcv["errors"]
            ),
            "signal_errors": len(
                signals["errors"]
            ),
        }

    return result


def build_report(result):
    lines = [
        "# Pattern 256 OOS Input Inspection",
        "",
        f"- Version: {VERSION}",
        f"- Execution: {result['execution']}",
        f"- Classification: {result['classification']}",
        "- Independent OOS: NOT PROVEN",
        "- Research decision: HOLD",
        "",
        "## File coverage",
        "",
        "| Timeframe | OHLCV | Signals | Matching | OHLCV rows | Signal rows |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for timeframe, item in result[
        "summary"
    ].items():
        lines.append(
            f"| {timeframe} "
            f"| {item['ohlcv_files']} "
            f"| {item['signal_files']} "
            f"| {item['matching_files']} "
            f"| {item['ohlcv_rows']} "
            f"| {item['signal_rows']} |"
        )

    lines.extend([
        "",
        "## Input schema",
        "",
    ])

    for group in result["groups"]:
        lines.append(
            f"### {group['category']}/{group['timeframe']}"
        )

        lines.append(
            f"- Files: {group['file_count']}"
        )

        lines.append(
            f"- Rows: {group['total_rows']}"
        )

        lines.append(
            "- Time columns: "
            + json.dumps(
                group["time_column_counts"],
                ensure_ascii=False,
            )
        )

        lines.append(
            "- Schema variants: "
            + str(len(group["schema_counts"]))
        )

        for schema, count in group[
            "schema_counts"
        ].items():
            lines.append(
                f"- {count} files: `{schema}`"
            )

        for error in group["errors"][:20]:
            lines.append(
                f"- ERROR: {error}"
            )

        lines.append("")

    lines.extend([
        "## Interpretation",
        "",
        "This inspection discovers actual source schemas.",
        "It does not assume which signal column is valid.",
        "It does not perform a backtest.",
        "It does not establish causal signal availability.",
        "Training and live trading remain unapproved.",
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
        raise RuntimeError(
            f"Root directory missing: {root}"
        )

    evidence = check_policy(root)

    groups = []

    for timeframe in TIMEFRAMES:
        for category in ("ohlcv", "signals"):
            print(
                f"[START] {category}/{timeframe}",
                flush=True,
            )

            group = inspect_group(
                root,
                category,
                timeframe,
            )

            groups.append(group)

            print(
                f"[DONE] {category}/{timeframe}: "
                f"{group['file_count']} files, "
                f"{group['total_rows']} rows",
                flush=True,
            )

    summary = build_summary(groups)

    has_errors = any(
        group["errors"]
        for group in groups
    )

    has_missing_matches = any(
        item["matching_files"] == 0
        for item in summary.values()
    )

    if has_errors:
        classification = (
            "INPUT_ERRORS_REQUIRE_REVIEW"
        )
    elif has_missing_matches:
        classification = (
            "SIGNAL_MATCHING_REQUIRES_REVIEW"
        )
    else:
        classification = (
            "SCHEMA_DISCOVERY_COMPLETE"
        )

    output = root / OUTPUT_DIR

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": classification,
        "policy_evidence": evidence,
        "summary": summary,
        "groups": groups,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "next_gate": (
            "IMPLEMENT_SCHEMA_AWARE_CHRONOLOGICAL_OOS"
        ),
    }

    result_path = (
        output / "oos_input_inspection_result.json"
    )

    inventory_path = (
        output / "oos_input_inventory.csv"
    )

    report_path = (
        output / "oos_input_inspection_report.md"
    )

    manifest_path = (
        output / "oos_input_inspection_manifest.json"
    )

    write_json(result_path, result)

    write_inventory_csv(
        inventory_path,
        groups,
    )

    report_path.write_text(
        build_report(result),
        encoding="utf-8",
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            path.name: sha256(path)
            for path in (
                result_path,
                inventory_path,
                report_path,
            )
        },
    }

    write_json(manifest_path, manifest)

    print("=" * 60)
    print("Pattern 256 OOS Input Inspection")
    print("Execution: PASS")
    print("Classification:", classification)

    for timeframe, item in summary.items():
        print(
            timeframe,
            "OHLCV:", item["ohlcv_files"],
            "Signals:", item["signal_files"],
            "Matching:", item["matching_files"],
        )

    print("Independent OOS: NOT PROVEN")
    print("Research decision: HOLD")
    print("Output:", output)
    print("=" * 60)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"[ERROR] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
