#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Feature JOIN Integrity Audit
Clean V001

PURPOSE
-------
Read-only integrity audit for TRAIN feature JOIN outputs.

Checks:
1. Source and output discovery
2. CSV structural integrity
3. SHA256 inventory
4. Row counts
5. Duplicate event keys
6. Timestamp consistency
7. Future-information leakage indicators
8. Source-to-JOIN key reconciliation
9. Missing and invalid feature values
10. Market/timeframe coverage

IMPORTANT
---------
- Does not modify source data.
- Does not perform trading.
- Does not approve live trading.
- Cannot prove statistical independence or profitability.
- Missing verification evidence results in HOLD.
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

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"

SOURCE_DIR = (
    "data/reports/pattern_256_train_only"
)

JOIN_DIR = (
    "data/reports/pattern_256_train_feature_join"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_feature_join_integrity"
)

RESULT_FILE = "feature_join_integrity_result.json"
MANIFEST_FILE = "feature_join_integrity_manifest.json"
COVERAGE_FILE = "feature_join_integrity_coverage.csv"
ISSUES_FILE = "feature_join_integrity_issues.csv"

SOURCE_CANDIDATES = (
    "train_only_events.csv",
    "train_events.csv",
    "train_only.csv",
)

JOIN_RESULT_CANDIDATES = (
    "train_feature_join_result.json",
    "feature_join_result.json",
)

JOIN_MANIFEST_CANDIDATES = (
    "train_feature_join_manifest.json",
    "feature_join_manifest.json",
)

KEY_CANDIDATES = (
    ("event_id",),
    ("market", "timeframe", "signal_timestamp", "horizon"),
    ("market", "timeframe", "signal_time", "horizon"),
    ("market", "timeframe", "timestamp", "horizon"),
    ("market", "timeframe", "signal_timestamp"),
    ("market", "timeframe", "signal_time"),
)

TIME_COLUMNS = (
    "signal_timestamp",
    "signal_time",
    "event_timestamp",
    "entry_timestamp",
    "timestamp",
)

FEATURE_TIME_COLUMNS = (
    "feature_timestamp",
    "feature_time",
    "feature_candle_timestamp",
    "feature_candle_time",
)

NULL_VALUES = {
    "",
    "null",
    "none",
    "nan",
    "na",
    "n/a",
}

EXPECTED_SOURCE_ROWS = 4164988
EXPECTED_JOINED_ROWS = 3336652


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def write_json_atomic(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(
                data,
                stream,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_csv_atomic(
    path: Path,
    columns: list[str],
    rows: list[dict],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:

            writer = csv.DictWriter(
                stream,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)

            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8-sig") as stream:
        result = json.load(stream)

    if not isinstance(result, dict):
        raise ValueError(
            f"Expected JSON object: {path}"
        )

    return result


def find_first(
    directory: Path,
    names: tuple[str, ...],
) -> Path | None:

    for name in names:
        path = directory / name

        if path.is_file():
            return path

    return None


def parse_timestamp(value: str):
    value = str(value).strip()

    if not value:
        return None

    try:
        if re.fullmatch(r"\d{10}", value):
            return datetime.fromtimestamp(
                int(value),
                tz=timezone.utc,
            )

        if re.fullmatch(r"\d{13}", value):
            return datetime.fromtimestamp(
                int(value) / 1000,
                tz=timezone.utc,
            )

        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        result = datetime.fromisoformat(value)

        if result.tzinfo is None:
            return None

        return result.astimezone(timezone.utc)

    except (ValueError, OverflowError, OSError):
        return None


def normalize_key_value(
    column: str,
    value: str,
) -> str:

    value = str(value).strip()

    if column in TIME_COLUMNS:
        parsed = parse_timestamp(value)

        if parsed is not None:
            return parsed.isoformat()

    return value


def make_key(
    row: dict,
    columns: tuple[str, ...],
) -> tuple[str, ...]:

    return tuple(
        normalize_key_value(
            column,
            row.get(column, ""),
        )
        for column in columns
    )


def detect_key(
    columns: set[str],
) -> tuple[str, ...] | None:

    for candidate in KEY_CANDIDATES:
        if all(
            column in columns
            for column in candidate
        ):
            return candidate

    return None


def record_issue(
    issues: list[dict],
    severity: str,
    code: str,
    file: str,
    detail: str,
) -> None:

    issues.append({
        "severity": severity,
        "code": code,
        "file": file,
        "detail": detail,
    })


def inspect_csv(
    path: Path,
    issues: list[dict],
    *,
    collect_keys: bool = False,
) -> dict:

    total = 0
    invalid_rows = 0
    duplicate_rows = 0
    invalid_timestamps = 0
    future_feature_rows = 0

    key_counter = Counter()

    columns = []
    key_columns = None

    markets = Counter()
    timeframes = Counter()

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:

        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                f"Missing CSV header: {path}"
            )

        columns = list(reader.fieldnames)

        if len(columns) != len(set(columns)):
            record_issue(
                issues,
                "ERROR",
                "DUPLICATE_COLUMNS",
                str(path),
                "Duplicate CSV column names",
            )

        key_columns = detect_key(set(columns))

        for row in reader:
            total += 1

            if None in row:
                invalid_rows += 1
                continue

            if any(
                value is None
                for value in row.values()
            ):
                invalid_rows += 1
                continue

            market = row.get("market", "")
            timeframe = row.get("timeframe", "")

            if market:
                markets[market] += 1

            if timeframe:
                timeframes[timeframe] += 1

            signal_time = None

            for column in TIME_COLUMNS:
                if column in row and row[column]:
                    signal_time = parse_timestamp(
                        row[column]
                    )

                    if signal_time is None:
                        invalid_timestamps += 1

                    break

            feature_time = None

            for column in FEATURE_TIME_COLUMNS:
                if column in row and row[column]:
                    feature_time = parse_timestamp(
                        row[column]
                    )

                    if feature_time is None:
                        invalid_timestamps += 1

                    break

            if (
                signal_time is not None
                and feature_time is not None
                and feature_time > signal_time
            ):
                future_feature_rows += 1

            if collect_keys and key_columns:
                key = make_key(
                    row,
                    key_columns,
                )

                if any(
                    value == ""
                    for value in key
                ):
                    invalid_rows += 1
                    continue

                key_counter[key] += 1

                if key_counter[key] > 1:
                    duplicate_rows += 1

            if total % 500000 == 0:
                print(
                    f"[SCAN] {path.name}: "
                    f"{total:,}",
                    flush=True,
                )

    if invalid_rows:
        record_issue(
            issues,
            "ERROR",
            "INVALID_CSV_ROWS",
            str(path),
            str(invalid_rows),
        )

    if invalid_timestamps:
        record_issue(
            issues,
            "ERROR",
            "INVALID_TIMESTAMPS",
            str(path),
            str(invalid_timestamps),
        )

    if future_feature_rows:
        record_issue(
            issues,
            "ERROR",
            "FUTURE_FEATURE_TIMESTAMP",
            str(path),
            str(future_feature_rows),
        )

    return {
        "path": str(path),
        "rows": total,
        "sha256": sha256_file(path),
        "columns": columns,
        "key_columns": list(key_columns)
        if key_columns else None,
        "invalid_rows": invalid_rows,
        "duplicate_rows": duplicate_rows,
        "invalid_timestamps": invalid_timestamps,
        "future_feature_rows": future_feature_rows,
        "markets": dict(markets),
        "timeframes": dict(timeframes),
        "key_counter": key_counter,
    }


def validate_manifest_hashes(
    manifest: dict,
    base_dir: Path,
    issues: list[dict],
) -> dict:

    checked = 0
    matched = 0
    missing = 0
    mismatched = 0

    def walk(value):
        nonlocal checked
        nonlocal matched
        nonlocal missing
        nonlocal mismatched

        if isinstance(value, dict):

            for filename, metadata in value.items():

                if (
                    isinstance(metadata, dict)
                    and "sha256" in metadata
                ):
                    expected = str(
                        metadata["sha256"]
                    ).lower()

                    candidate = base_dir / filename

                    if not candidate.is_file():
                        continue

                    checked += 1

                    actual = sha256_file(
                        candidate
                    )

                    if actual == expected:
                        matched += 1

                    else:
                        mismatched += 1

                        record_issue(
                            issues,
                            "ERROR",
                            "SHA256_MISMATCH",
                            str(candidate),
                            "Manifest hash mismatch",
                        )

                walk(metadata)

        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(manifest)

    return {
        "checked": checked,
        "matched": matched,
        "missing": missing,
        "mismatched": mismatched,
    }


def main() -> int:

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--source",
        default=None,
    )

    parser.add_argument(
        "--join-dir",
        default=None,
    )

    parser.add_argument(
        "--output-dir",
        default=None,
    )

    parser.add_argument(
        "--expected-source-rows",
        type=int,
        default=EXPECTED_SOURCE_ROWS,
    )

    parser.add_argument(
        "--expected-joined-rows",
        type=int,
        default=EXPECTED_JOINED_ROWS,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source_dir = root / SOURCE_DIR

    join_dir = (
        Path(args.join_dir).resolve()
        if args.join_dir
        else root / JOIN_DIR
    )

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else root / OUTPUT_DIR
    )

    source_path = (
        Path(args.source).resolve()
        if args.source
        else find_first(
            source_dir,
            SOURCE_CANDIDATES,
        )
    )

    if source_path is None:
        raise FileNotFoundError(
            "TRAIN source CSV not found. "
            "Specify --source."
        )

    if not join_dir.is_dir():
        raise FileNotFoundError(
            f"JOIN directory missing: {join_dir}"
        )

    if (
        output_dir == join_dir
        or output_dir == source_dir
        or output_dir == root
        or output_dir in join_dir.parents
        or output_dir in source_path.parents
    ):
        raise ValueError(
            "Unsafe output directory"
        )

    issues = []

    print(
        f"[START] {VERSION}",
        flush=True,
    )

    print(
        f"[SOURCE] {source_path}",
        flush=True,
    )

    print(
        f"[JOIN] {join_dir}",
        flush=True,
    )

    join_result_path = find_first(
        join_dir,
        JOIN_RESULT_CANDIDATES,
    )

    join_manifest_path = find_first(
        join_dir,
        JOIN_MANIFEST_CANDIDATES,
    )

    join_result = {}

    if join_result_path:
        join_result = read_json(
            join_result_path
        )

    else:
        record_issue(
            issues,
            "WARNING",
            "MISSING_JOIN_RESULT",
            str(join_dir),
            "JOIN result JSON unavailable",
        )

    join_manifest = {}

    if join_manifest_path:
        join_manifest = read_json(
            join_manifest_path
        )

    else:
        record_issue(
            issues,
            "WARNING",
            "MISSING_JOIN_MANIFEST",
            str(join_dir),
            "JOIN manifest JSON unavailable",
        )

    shard_dir = join_dir / "shards"

    if not shard_dir.is_dir():
        raise FileNotFoundError(
            f"JOIN shards missing: {shard_dir}"
        )

    shard_files = sorted(
        shard_dir.rglob("*.csv")
    )

    if not shard_files:
        raise ValueError(
            "No JOIN shard CSV files"
        )

    print(
        f"[SHARDS] {len(shard_files)}",
        flush=True,
    )

    source_info = inspect_csv(
        source_path,
        issues,
        collect_keys=True,
    )

    source_rows = source_info["rows"]

    if source_rows != args.expected_source_rows:
        record_issue(
            issues,
            "ERROR",
            "SOURCE_ROW_COUNT",
            str(source_path),
            (
                f"Expected "
                f"{args.expected_source_rows}, "
                f"actual {source_rows}"
            ),
        )

    source_keys = source_info[
        "key_counter"
    ]

    source_key_columns = (
        tuple(source_info["key_columns"])
        if source_info["key_columns"]
        else None
    )

    joined_rows = 0
    duplicate_join_keys = 0

    joined_key_counter = Counter()

    coverage = []

    shard_hashes = {}

    schema_variants = Counter()

    for index, shard in enumerate(
        shard_files,
        start=1,
    ):

        info = inspect_csv(
            shard,
            issues,
            collect_keys=True,
        )

        joined_rows += info["rows"]

        shard_hashes[
            str(shard.relative_to(join_dir))
        ] = info["sha256"]

        schema_variants[
            tuple(info["columns"])
        ] += 1

        shard_keys = info["key_counter"]

        if (
            source_key_columns
            and info["key_columns"]
            and tuple(info["key_columns"])
            == source_key_columns
        ):
            joined_key_counter.update(
                shard_keys
            )

        else:
            record_issue(
                issues,
                "WARNING",
                "KEY_SCHEMA_MISMATCH",
                str(shard),
                (
                    "Source and shard keys "
                    "cannot be safely reconciled"
                ),
            )

        duplicate_join_keys += (
            info["duplicate_rows"]
        )

        relative = shard.relative_to(
            join_dir
        )

        coverage.append({
            "file": str(relative),
            "rows": info["rows"],
            "markets": len(
                info["markets"]
            ),
            "timeframes": len(
                info["timeframes"]
            ),
            "invalid_rows": info[
                "invalid_rows"
            ],
            "duplicate_rows": info[
                "duplicate_rows"
            ],
            "future_feature_rows": info[
                "future_feature_rows"
            ],
            "sha256": info["sha256"],
        })

        if index % 25 == 0:
            print(
                f"[AUDIT] "
                f"{index}/{len(shard_files)} "
                f"ROWS={joined_rows:,}",
                flush=True,
            )

    if joined_rows != args.expected_joined_rows:
        record_issue(
            issues,
            "ERROR",
            "JOIN_ROW_COUNT",
            str(join_dir),
            (
                f"Expected "
                f"{args.expected_joined_rows}, "
                f"actual {joined_rows}"
            ),
        )

    if duplicate_join_keys:
        record_issue(
            issues,
            "WARNING",
            "DUPLICATE_JOIN_KEYS",
            str(join_dir),
            str(duplicate_join_keys),
        )

    source_key_count = sum(
        source_keys.values()
    )

    joined_key_count = sum(
        joined_key_counter.values()
    )

    unmatched_join_rows = 0

    if (
        source_key_columns
        and joined_key_count == joined_rows
    ):

        for key, count in joined_key_counter.items():
            available = source_keys.get(
                key,
                0,
            )

            if count > available:
                unmatched_join_rows += (
                    count - available
                )

        if unmatched_join_rows:
            record_issue(
                issues,
                "ERROR",
                "UNMATCHED_JOIN_EVENTS",
                str(join_dir),
                str(unmatched_join_rows),
            )

    else:
        record_issue(
            issues,
            "WARNING",
            "ROW_LEVEL_RECONCILIATION_UNPROVEN",
            str(join_dir),
            (
                "Complete source-to-JOIN "
                "event reconciliation unavailable"
            ),
        )

    manifest_checks = {}

    if join_manifest:
        manifest_checks = (
            validate_manifest_hashes(
                join_manifest,
                join_dir,
                issues,
            )
        )

    error_count = sum(
        item["severity"] == "ERROR"
        for item in issues
    )

    warning_count = sum(
        item["severity"] == "WARNING"
        for item in issues
    )

    reconciliation_proven = (
        source_key_columns is not None
        and joined_key_count == joined_rows
        and unmatched_join_rows == 0
    )

    integrity_verified = (
        error_count == 0
        and warning_count == 0
        and reconciliation_proven
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": source_rows,
        "expected_source_rows": (
            args.expected_source_rows
        ),
        "joined_rows": joined_rows,
        "expected_joined_rows": (
            args.expected_joined_rows
        ),
        "unjoined_rows": (
            source_rows - joined_rows
        ),
        "join_rate_pct": (
            round(
                joined_rows
                / source_rows
                * 100,
                6,
            )
            if source_rows
            else 0
        ),
        "shard_files": len(
            shard_files
        ),
        "schema_variants": len(
            schema_variants
        ),
        "source_key_rows": (
            source_key_count
        ),
        "joined_key_rows": (
            joined_key_count
        ),
        "duplicate_join_keys": (
            duplicate_join_keys
        ),
        "unmatched_join_rows": (
            unmatched_join_rows
        ),
        "row_level_reconciliation_proven": (
            reconciliation_proven
        ),
        "integrity_verified": (
            integrity_verified
        ),
        "errors": error_count,
        "warnings": warning_count,
        "research_decision": "HOLD",
        "train_experiment_approved": False,
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "limitations": [
            (
                "Timestamp checks only cover "
                "recognized timestamp columns."
            ),
            (
                "SHA256 verifies file identity, "
                "not feature formula correctness."
            ),
            (
                "Feature lookback and closed-candle "
                "semantics require separate "
                "point-in-time verification."
            ),
            (
                "Missing feature values and "
                "selection bias require "
                "additional schema-specific checks."
            ),
            (
                "No independent OOS or "
                "profitability claim is made."
            ),
        ],
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    coverage_path = (
        output_dir / COVERAGE_FILE
    )

    issues_path = (
        output_dir / ISSUES_FILE
    )

    result_path = (
        output_dir / RESULT_FILE
    )

    manifest_path = (
        output_dir / MANIFEST_FILE
    )

    write_csv_atomic(
        coverage_path,
        [
            "file",
            "rows",
            "markets",
            "timeframes",
            "invalid_rows",
            "duplicate_rows",
            "future_feature_rows",
            "sha256",
        ],
        coverage,
    )

    write_csv_atomic(
        issues_path,
        [
            "severity",
            "code",
            "file",
            "detail",
        ],
        issues,
    )

    write_json_atomic(
        result_path,
        result,
    )

    input_hashes = {
        str(source_path): (
            source_info["sha256"]
        ),
    }

    if join_result_path:
        input_hashes[
            str(join_result_path)
        ] = sha256_file(
            join_result_path
        )

    if join_manifest_path:
        input_hashes[
            str(join_manifest_path)
        ] = sha256_file(
            join_manifest_path
        )

    manifest = {
        "version": VERSION,
        "input_sha256": input_hashes,
        "shard_sha256": shard_hashes,
        "output_sha256": {
            COVERAGE_FILE: sha256_file(
                coverage_path
            ),
            ISSUES_FILE: sha256_file(
                issues_path
            ),
            RESULT_FILE: sha256_file(
                result_path
            ),
        },
        "manifest_checks": manifest_checks,
    }

    write_json_atomic(
        manifest_path,
        manifest,
    )

    print()
    print(
        f"Source rows: {source_rows:,}"
    )
    print(
        f"Joined rows: {joined_rows:,}"
    )
    print(
        f"Unjoined rows: "
        f"{source_rows - joined_rows:,}"
    )
    print(
        f"Shard files: {len(shard_files)}"
    )
    print(
        f"Errors: {error_count}"
    )
    print(
        f"Warnings: {warning_count}"
    )
    print(
        f"Integrity verified: "
        f"{integrity_verified}"
    )
    print(
        "Research decision: HOLD"
    )
    print(
        f"Reports: {output_dir}"
    )

    return 0 if error_count == 0 else 1


if __name__ == "__main__":

    try:
        sys.exit(main())

    except Exception as exc:
        print(
            f"FATAL: "
            f"{type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
