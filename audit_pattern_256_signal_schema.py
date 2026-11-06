# -*- coding: utf-8 -*-
"""
audit_pattern_256_signal_schema.py

Clean V001

Pattern 256 Signal Schema Audit

Purpose:
- Verify previous split candle audit evidence.
- Inspect every Signal 256 CSV schema.
- Identify binary-like candidate columns.
- Record sample value distributions.
- Preserve all original data.
- Support SHA256-validated checkpoint/resume.
- Keep research decision HOLD.

This is NOT a trading signal performance test.

Usage:
    py audit_pattern_256_signal_schema.py --root .
    py audit_pattern_256_signal_schema.py --root . --force
"""

import argparse
import csv
import hashlib
import json
import os
import sys

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

TIMEFRAMES = ("h1", "h4", "d1")

SAMPLE_LIMIT = 5000

PREVIOUS_DIR = Path(
    "data/reports/pattern_256_split_candles"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_signal_schema"
)

REPORT_COLUMNS = (
    "timeframe",
    "market",
    "column",
    "sample_rows",
    "nonempty_rows",
    "distinct_sample_values",
    "sample_values",
    "binary_like",
    "candidate_only",
)

FILE_COLUMNS = (
    "timeframe",
    "market",
    "file",
    "source_sha256",
    "column_count",
    "sample_rows",
    "binary_like_columns",
    "status",
    "error",
)

BINARY_VALUES = {
    "0",
    "1",
    "0.0",
    "1.0",
    "true",
    "false",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def atomic_json(path, value):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            value,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")

    os.replace(temporary, path)


def atomic_csv(path, columns, rows):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
        )

        writer.writeheader()
        writer.writerows(rows)

    os.replace(temporary, path)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_previous(root):
    report = root / PREVIOUS_DIR

    result_path = (
        report / "split_candle_result.json"
    )

    manifest_path = (
        report / "split_candle_manifest.json"
    )

    checks_path = (
        report / "split_candle_checks.csv"
    )

    for path in (
        result_path,
        manifest_path,
        checks_path,
    ):
        require(
            path.is_file(),
            f"Missing previous evidence: {path}",
        )

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    require(
        result.get("execution") == "PASS",
        "Previous audit is not PASS",
    )

    require(
        result.get("failed_jobs") == 0,
        "Previous audit contains failed jobs",
    )

    require(
        result.get("completed_jobs")
        == result.get("jobs"),
        "Previous audit is incomplete",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected previous research decision",
    )

    require(
        result.get("independent_oos_proven") is False,
        "Unexpected previous OOS approval",
    )

    hashes = manifest.get(
        "output_sha256",
        {},
    )

    for filename in (
        "split_candle_checks.csv",
        "split_candle_result.json",
    ):
        expected = hashes.get(filename)

        require(
            isinstance(expected, str)
            and len(expected) == 64,
            f"Missing previous SHA256: {filename}",
        )

        actual = sha256_file(
            report / filename
        )

        require(
            actual == expected.lower(),
            f"Previous SHA256 mismatch: {filename}",
        )

    return {
        "manifest_sha256": sha256_file(
            manifest_path
        ),
        "previous_jobs": result["jobs"],
        "previous_period_records": result[
            "period_records"
        ],
    }


def normalize_value(value):
    return str(
        value if value is not None else ""
    ).strip().lower()


def inspect_signal_file(path, timeframe, market):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        columns = reader.fieldnames

        require(
            bool(columns),
            f"Missing CSV header: {path}",
        )

        require(
            len(columns) == len(set(columns)),
            f"Duplicate column names: {path}",
        )

        counters = {
            column: Counter()
            for column in columns
        }

        sample_rows = 0

        for row in reader:
            sample_rows += 1

            for column in columns:
                value = normalize_value(
                    row.get(column)
                )

                counters[column][value] += 1

            if sample_rows >= SAMPLE_LIMIT:
                break

    column_rows = []

    binary_columns = []

    for column in columns:
        counter = counters[column]

        nonempty = {
            value: count
            for value, count in counter.items()
            if value not in ("", "nan", "none", "null")
        }

        distinct = set(nonempty)

        binary_like = (
            bool(distinct)
            and distinct.issubset(BINARY_VALUES)
        )

        if binary_like:
            binary_columns.append(column)

        most_common = counter.most_common(10)

        column_rows.append({
            "timeframe": timeframe,
            "market": market,
            "column": column,
            "sample_rows": sample_rows,
            "nonempty_rows": sum(
                nonempty.values()
            ),
            "distinct_sample_values": len(
                counter
            ),
            "sample_values": json.dumps(
                most_common,
                ensure_ascii=False,
            ),
            "binary_like": binary_like,
            "candidate_only": True,
        })

    return {
        "columns": columns,
        "sample_rows": sample_rows,
        "binary_columns": binary_columns,
        "column_rows": column_rows,
    }


def discover_jobs(root):
    jobs = []

    for timeframe in TIMEFRAMES:
        directory = (
            root
            / "data/signals/256"
            / timeframe
        )

        require(
            directory.is_dir(),
            f"Missing signal directory: {directory}",
        )

        files = sorted(
            directory.glob("KRW-*.csv")
        )

        require(
            bool(files),
            f"No signal CSV files: {directory}",
        )

        for path in files:
            jobs.append(
                (
                    timeframe,
                    path.stem,
                    path,
                )
            )

    return jobs


def checkpoint_valid(
    candidate,
    source_sha256,
    previous_sha256,
    script_sha256,
):
    if not isinstance(candidate, dict):
        return False

    if candidate.get("version") != VERSION:
        return False

    if candidate.get("source_sha256") != source_sha256:
        return False

    if candidate.get(
        "previous_manifest_sha256"
    ) != previous_sha256:
        return False

    if candidate.get("script_sha256") != script_sha256:
        return False

    data = candidate.get("data")

    if not isinstance(data, dict):
        return False

    if not isinstance(
        data.get("column_rows"),
        list,
    ):
        return False

    if not isinstance(
        data.get("columns"),
        list,
    ):
        return False

    if len(data["column_rows"]) != len(
        data["columns"]
    ):
        return False

    if not isinstance(
        data.get("binary_columns"),
        list,
    ):
        return False

    return True


def run(root, force):
    root = root.resolve()

    print("=" * 64)
    print("PATTERN 256 SIGNAL SCHEMA AUDIT")
    print(f"Version: {VERSION}")
    print(f"Root: {root}")
    print("=" * 64)

    previous = verify_previous(root)

    print("[PASS] Previous evidence verified")

    script_hash = sha256_file(
        Path(__file__).resolve()
    )

    jobs = discover_jobs(root)

    print(
        f"[INFO] Discovered jobs: {len(jobs)}"
    )

    output = root / OUTPUT_DIR

    checkpoints = (
        output / "checkpoints"
    )

    checkpoints.mkdir(
        parents=True,
        exist_ok=True,
    )

    column_rows = []
    file_rows = []
    errors = []

    resumed = 0
    executed = 0

    for index, (
        timeframe,
        market,
        path,
    ) in enumerate(jobs, 1):

        source_hash = ""

        try:
            source_hash = sha256_file(path)

            checkpoint = (
                checkpoints
                / f"{timeframe}_{market}.json"
            )

            cached = None

            if not force and checkpoint.is_file():
                try:
                    candidate = read_json(
                        checkpoint
                    )

                    if checkpoint_valid(
                        candidate,
                        source_hash,
                        previous["manifest_sha256"],
                        script_hash,
                    ):
                        cached = candidate["data"]

                except (
                    OSError,
                    ValueError,
                    TypeError,
                ):
                    cached = None

            if cached is not None:
                data = cached
                resumed += 1

            else:
                data = inspect_signal_file(
                    path,
                    timeframe,
                    market,
                )

                checkpoint_data = {
                    "version": VERSION,
                    "source_sha256": source_hash,
                    "previous_manifest_sha256": (
                        previous["manifest_sha256"]
                    ),
                    "script_sha256": script_hash,
                    "data": data,
                }

                atomic_json(
                    checkpoint,
                    checkpoint_data,
                )

                executed += 1

            column_rows.extend(
                data["column_rows"]
            )

            file_rows.append({
                "timeframe": timeframe,
                "market": market,
                "file": str(
                    path.relative_to(root)
                ),
                "source_sha256": source_hash,
                "column_count": len(
                    data["columns"]
                ),
                "sample_rows": data[
                    "sample_rows"
                ],
                "binary_like_columns": json.dumps(
                    data["binary_columns"],
                    ensure_ascii=False,
                ),
                "status": "PASS",
                "error": "",
            })

        except Exception as exc:
            message = (
                f"{type(exc).__name__}: {exc}"
            )

            errors.append({
                "timeframe": timeframe,
                "market": market,
                "error": message,
            })

            file_rows.append({
                "timeframe": timeframe,
                "market": market,
                "file": str(
                    path.relative_to(root)
                ),
                "source_sha256": source_hash,
                "column_count": 0,
                "sample_rows": 0,
                "binary_like_columns": "[]",
                "status": "ERROR",
                "error": message,
            })

            print(
                f"[ERROR] {timeframe} {market}: "
                f"{message}",
                flush=True,
            )

        if (
            index % 25 == 0
            or index == len(jobs)
        ):
            print(
                f"[PROGRESS] {index}/{len(jobs)} "
                f"resumed={resumed} "
                f"executed={executed} "
                f"errors={len(errors)}",
                flush=True,
            )

    files_path = (
        output / "signal_schema_files.csv"
    )

    columns_path = (
        output / "signal_schema_columns.csv"
    )

    result_path = (
        output / "signal_schema_result.json"
    )

    manifest_path = (
        output / "signal_schema_manifest.json"
    )

    atomic_csv(
        files_path,
        FILE_COLUMNS,
        file_rows,
    )

    atomic_csv(
        columns_path,
        REPORT_COLUMNS,
        column_rows,
    )

    complete = (
        len(errors) == 0
        and len(file_rows) == len(jobs)
        and resumed + executed == len(jobs)
    )

    binary_candidates = sum(
        str(row["binary_like"]).lower()
        == "true"
        for row in column_rows
    )

    result = {
        "audit": "pattern_256_signal_schema",
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": (
            "PASS"
            if complete
            else "REVIEW_REQUIRED"
        ),
        "previous_evidence_verified": True,
        "previous_evidence": previous,
        "jobs": len(jobs),
        "completed_jobs": resumed + executed,
        "resumed_jobs": resumed,
        "executed_jobs": executed,
        "failed_jobs": len(errors),
        "errors": errors,
        "column_records": len(column_rows),
        "binary_like_candidate_records": (
            binary_candidates
        ),
        "sample_limit_per_file": SAMPLE_LIMIT,
        "signal_column_selected": False,
        "signal_counts_verified": False,
        "historical_research_overlap_checked": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Only the first 5000 rows of each CSV are sampled.",
            "Binary-like does not establish trading signal meaning.",
            "No signal event count is approved.",
            "No prediction performance is calculated.",
            "Historical research exposure is not reconstructed.",
            "Independent OOS remains unproven.",
        ],
    }

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "audit": "pattern_256_signal_schema",
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": script_hash,
        "previous_manifest_sha256": (
            previous["manifest_sha256"]
        ),
        "output_sha256": {
            "signal_schema_files.csv": (
                sha256_file(files_path)
            ),
            "signal_schema_columns.csv": (
                sha256_file(columns_path)
            ),
            "signal_schema_result.json": (
                sha256_file(result_path)
            ),
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print()
    print("===== RESULT =====")
    print("Execution:", result["execution"])
    print("Jobs:", len(jobs))
    print("Executed:", executed)
    print("Resumed:", resumed)
    print("Errors:", len(errors))
    print(
        "Column records:",
        len(column_rows),
    )
    print(
        "Binary-like candidate records:",
        binary_candidates,
    )
    print("Signal counts verified: False")
    print("Research decision: HOLD")

    print()
    print("===== OUTPUT =====")
    print(files_path)
    print(columns_path)
    print(result_path)
    print(manifest_path)

    return 0 if complete else 1


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 Signal Schema Audit"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore valid checkpoints",
    )

    args = parser.parse_args()

    try:
        return run(
            Path(args.root),
            args.force,
        )

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
