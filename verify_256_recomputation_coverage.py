
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
verify_256_recomputation_coverage.py
Clean V001

Independent recomputation result consistency verification.

Checks:
- audit_result.json
- audit_summary.csv
- checkpoint.json
- Job coverage and row totals
- Per-column mismatch totals
- Source SHA256 consistency across reports
- Report file SHA256 manifest

This script does not modify original feature/signal data.
It does not approve live trading.
"""

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
EXPECTED_AUDIT_VERSION = "Clean V003-26"

SOURCE_RELATIVE = Path(
    "data/reports/pattern_256_independent_recomputation"
)

OUTPUT_RELATIVE = Path(
    "data/reports/pattern_256_recomputation_coverage"
)

SOURCE_FILES = (
    "audit_result.json",
    "audit_summary.csv",
    "checkpoint.json",
)

OUTPUT_FILES = (
    "coverage_result.json",
    "coverage_manifest.json",
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        required = {
            "job",
            "status",
            "rows",
            "mismatches",
            "feature_sha256",
            "signal_sha256",
            "error",
        }

        if not reader.fieldnames:
            raise ValueError("CSV header missing")

        missing = required - set(reader.fieldnames)

        if missing:
            raise ValueError(
                f"CSV columns missing: {sorted(missing)}"
            )

        return list(reader)


def require_integer(value, label):
    if isinstance(value, bool):
        raise ValueError(f"Invalid integer: {label}")

    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Invalid integer: {label}={value}"
        ) from None

    if result < 0:
        raise ValueError(
            f"Negative integer: {label}={result}"
        )

    return result


def require_hash(value, label):
    if not isinstance(value, str):
        raise ValueError(f"Invalid SHA256: {label}")

    value = value.strip().lower()

    if len(value) != 64:
        raise ValueError(f"Invalid SHA256 length: {label}")

    if any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"Invalid SHA256 characters: {label}")

    return value


def atomic_json_write(path, payload):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        path.name + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()

    temporary.replace(path)


def compare_job(job_name, result, checkpoint, csv_row):
    errors = []

    if not isinstance(result, dict):
        return ["Invalid result job object"]

    if not isinstance(checkpoint, dict):
        return ["Invalid checkpoint job object"]

    for label, item in (
        ("result", result),
        ("checkpoint", checkpoint),
    ):
        if item.get("version") != EXPECTED_AUDIT_VERSION:
            errors.append(
                f"{label}: version mismatch"
            )

        if item.get("status") != "PASS":
            errors.append(
                f"{label}: status is not PASS"
            )

    if csv_row.get("status") != "PASS":
        errors.append("CSV status is not PASS")

    try:
        result_rows = require_integer(
            result.get("rows"),
            f"{job_name}.result.rows",
        )

        checkpoint_rows = require_integer(
            checkpoint.get("rows"),
            f"{job_name}.checkpoint.rows",
        )

        csv_rows = require_integer(
            csv_row.get("rows"),
            f"{job_name}.csv.rows",
        )

        if result_rows == 0:
            errors.append("Zero audited rows")

        if not (
            result_rows
            == checkpoint_rows
            == csv_rows
        ):
            errors.append("Row count mismatch")

    except ValueError as exc:
        errors.append(str(exc))

    try:
        mismatch_values = [
            require_integer(
                result.get("mismatches"),
                "result.mismatches",
            ),
            require_integer(
                checkpoint.get("mismatches"),
                "checkpoint.mismatches",
            ),
            require_integer(
                csv_row.get("mismatches"),
                "csv.mismatches",
            ),
        ]

        if any(value != 0 for value in mismatch_values):
            errors.append("Nonzero mismatches")

    except ValueError as exc:
        errors.append(str(exc))

    for hash_name in (
        "feature_sha256",
        "signal_sha256",
    ):
        try:
            values = [
                require_hash(
                    result.get(hash_name),
                    f"result.{hash_name}",
                ),
                require_hash(
                    checkpoint.get(hash_name),
                    f"checkpoint.{hash_name}",
                ),
                require_hash(
                    csv_row.get(hash_name),
                    f"csv.{hash_name}",
                ),
            ]

            if len(set(values)) != 1:
                errors.append(
                    f"{hash_name} mismatch"
                )

        except ValueError as exc:
            errors.append(str(exc))

    result_columns = result.get("by_column")
    checkpoint_columns = checkpoint.get("by_column")

    if not isinstance(result_columns, dict):
        errors.append("Invalid result by_column")
    elif not result_columns:
        errors.append("Empty result by_column")
    else:
        for column, value in result_columns.items():
            try:
                mismatch = require_integer(
                    value,
                    f"{job_name}.{column}",
                )

                if mismatch != 0:
                    errors.append(
                        f"Column mismatch: {column}"
                    )

            except ValueError as exc:
                errors.append(str(exc))

    if result_columns != checkpoint_columns:
        errors.append("by_column disagreement")

    if result.get("examples") != checkpoint.get("examples"):
        errors.append("Mismatch examples disagreement")

    if csv_row.get("error", "").strip():
        errors.append("CSV error field not empty")

    return errors


def verify(root):
    source_dir = root / SOURCE_RELATIVE
    output_dir = root / OUTPUT_RELATIVE

    for filename in SOURCE_FILES:
        path = source_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Required report missing: {path}"
            )

    audit = read_json(
        source_dir / "audit_result.json"
    )

    checkpoint = read_json(
        source_dir / "checkpoint.json"
    )

    summary = read_csv(
        source_dir / "audit_summary.csv"
    )

    errors = []

    if audit.get("version") != EXPECTED_AUDIT_VERSION:
        errors.append("Audit version mismatch")

    if checkpoint.get("version") != EXPECTED_AUDIT_VERSION:
        errors.append("Checkpoint version mismatch")

    if audit.get("status") != "PASS":
        errors.append("Audit status is not PASS")

    if audit.get("independent_oos_proven") is not False:
        errors.append(
            "Unexpected independent OOS proof flag"
        )

    if audit.get("approved_for_live_trading") is not False:
        errors.append(
            "Unexpected live trading approval flag"
        )

    result_jobs = audit.get("results")
    checkpoint_jobs = checkpoint.get("jobs")

    if not isinstance(result_jobs, dict):
        raise ValueError("Invalid audit results object")

    if not isinstance(checkpoint_jobs, dict):
        raise ValueError("Invalid checkpoint jobs object")

    csv_jobs = {}

    for row in summary:
        name = row.get("job", "").strip()

        if not name:
            errors.append("Empty CSV job name")
            continue

        if name in csv_jobs:
            errors.append(
                f"Duplicate CSV job: {name}"
            )
            continue

        csv_jobs[name] = row

    result_names = set(result_jobs)
    checkpoint_names = set(checkpoint_jobs)
    csv_names = set(csv_jobs)

    if result_names != checkpoint_names:
        errors.append(
            "Result/checkpoint job coverage mismatch"
        )

    if result_names != csv_names:
        errors.append(
            "Result/CSV job coverage mismatch"
        )

    if not result_names:
        errors.append("No audited jobs")

    verified_jobs = 0
    verified_rows = 0
    failed_jobs = {}

    for job_name in sorted(result_names):
        if (
            job_name not in checkpoint_jobs
            or job_name not in csv_jobs
        ):
            failed_jobs[job_name] = [
                "Job missing from checkpoint or CSV"
            ]
            continue

        result = result_jobs[job_name]
        checkpoint_job = checkpoint_jobs[job_name]
        csv_row = csv_jobs[job_name]

        job_errors = compare_job(
            job_name,
            result,
            checkpoint_job,
            csv_row,
        )

        if job_errors:
            failed_jobs[job_name] = job_errors
            continue

        verified_jobs += 1
        verified_rows += int(result["rows"])

    if failed_jobs:
        errors.append(
            f"Failed job consistency checks: {len(failed_jobs)}"
        )

    try:
        expected_jobs = require_integer(
            audit.get("jobs"),
            "audit.jobs",
        )

        if expected_jobs != len(result_names):
            errors.append("Audit job count mismatch")

        if expected_jobs != verified_jobs:
            errors.append("Verified job count mismatch")

        expected_rows = require_integer(
            audit.get("audited_rows"),
            "audit.audited_rows",
        )

        if expected_rows != verified_rows:
            errors.append("Audited row total mismatch")

        expected_failed = require_integer(
            audit.get("failed_jobs"),
            "audit.failed_jobs",
        )

        if expected_failed != 0:
            errors.append("Audit contains failed jobs")

    except ValueError as exc:
        errors.append(str(exc))

    source_hashes = {
        filename: sha256_file(
            source_dir / filename
        )
        for filename in SOURCE_FILES
    }

    status = "PASS" if not errors else "FAIL"

    report = {
        "version": VERSION,
        "source_audit_version": EXPECTED_AUDIT_VERSION,
        "generated_utc": utc_now(),
        "status": status,
        "source_jobs": len(result_names),
        "verified_jobs": verified_jobs,
        "verified_rows": verified_rows,
        "failed_job_count": len(failed_jobs),
        "errors": errors,
        "failed_job_examples": dict(
            list(failed_jobs.items())[:20]
        ),
        "source_sha256": source_hashes,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output_dir / "coverage_result.json"
    )

    atomic_json_write(
        result_path,
        report,
    )

    manifest = {
        "version": VERSION,
        "generated_utc": utc_now(),
        "source_sha256": source_hashes,
        "output_sha256": {
            "coverage_result.json": sha256_file(
                result_path
            )
        },
    }

    atomic_json_write(
        output_dir / "coverage_manifest.json",
        manifest,
    )

    print(
        json.dumps(
            {
                "status": status,
                "verified_jobs": verified_jobs,
                "verified_rows": verified_rows,
                "failed_job_count": len(failed_jobs),
                "errors": errors[:10],
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    return status == "PASS"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
        help="Audit workspace root",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    if not root.is_dir():
        raise NotADirectoryError(root)

    success = verify(root)

    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
