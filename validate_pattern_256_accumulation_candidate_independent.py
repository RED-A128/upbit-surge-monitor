
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_candidate_independent.py

Upbit Surge Monitor
Candidate Independent Validation
Clean V001

Purpose
-------
1. Read existing candidate OOS evaluation reports.
2. Verify input files and SHA256 hashes.
3. Inspect candidate decisions and supporting metrics.
4. Detect missing evidence and inconsistent records.
5. Produce an independent validation readiness report.
6. Never modify upstream reports or OHLCV data.
7. Never claim true independent predictive validation
   without a separately identified unseen dataset.

Execution
---------
py validate_pattern_256_accumulation_candidate_independent.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor"

Optional:
    --output-dir PATH
    --allow-existing-output

Outputs
-------
data/reports/pattern_256_accumulation_candidate_independent/
    independent_validation_summary.csv
    independent_validation_checks.csv
    independent_validation_result.json
    independent_validation_manifest.json

Dependencies
------------
Python 3.11+
Standard library only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


VERSION = "Clean V001"

INPUT_DIRECTORY = (
    Path("data")
    / "reports"
    / "pattern_256_accumulation_candidate_oos_evaluation"
)

OUTPUT_DIRECTORY = (
    Path("data")
    / "reports"
    / "pattern_256_accumulation_candidate_independent"
)

REQUIRED_INPUT_FILES = (
    "oos_evaluation_summary.csv",
    "oos_evaluation_comparison.csv",
    "oos_evaluation_decision.csv",
    "oos_evaluation_result.json",
    "oos_evaluation_manifest.json",
)

OUTPUT_FILES = (
    "independent_validation_summary.csv",
    "independent_validation_checks.csv",
    "independent_validation_result.json",
    "independent_validation_manifest.json",
)

SUMMARY_FIELDS = (
    "source_file",
    "source_row",
    "candidate_id",
    "timeframe",
    "reported_decision",
    "train_samples",
    "oos_samples",
    "train_hit_rate",
    "oos_hit_rate",
    "hit_rate_gap",
    "evidence_status",
    "independent_validation_status",
    "reason",
)

CHECK_FIELDS = (
    "check_id",
    "status",
    "detail",
)

# Aliases are deliberately conservative.
# An unknown column is not automatically interpreted
# as a performance metric.

CANDIDATE_ALIASES = (
    "candidate_id",
    "candidate",
    "rule_id",
    "rule_name",
    "candidate_rule",
)

TIMEFRAME_ALIASES = (
    "timeframe",
    "interval",
    "tf",
)

DECISION_ALIASES = (
    "decision",
    "final_decision",
    "recommendation",
    "selection_decision",
    "status",
)

TRAIN_SAMPLE_ALIASES = (
    "train_samples",
    "train_sample_count",
    "train_n",
    "train_count",
)

OOS_SAMPLE_ALIASES = (
    "oos_samples",
    "oos_sample_count",
    "oos_n",
    "oos_count",
)

TRAIN_RATE_ALIASES = (
    "train_hit_rate",
    "train_hit_rate_30pct",
    "train_success_rate",
)

OOS_RATE_ALIASES = (
    "oos_hit_rate",
    "oos_hit_rate_30pct",
    "oos_success_rate",
)

ALLOWED_TIMEFRAMES = {
    "h1",
    "h4",
    "d1",
    "1h",
    "4h",
    "1d",
}

RATE_FIELDS = (
    "train_hit_rate",
    "oos_hit_rate",
)

SAMPLE_FIELDS = (
    "train_samples",
    "oos_samples",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )


def log(message: str) -> None:
    stamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"[{stamp}] {message}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            chunk = stream.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def normalize_column(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )


def normalize_text(value: Any) -> str:
    return str(value or "").strip()


def find_column(
    columns: list[str],
    aliases: tuple[str, ...],
) -> str | None:
    lookup = {
        normalize_column(column): column
        for column in columns
    }

    for alias in aliases:
        actual = lookup.get(
            normalize_column(alias)
        )

        if actual is not None:
            return actual

    return None


def parse_number(
    value: Any,
) -> float | None:
    text = normalize_text(value)

    if not text:
        return None

    text = text.replace(",", "")

    try:
        result = float(text)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result):
        return None

    return result


def parse_count(
    value: Any,
) -> int | None:
    number = parse_number(value)

    if number is None:
        return None

    if number < 0:
        return None

    if not number.is_integer():
        return None

    return int(number)


def parse_rate(
    value: Any,
) -> float | None:
    """
    Accept only an unambiguous rate representation.

    0.25  -> 0.25
    25%   -> 0.25

    Bare 25 is rejected rather than guessed
    to represent either 25 or 25%.
    """
    text = normalize_text(value)

    if not text:
        return None

    if text.endswith("%"):
        number = parse_number(
            text[:-1]
        )

        if number is None:
            return None

        number /= 100.0
    else:
        number = parse_number(text)

    if number is None:
        return None

    if not 0.0 <= number <= 1.0:
        return None

    return number


def read_csv(
    path: Path,
) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                f"CSV header missing: {path}"
            )

        columns = list(reader.fieldnames)

        normalized = [
            normalize_column(item)
            for item in columns
        ]

        if len(normalized) != len(set(normalized)):
            raise ValueError(
                f"Duplicate normalized columns: {path}"
            )

        rows = []

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            if None in row:
                raise ValueError(
                    f"Unexpected extra columns: "
                    f"{path}, line={line_number}"
                )

            if any(
                value is None
                for value in row.values()
            ):
                raise ValueError(
                    f"Missing CSV fields: "
                    f"{path}, line={line_number}"
                )

            rows.append({
                str(key): str(value)
                for key, value in row.items()
            })

    return columns, rows


def read_json(path: Path) -> Any:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        return json.load(stream)


def add_check(
    checks: list[dict[str, str]],
    check_id: str,
    status: str,
    detail: str,
) -> None:
    checks.append({
        "check_id": check_id,
        "status": status,
        "detail": detail,
    })

    log(
        f"[{status}] {check_id}: {detail}"
    )


def atomic_write_text(
    path: Path,
    content: str,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_name = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_name = stream.name
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(
            temporary_name,
            path,
        )

    finally:
        if (
            temporary_name is not None
            and os.path.exists(temporary_name)
        ):
            os.unlink(temporary_name)


def write_json(
    path: Path,
    payload: dict[str, Any],
) -> None:
    content = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )

    atomic_write_text(
        path,
        content + "\n",
    )


def write_csv(
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    import io

    buffer = io.StringIO(
        newline=""
    )

    writer = csv.DictWriter(
        buffer,
        fieldnames=list(columns),
        extrasaction="ignore",
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    atomic_write_text(
        path,
        buffer.getvalue(),
    )


def resolve_root(
    value: str,
) -> Path:
    root = Path(value).expanduser().resolve()

    if not root.is_dir():
        raise FileNotFoundError(
            f"Project root not found: {root}"
        )

    if not (
        root / "data" / "reports"
    ).is_dir():
        raise FileNotFoundError(
            f"Reports directory not found: "
            f"{root / 'data' / 'reports'}"
        )

    return root


def resolve_output(
    root: Path,
    requested: str | None,
) -> Path:
    if requested:
        output = Path(
            requested
        ).expanduser().resolve()
    else:
        output = (
            root / OUTPUT_DIRECTORY
        ).resolve()

    input_dir = (
        root / INPUT_DIRECTORY
    ).resolve()

    if output == input_dir:
        raise ValueError(
            "Output directory cannot equal input directory."
        )

    if input_dir in output.parents:
        raise ValueError(
            "Output directory cannot be inside input directory."
        )

    if output in input_dir.parents:
        raise ValueError(
            "Output directory cannot contain input directory."
        )

    return output


def capture_input_hashes(
    input_dir: Path,
) -> dict[str, str]:
    hashes = {}

    for filename in REQUIRED_INPUT_FILES:
        path = input_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Required input missing: {path}"
            )

        hashes[filename] = sha256_file(path)

    return hashes


def verify_input_hashes(
    input_dir: Path,
    original: dict[str, str],
) -> None:
    current = capture_input_hashes(
        input_dir
    )

    if current != original:
        changed = [
            name
            for name in REQUIRED_INPUT_FILES
            if current.get(name) != original.get(name)
        ]

        raise RuntimeError(
            "Production inputs changed during execution: "
            + ", ".join(changed)
        )


def get_value(
    row: dict[str, str],
    column: str | None,
) -> str:
    if column is None:
        return ""

    return normalize_text(
        row.get(column, "")
    )


def inspect_csv_schema(
    filename: str,
    columns: list[str],
    rows: list[dict[str, str]],
    checks: list[dict[str, str]],
) -> None:
    add_check(
        checks,
        f"schema_{filename}",
        "PASS" if columns else "FAIL",
        (
            f"columns={len(columns)}, "
            f"rows={len(rows)}"
        ),
    )

    if not rows:
        add_check(
            checks,
            f"data_{filename}",
            "INSUFFICIENT_DATA",
            "CSV contains no data rows.",
        )


def evaluate_record(
    filename: str,
    row_number: int,
    row: dict[str, str],
    mapping: dict[str, str | None],
) -> dict[str, Any]:
    candidate = get_value(
        row,
        mapping["candidate"],
    )

    timeframe = get_value(
        row,
        mapping["timeframe"],
    )

    decision = get_value(
        row,
        mapping["decision"],
    )

    train_count = parse_count(
        get_value(
            row,
            mapping["train_samples"],
        )
    )

    oos_count = parse_count(
        get_value(
            row,
            mapping["oos_samples"],
        )
    )

    train_rate = parse_rate(
        get_value(
            row,
            mapping["train_hit_rate"],
        )
    )

    oos_rate = parse_rate(
        get_value(
            row,
            mapping["oos_hit_rate"],
        )
    )

    reasons = []

    if not candidate:
        reasons.append(
            "candidate_identifier_missing"
        )

    if not timeframe:
        reasons.append(
            "timeframe_missing"
        )
    elif timeframe.lower() not in ALLOWED_TIMEFRAMES:
        reasons.append(
            "unrecognized_timeframe"
        )

    if not decision:
        reasons.append(
            "reported_decision_missing"
        )

    if train_count is None:
        reasons.append(
            "train_sample_count_missing_or_invalid"
        )

    if oos_count is None:
        reasons.append(
            "oos_sample_count_missing_or_invalid"
        )

    if train_rate is None:
        reasons.append(
            "train_hit_rate_missing_or_invalid"
        )

    if oos_rate is None:
        reasons.append(
            "oos_hit_rate_missing_or_invalid"
        )

    gap = None

    if (
        train_rate is not None
        and oos_rate is not None
    ):
        gap = oos_rate - train_rate

    if (
        train_count is not None
        and train_count == 0
    ):
        reasons.append(
            "zero_train_samples"
        )

    if (
        oos_count is not None
        and oos_count == 0
    ):
        reasons.append(
            "zero_oos_samples"
        )

    evidence_status = (
        "INSUFFICIENT_DATA"
        if reasons
        else "METRICS_PRESENT"
    )

    # This program does not use an unseen external
    # dataset. Therefore PASS is never awarded
    # as a genuine independent predictive result.

    independent_status = "NOT_INDEPENDENTLY_TESTED"

    if reasons:
        independent_status = "INSUFFICIENT_DATA"

    return {
        "source_file": filename,
        "source_row": row_number,
        "candidate_id": candidate,
        "timeframe": timeframe,
        "reported_decision": decision,
        "train_samples": (
            "" if train_count is None
            else train_count
        ),
        "oos_samples": (
            "" if oos_count is None
            else oos_count
        ),
        "train_hit_rate": (
            "" if train_rate is None
            else train_rate
        ),
        "oos_hit_rate": (
            "" if oos_rate is None
            else oos_rate
        ),
        "hit_rate_gap": (
            "" if gap is None
            else round(gap, 10)
        ),
        "evidence_status": evidence_status,
        "independent_validation_status": (
            independent_status
        ),
        "reason": (
            ";".join(reasons)
            if reasons
            else "Separate unseen dataset required."
        ),
    }


def build_mapping(
    columns: list[str],
) -> dict[str, str | None]:
    return {
        "candidate": find_column(
            columns,
            CANDIDATE_ALIASES,
        ),
        "timeframe": find_column(
            columns,
            TIMEFRAME_ALIASES,
        ),
        "decision": find_column(
            columns,
            DECISION_ALIASES,
        ),
        "train_samples": find_column(
            columns,
            TRAIN_SAMPLE_ALIASES,
        ),
        "oos_samples": find_column(
            columns,
            OOS_SAMPLE_ALIASES,
        ),
        "train_hit_rate": find_column(
            columns,
            TRAIN_RATE_ALIASES,
        ),
        "oos_hit_rate": find_column(
            columns,
            OOS_RATE_ALIASES,
        ),
    }


def inspect_records(
    datasets: dict[
        str,
        tuple[
            list[str],
            list[dict[str, str]],
        ],
    ],
    checks: list[dict[str, str]],
) -> list[dict[str, Any]]:
    results = []

    for filename in (
        "oos_evaluation_summary.csv",
        "oos_evaluation_comparison.csv",
        "oos_evaluation_decision.csv",
    ):
        columns, rows = datasets[filename]

        inspect_csv_schema(
            filename,
            columns,
            rows,
            checks,
        )

        mapping = build_mapping(
            columns
        )

        add_check(
            checks,
            f"mapping_{filename}",
            "PASS",
            json.dumps(
                mapping,
                ensure_ascii=False,
                sort_keys=True,
            ),
        )

        for index, row in enumerate(
            rows,
            start=2,
        ):
            result = evaluate_record(
                filename,
                index,
                row,
                mapping,
            )

            results.append(result)

    return results


def inspect_json_metadata(
    result_data: Any,
    manifest_data: Any,
    checks: list[dict[str, str]],
) -> None:
    if isinstance(result_data, dict):
        add_check(
            checks,
            "oos_result_json",
            "PASS",
            "Valid JSON object.",
        )
    else:
        add_check(
            checks,
            "oos_result_json",
            "INSUFFICIENT_DATA",
            "Result JSON is not an object.",
        )

    if isinstance(manifest_data, dict):
        add_check(
            checks,
            "oos_manifest_json",
            "PASS",
            "Valid JSON object.",
        )
    else:
        add_check(
            checks,
            "oos_manifest_json",
            "INSUFFICIENT_DATA",
            "Manifest JSON is not an object.",
        )

    # SHA256 values are captured independently.
    # Unknown upstream manifest layouts are not
    # interpreted as verified file-hash mappings.

    add_check(
        checks,
        "upstream_manifest_semantics",
        "NOT_VERIFIED",
        (
            "Manifest structure has not been "
            "mapped to a verified upstream schema."
        ),
    )


def check_duplicate_records(
    records: list[dict[str, Any]],
    checks: list[dict[str, str]],
) -> None:
    keys = []

    for row in records:
        candidate = row["candidate_id"]
        timeframe = row["timeframe"]

        if not candidate or not timeframe:
            continue

        keys.append((
            row["source_file"],
            candidate,
            timeframe,
        ))

    counts = Counter(keys)

    duplicates = [
        key
        for key, count in counts.items()
        if count > 1
    ]

    if duplicates:
        add_check(
            checks,
            "duplicate_candidate_timeframe",
            "REVIEW",
            (
                f"Duplicate keys found: "
                f"{len(duplicates)}. "
                "May represent distinct metrics "
                "or horizons; not automatically invalid."
            ),
        )
    else:
        add_check(
            checks,
            "duplicate_candidate_timeframe",
            "PASS",
            "No duplicate candidate/timeframe keys.",
        )


def inspect_decisions(
    records: list[dict[str, Any]],
    checks: list[dict[str, str]],
) -> None:
    decisions = Counter()

    for row in records:
        if row["source_file"] != (
            "oos_evaluation_decision.csv"
        ):
            continue

        decision = row[
            "reported_decision"
        ]

        if decision:
            decisions[decision] += 1

    if decisions:
        add_check(
            checks,
            "reported_candidate_decisions",
            "PASS",
            json.dumps(
                dict(decisions),
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
    else:
        add_check(
            checks,
            "reported_candidate_decisions",
            "INSUFFICIENT_DATA",
            "No recognized decision column or values.",
        )


def prepare_output(
    output_dir: Path,
    allow_existing: bool,
) -> None:
    if output_dir.exists():
        if not output_dir.is_dir():
            raise ValueError(
                f"Output path is not a directory: "
                f"{output_dir}"
            )

        existing = list(
            output_dir.iterdir()
        )

        if existing:
            if not allow_existing:
                raise FileExistsError(
                    "Output directory is not empty. "
                    "Existing reports will not be "
                    f"overwritten: {output_dir}"
                )

            # Even when explicitly allowed, refuse
            # to overwrite files outside this
            # program's known output set.

            unexpected = [
                item.name
                for item in existing
                if item.name not in OUTPUT_FILES
            ]

            if unexpected:
                raise FileExistsError(
                    "Unexpected files in output directory: "
                    + ", ".join(unexpected)
                )

    else:
        output_dir.mkdir(
            parents=True,
            exist_ok=False,
        )


def build_manifest(
    output_dir: Path,
    input_hashes: dict[str, str],
) -> dict[str, Any]:
    hashes = {}

    for filename in OUTPUT_FILES:
        if filename == (
            "independent_validation_manifest.json"
        ):
            continue

        path = output_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Output missing: {path}"
            )

        hashes[filename] = sha256_file(
            path
        )

    return {
        "version": VERSION,
        "created_at_utc": utc_now(),
        "input_sha256": input_hashes,
        "output_sha256": hashes,
        "manifest_self_hash": None,
        "note": (
            "Manifest does not hash itself. "
            "Input hashes are point-in-time snapshots."
        ),
    }


def verify_output_manifest(
    output_dir: Path,
) -> None:
    manifest_path = (
        output_dir
        / "independent_validation_manifest.json"
    )

    manifest = read_json(
        manifest_path
    )

    expected = manifest.get(
        "output_sha256",
        {},
    )

    if not isinstance(expected, dict):
        raise ValueError(
            "Invalid output manifest hash structure."
        )

    expected_names = set(
        OUTPUT_FILES
    ) - {
        "independent_validation_manifest.json"
    }

    if set(expected) != expected_names:
        raise ValueError(
            "Output manifest file set mismatch."
        )

    for filename, expected_hash in expected.items():
        actual_hash = sha256_file(
            output_dir / filename
        )

        if actual_hash != expected_hash:
            raise RuntimeError(
                f"Output SHA256 mismatch: {filename}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Candidate independent validation "
            "readiness and integrity audit."
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help="Production project root.",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional independent report destination.",
    )

    parser.add_argument(
        "--allow-existing-output",
        action="store_true",
        help=(
            "Allow replacement of this program's "
            "four known report files only."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log("=" * 70)
    log(
        "Candidate Independent Validation "
        f"/ {VERSION}"
    )
    log("=" * 70)

    root = resolve_root(
        args.root
    )

    input_dir = (
        root / INPUT_DIRECTORY
    ).resolve()

    output_dir = resolve_output(
        root,
        args.output_dir,
    )

    log(f"[ROOT] {root}")
    log(f"[INPUT] {input_dir}")
    log(f"[OUTPUT] {output_dir}")

    checks: list[dict[str, str]] = []

    log("[CHECK] Production input files")

    before_hashes = capture_input_hashes(
        input_dir
    )

    add_check(
        checks,
        "required_input_files",
        "PASS",
        (
            f"{len(before_hashes)} required "
            "input files found."
        ),
    )

    log("[READ] Existing OOS evaluation reports")

    datasets = {}

    for filename in (
        "oos_evaluation_summary.csv",
        "oos_evaluation_comparison.csv",
        "oos_evaluation_decision.csv",
    ):
        datasets[filename] = read_csv(
            input_dir / filename
        )

    result_data = read_json(
        input_dir / "oos_evaluation_result.json"
    )

    manifest_data = read_json(
        input_dir / "oos_evaluation_manifest.json"
    )

    inspect_json_metadata(
        result_data,
        manifest_data,
        checks,
    )

    log("[CHECK] Candidate evaluation records")

    records = inspect_records(
        datasets,
        checks,
    )

    check_duplicate_records(
        records,
        checks,
    )

    inspect_decisions(
        records,
        checks,
    )

    log("[CHECK] Independent validation eligibility")

    add_check(
        checks,
        "unseen_dataset",
        "NOT_VERIFIED",
        (
            "No separately identified unseen "
            "dataset was provided to this program."
        ),
    )

    add_check(
        checks,
        "selection_freeze",
        "NOT_VERIFIED",
        (
            "No independently verifiable "
            "pre-validation rule freeze was provided."
        ),
    )

    add_check(
        checks,
        "future_information_leakage",
        "NOT_VERIFIED",
        (
            "Aggregate reports alone cannot prove "
            "absence of future-data leakage."
        ),
    )

    log("[CHECK] Production input SHA256")

    verify_input_hashes(
        input_dir,
        before_hashes,
    )

    add_check(
        checks,
        "input_sha256_before_output",
        "PASS",
        "All five upstream input hashes unchanged.",
    )

    counts = Counter(
        row["evidence_status"]
        for row in records
    )

    check_counts = Counter(
        item["status"]
        for item in checks
    )

    # PASS means this audit executed correctly.
    # It does NOT mean the strategy passed
    # independent predictive validation.

    result = {
        "program": (
            "validate_pattern_256_accumulation_"
            "candidate_independent.py"
        ),
        "version": VERSION,
        "created_at_utc": utc_now(),
        "execution_status": "PASS",
        "independent_predictive_validation": (
            "NOT_PERFORMED"
        ),
        "candidate_approval": (
            "NOT_DETERMINED"
        ),
        "records_inspected": len(records),
        "evidence_counts": dict(counts),
        "check_counts": dict(check_counts),
        "input_files": list(
            REQUIRED_INPUT_FILES
        ),
        "input_sha256": before_hashes,
        "limitations": [
            (
                "Existing OOS reports are not "
                "a new independent dataset."
            ),
            (
                "Unknown metric columns are not "
                "automatically interpreted."
            ),
            (
                "No candidate is approved "
                "for live trading."
            ),
            (
                "Repeated signals, censoring, "
                "transaction costs, and regime "
                "stability require further testing."
            ),
            (
                "A separate unseen dataset and "
                "frozen selection rules are required "
                "for genuine independent validation."
            ),
        ],
    }

    log("[WRITE] Independent validation reports")

    prepare_output(
        output_dir,
        args.allow_existing_output,
    )

    write_csv(
        output_dir
        / "independent_validation_summary.csv",
        SUMMARY_FIELDS,
        records,
    )

    write_csv(
        output_dir
        / "independent_validation_checks.csv",
        CHECK_FIELDS,
        checks,
    )

    write_json(
        output_dir
        / "independent_validation_result.json",
        result,
    )

    manifest = build_manifest(
        output_dir,
        before_hashes,
    )

    write_json(
        output_dir
        / "independent_validation_manifest.json",
        manifest,
    )

    verify_output_manifest(
        output_dir
    )

    log("[CHECK] Final production input SHA256")

    verify_input_hashes(
        input_dir,
        before_hashes,
    )

    log("[PASS] Report files generated.")
    log("[PASS] Output SHA256 verified.")
    log("[PASS] Production inputs unchanged.")
    log(
        "[INFO] Independent predictive validation "
        "has NOT been performed."
    )
    log(
        "[INFO] Candidate trading approval "
        "has NOT been determined."
    )

    log("=" * 70)
    log("Candidate Independent Validation Complete")
    log("=" * 70)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        log(
            f"[FAIL] {type(exc).__name__}: {exc}"
        )
        raise
