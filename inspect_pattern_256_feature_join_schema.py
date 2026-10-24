#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Feature Join Schema Gate
Clean V001

Read-only feature schema inventory.

Does not:
- Modify source data
- Join event and feature rows
- Train a model
- Enable automated trading
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

TRAIN_DIR = Path(
    "data/reports/pattern_256_train_only"
)

AUDIT_DIR = Path(
    "data/reports/pattern_256_feature_integrity"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_feature_join_schema"
)

RESULT_NAME = "feature_join_schema_result.json"

MANIFEST_NAME = "feature_join_schema_manifest.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def load_json(path: Path) -> dict:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        value = json.load(handle)

    if not isinstance(value, dict):
        raise ValueError(
            f"Expected JSON object: {path}"
        )

    return value


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=".schema_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                value,
                handle,
                indent=2,
                ensure_ascii=False,
                sort_keys=True,
            )

            handle.write("\n")

        os.replace(
            temporary,
            path,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def csv_header(path: Path) -> list[str]:
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.reader(handle)

        return next(reader, [])


def csv_sample(
    path: Path,
    count: int = 2,
) -> list[dict]:
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        rows = []

        for index, row in enumerate(reader):
            if index >= count:
                break

            rows.append(
                {
                    key: (
                        value[:100]
                        if value
                        else ""
                    )
                    for key, value in row.items()
                }
            )

        return rows


def find_feature_csvs(
    root: Path,
    max_files: int,
) -> list[Path]:

    search_roots = [
        root / "data/features",
        root / "data/feature_candidates",
        root / "data/reports/pattern_256_feature_candidate",
        root / "data/reports/pattern_256_feature_candidates",
    ]

    discovered = set()

    for directory in search_roots:
        if not directory.is_dir():
            continue

        for path in directory.rglob("*.csv"):
            if not path.is_file():
                continue

            discovered.add(
                path.resolve()
            )

            if len(discovered) > max_files:
                raise RuntimeError(
                    "Feature CSV inventory exceeds "
                    "--max-files"
                )

    return sorted(discovered)


def run(args: argparse.Namespace) -> None:

    root = Path(
        args.root
    ).expanduser().resolve()

    train_csv = (
        root
        / TRAIN_DIR
        / "train_only_events.csv"
    )

    train_result_path = (
        root
        / TRAIN_DIR
        / "train_only_result.json"
    )

    train_manifest_path = (
        root
        / TRAIN_DIR
        / "train_only_manifest.json"
    )

    audit_result_path = (
        root
        / AUDIT_DIR
        / "feature_integrity_summary.json"
    )

    audit_markets_path = (
        root
        / AUDIT_DIR
        / "feature_integrity_markets.csv"
    )

    required_files = [
        train_csv,
        train_result_path,
        train_manifest_path,
        audit_result_path,
        audit_markets_path,
    ]

    for path in required_files:
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing source: {path}"
            )

    train_result = load_json(
        train_result_path
    )

    train_manifest = load_json(
        train_manifest_path
    )

    audit_result = load_json(
        audit_result_path
    )

    if train_result.get(
        "execution_status"
    ) != "PASS":
        raise ValueError(
            "TRAIN preparation is not PASS"
        )

    if train_result.get(
        "train_dataset_prepared"
    ) is not True:
        raise ValueError(
            "TRAIN dataset not prepared"
        )

    if audit_result.get(
        "execution_status"
    ) != "PASS":
        raise ValueError(
            "Feature integrity audit is not PASS"
        )

    if audit_result.get(
        "feature_integrity_verified"
    ) is not True:
        raise ValueError(
            "Feature integrity not verified"
        )

    if train_result.get(
        "approved_for_live_trading"
    ) is not False:
        raise ValueError(
            "Unexpected trading approval"
        )

    expected_train_hash = (
        train_manifest
        .get("output_sha256", {})
        .get("train_only_events.csv")
    )

    if sha256_file(
        train_csv
    ) != expected_train_hash:
        raise ValueError(
            "TRAIN CSV SHA256 mismatch"
        )

    expected_result_hash = (
        train_manifest
        .get("output_sha256", {})
        .get("train_only_result.json")
    )

    if sha256_file(
        train_result_path
    ) != expected_result_hash:
        raise ValueError(
            "TRAIN result SHA256 mismatch"
        )

    feature_files = find_feature_csvs(
        root,
        args.max_files,
    )

    schema_groups = {}
    file_samples = []

    for index, path in enumerate(
        feature_files,
        start=1,
    ):
        columns = csv_header(
            path
        )

        if not columns:
            raise ValueError(
                f"Empty CSV header: {path}"
            )

        if len(columns) != len(set(columns)):
            raise ValueError(
                f"Duplicate CSV columns: {path}"
            )

        schema_key = "\x1f".join(
            columns
        )

        if schema_key not in schema_groups:
            schema_groups[schema_key] = {
                "columns": columns,
                "files": 0,
                "example": str(
                    path.relative_to(root)
                ),
            }

        schema_groups[
            schema_key
        ]["files"] += 1

        if index <= args.sample_files:
            file_samples.append(
                {
                    "path": str(
                        path.relative_to(root)
                    ),
                    "bytes": path.stat().st_size,
                    "columns": columns,
                    "sample_rows": csv_sample(
                        path
                    ),
                }
            )

        if index % 100 == 0:
            print(
                f"[SCHEMA] "
                f"{index}/{len(feature_files)}",
                flush=True,
            )

    schema_list = sorted(
        schema_groups.values(),
        key=lambda item: (
            -item["files"],
            item["example"],
        ),
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "generated_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "train_rows": train_result.get(
            "selected_rows"
        ),
        "train_cutoff": train_result.get(
            "train_cutoff"
        ),
        "feature_files_discovered": len(
            feature_files
        ),
        "feature_schema_variants": len(
            schema_list
        ),
        "feature_schemas": schema_list,
        "feature_file_samples": file_samples,
        "train_columns": csv_header(
            train_csv
        ),
        "train_sample_rows": csv_sample(
            train_csv
        ),
        "integrity_market_columns": csv_header(
            audit_markets_path
        ),
        "integrity_market_samples": csv_sample(
            audit_markets_path
        ),
        "join_completed": False,
        "join_readiness_proven": False,
        "train_experiment_completed": False,
        "research_decision": "HOLD",
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "automated_trading_enabled": False,
        "next_stage": (
            "IMPLEMENT_VERIFIED_FEATURE_EVENT_JOIN"
        ),
        "limitations": [
            "Schema inspection is not a feature join.",
            "Feature timestamp semantics require verification.",
            "Gap eligibility requires row-level verification.",
            "No model training was performed.",
            "No independent OOS validation was performed.",
        ],
    }

    output_dir = (
        root / OUTPUT_DIR
    )

    result_path = (
        output_dir / RESULT_NAME
    )

    manifest_path = (
        output_dir / MANIFEST_NAME
    )

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "input_sha256": {
            "train_only_events.csv": sha256_file(
                train_csv
            ),
            "train_only_result.json": sha256_file(
                train_result_path
            ),
            "train_only_manifest.json": sha256_file(
                train_manifest_path
            ),
            "feature_integrity_summary.json": sha256_file(
                audit_result_path
            ),
            "feature_integrity_markets.csv": sha256_file(
                audit_markets_path
            ),
        },
        "output_sha256": {
            RESULT_NAME: sha256_file(
                result_path
            ),
        },
        "feature_files_discovered": len(
            feature_files
        ),
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    if sha256_file(
        result_path
    ) != load_json(
        manifest_path
    )["output_sha256"][RESULT_NAME]:
        raise RuntimeError(
            "Output SHA256 verification failed"
        )

    print(
        "PASS: Feature schema inventory"
    )

    print(
        f"Feature files: {len(feature_files)}"
    )

    print(
        f"Schema variants: {len(schema_list)}"
    )

    print(
        "Join completed: False"
    )

    print(
        "Research decision: HOLD"
    )

    print(
        f"Output: {output_dir}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 Feature Join Schema Gate"
        )
    )

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--max-files",
        type=int,
        default=2000,
    )

    parser.add_argument(
        "--sample-files",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    try:
        run(args)
        return 0

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
