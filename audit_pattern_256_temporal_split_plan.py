# -*- coding: utf-8 -*-
"""
audit_pattern_256_temporal_split_plan.py

Clean V001

Pattern 256 Temporal Split Planning Audit

PURPOSE
-------
1. Verify OOS readiness evidence.
2. Verify inventory SHA256.
3. Verify OHLCV / Feature / Signal inventory alignment.
4. Generate chronological split candidates.
5. Estimate market coverage using inventory metadata.
6. Identify possible historical research exposure.
7. Preserve existing research datasets.
8. Keep research decision HOLD.

IMPORTANT
---------
This script DOES NOT:
- Train a model.
- Execute a performance backtest.
- Prove independent OOS performance.
- Prove absence of look-ahead bias.
- Approve live trading.

RUN
---
py audit_pattern_256_temporal_split_plan.py --root .
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

TIMEFRAMES = ("h1", "h4", "d1")

DATASETS = (
    "ohlcv",
    "features",
    "signals_256",
)

READINESS_DIR = Path(
    "data/reports/pattern_256_oos_readiness"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_temporal_split_plan"
)

# Retrospective research split candidates.
# These dates are NOT approved independent OOS boundaries.
SPLITS = (
    (
        "DEVELOPMENT",
        "2017-01-01T00:00:00+00:00",
        "2024-01-01T00:00:00+00:00",
    ),
    (
        "VALIDATION",
        "2024-01-01T00:00:00+00:00",
        "2025-07-01T00:00:00+00:00",
    ),
    (
        "RETROSPECTIVE_TEST",
        "2025-07-01T00:00:00+00:00",
        "2026-10-01T00:00:00+00:00",
    ),
)

ALIGNMENT_COLUMNS = (
    "timeframe",
    "market",
    "ohlcv_present",
    "features_present",
    "signals_present",
    "row_counts_equal",
    "first_timestamps_equal",
    "last_timestamps_equal",
    "timestamp_columns_equal",
    "status",
)

COVERAGE_COLUMNS = (
    "timeframe",
    "market",
    "split",
    "split_start_utc",
    "split_end_utc_exclusive",
    "market_first_utc",
    "market_last_utc",
    "date_range_overlap",
    "full_date_range_coverage",
    "actual_rows_in_split_verified",
    "independent_oos_approved",
)


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


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


def write_json(path, value):
    with path.open(
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


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return list(
            csv.DictReader(handle)
        )


def write_csv(path, columns, rows):
    with path.open(
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


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def parse_time(value):
    return datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    ).astimezone(timezone.utc)


def verify_readiness(root):
    report = root / READINESS_DIR

    result_path = (
        report / "oos_readiness_result.json"
    )

    manifest_path = (
        report / "oos_readiness_manifest.json"
    )

    inventory_path = (
        report / "oos_data_inventory.csv"
    )

    for path in (
        result_path,
        manifest_path,
        inventory_path,
    ):
        require(
            path.is_file(),
            f"Required evidence missing: {path}",
        )

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    require(
        result.get("execution") == "PASS",
        "Readiness execution is not PASS",
    )

    require(
        result.get("review_required_files") == 0,
        "Readiness contains files requiring review",
    )

    require(
        result.get("discovered_files")
        == result.get("pass_files"),
        "Readiness file counts do not match",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected research decision",
    )

    require(
        result.get("independent_oos_proven") is False,
        "Unexpected OOS approval",
    )

    require(
        result.get("train_approved") is False,
        "Unexpected TRAIN approval",
    )

    require(
        result.get("live_trading_approved") is False,
        "Unexpected trading approval",
    )

    require(
        manifest.get("execution") == "PASS",
        "Readiness manifest is not PASS",
    )

    expected_hashes = manifest.get(
        "output_sha256",
        {},
    )

    for filename in (
        "oos_data_inventory.csv",
        "oos_readiness_result.json",
    ):
        require(
            filename in expected_hashes,
            f"Missing SHA256: {filename}",
        )

        actual = sha256_file(
            report / filename
        )

        expected = str(
            expected_hashes[filename]
        ).lower()

        require(
            actual == expected,
            f"SHA256 mismatch: {filename}",
        )

    inventory = read_csv(
        inventory_path
    )

    require(
        len(inventory)
        == int(result["discovered_files"]),
        "Inventory row count mismatch",
    )

    for row in inventory:
        require(
            row.get("status") == "PASS",
            (
                "Inventory contains non-PASS file: "
                + str(row.get("file"))
            ),
        )

    return inventory, {
        "readiness_result_sha256": sha256_file(
            result_path
        ),
        "readiness_manifest_sha256": sha256_file(
            manifest_path
        ),
        "inventory_sha256": sha256_file(
            inventory_path
        ),
        "verified_files": len(inventory),
    }


def build_index(inventory):
    index = {}

    for row in inventory:
        key = (
            row["dataset"],
            row["timeframe"],
            row["market"],
        )

        require(
            key not in index,
            f"Duplicate inventory key: {key}",
        )

        index[key] = row

    return index


def compare_alignment(index):
    combinations = sorted({
        (
            timeframe,
            market,
        )
        for _, timeframe, market in index
    })

    rows = []

    for timeframe, market in combinations:
        entries = {
            dataset: index.get(
                (
                    dataset,
                    timeframe,
                    market,
                )
            )
            for dataset in DATASETS
        }

        present = {
            dataset: entries[dataset] is not None
            for dataset in DATASETS
        }

        all_present = all(
            present.values()
        )

        row_counts_equal = False
        first_equal = False
        last_equal = False
        timestamp_columns_equal = False

        if all_present:
            row_counts_equal = (
                len({
                    int(entries[d]["rows"])
                    for d in DATASETS
                }) == 1
            )

            first_equal = (
                len({
                    entries[d]["first_timestamp"]
                    for d in DATASETS
                }) == 1
            )

            last_equal = (
                len({
                    entries[d]["last_timestamp"]
                    for d in DATASETS
                }) == 1
            )

            timestamp_columns_equal = (
                len({
                    entries[d]["timestamp_column"]
                    for d in DATASETS
                }) == 1
            )

        status = (
            "PASS"
            if (
                all_present
                and row_counts_equal
                and first_equal
                and last_equal
                and timestamp_columns_equal
            )
            else "REVIEW_REQUIRED"
        )

        rows.append({
            "timeframe": timeframe,
            "market": market,
            "ohlcv_present": present["ohlcv"],
            "features_present": present["features"],
            "signals_present": present["signals_256"],
            "row_counts_equal": row_counts_equal,
            "first_timestamps_equal": first_equal,
            "last_timestamps_equal": last_equal,
            "timestamp_columns_equal": (
                timestamp_columns_equal
            ),
            "status": status,
        })

    return rows


def build_coverage(index):
    rows = []

    keys = sorted({
        (
            timeframe,
            market,
        )
        for dataset, timeframe, market in index
        if dataset == "ohlcv"
    })

    for timeframe, market in keys:
        source = index[
            (
                "ohlcv",
                timeframe,
                market,
            )
        ]

        first = parse_time(
            source["first_timestamp"]
        )

        last = parse_time(
            source["last_timestamp"]
        )

        for name, start_text, end_text in SPLITS:
            start = parse_time(start_text)
            end = parse_time(end_text)

            require(
                start < end,
                f"Invalid split: {name}",
            )

            # Source endpoints are inclusive.
            # Split end is exclusive.
            overlap = (
                last >= start
                and first < end
            )

            # Metadata only: this does not prove
            # continuous candle coverage.
            full_range = (
                first <= start
                and last >= end
            )

            rows.append({
                "timeframe": timeframe,
                "market": market,
                "split": name,
                "split_start_utc": start_text,
                "split_end_utc_exclusive": end_text,
                "market_first_utc": (
                    source["first_timestamp"]
                ),
                "market_last_utc": (
                    source["last_timestamp"]
                ),
                "date_range_overlap": overlap,
                "full_date_range_coverage": full_range,
                "actual_rows_in_split_verified": False,
                "independent_oos_approved": False,
            })

    return rows


def summarize_coverage(rows):
    summary = {}

    for timeframe in TIMEFRAMES:
        summary[timeframe] = {}

        for split_name, _, _ in SPLITS:
            selected = [
                row
                for row in rows
                if row["timeframe"] == timeframe
                and row["split"] == split_name
            ]

            summary[timeframe][split_name] = {
                "markets": len(selected),
                "markets_with_date_overlap": sum(
                    row["date_range_overlap"]
                    for row in selected
                ),
                "markets_with_full_range_coverage": sum(
                    row["full_date_range_coverage"]
                    for row in selected
                ),
                "actual_candle_counts_verified": False,
            }

    return summary


def run(root):
    root = root.resolve()

    print("=" * 64)
    print("PATTERN 256 TEMPORAL SPLIT PLAN")
    print(f"Version: {VERSION}")
    print(f"Root: {root}")
    print("=" * 64)

    inventory, evidence = verify_readiness(
        root
    )

    print(
        "[PASS] Readiness evidence verified"
    )

    index = build_index(
        inventory
    )

    alignment = compare_alignment(
        index
    )

    coverage = build_coverage(
        index
    )

    alignment_issues = sum(
        row["status"] != "PASS"
        for row in alignment
    )

    summary = summarize_coverage(
        coverage
    )

    output = root / OUTPUT_DIR

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    alignment_path = (
        output / "temporal_alignment.csv"
    )

    coverage_path = (
        output / "temporal_coverage.csv"
    )

    result_path = (
        output / "temporal_split_result.json"
    )

    manifest_path = (
        output / "temporal_split_manifest.json"
    )

    write_csv(
        alignment_path,
        ALIGNMENT_COLUMNS,
        alignment,
    )

    write_csv(
        coverage_path,
        COVERAGE_COLUMNS,
        coverage,
    )

    result = {
        "audit": "pattern_256_temporal_split_plan",
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": (
            "PASS"
            if alignment_issues == 0
            else "REVIEW_REQUIRED"
        ),
        "readiness_evidence_verified": True,
        "readiness_evidence": evidence,
        "market_timeframe_combinations": len(
            alignment
        ),
        "aligned_combinations": (
            len(alignment) - alignment_issues
        ),
        "alignment_issues": alignment_issues,
        "coverage_rows": len(coverage),
        "split_candidates": [
            {
                "name": name,
                "start_utc": start,
                "end_utc_exclusive": end,
            }
            for name, start, end in SPLITS
        ],
        "coverage_summary": summary,
        "split_type": (
            "RETROSPECTIVE_RESEARCH_CANDIDATES"
        ),
        "split_dates_approved": False,
        "actual_split_candle_counts_verified": False,
        "per_row_timestamp_alignment_proven": False,
        "historical_research_overlap_checked": False,
        "historical_research_exposure_status": "UNKNOWN",
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            (
                "Date ranges are retrospective "
                "research candidates, not independent OOS."
            ),
            (
                "Inventory metadata cannot establish "
                "per-row timestamp alignment."
            ),
            (
                "Date range overlap does not prove "
                "continuous candle coverage."
            ),
            (
                "Prior research exposure has not "
                "been independently reconstructed."
            ),
            (
                "No training, prediction or "
                "performance evaluation is performed."
            ),
        ],
    }

    write_json(
        result_path,
        result
    )

    manifest = {
        "audit": "pattern_256_temporal_split_plan",
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": result["execution"],
        "input_sha256": evidence,
        "output_sha256": {
            "temporal_alignment.csv": sha256_file(
                alignment_path
            ),
            "temporal_coverage.csv": sha256_file(
                coverage_path
            ),
            "temporal_split_result.json": sha256_file(
                result_path
            ),
        },
    }

    write_json(
        manifest_path,
        manifest
    )

    print()
    print("===== RESULT =====")
    print(
        "Market/timeframe combinations:",
        len(alignment),
    )
    print(
        "Aligned combinations:",
        len(alignment) - alignment_issues,
    )
    print(
        "Alignment issues:",
        alignment_issues,
    )
    print(
        "Coverage rows:",
        len(coverage),
    )
    print(
        "Execution:",
        result["execution"],
    )
    print(
        "Independent OOS proven: False"
    )
    print(
        "Research decision: HOLD"
    )

    print()
    print("===== OUTPUT =====")
    print(alignment_path)
    print(coverage_path)
    print(result_path)
    print(manifest_path)

    return 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 temporal split "
            "planning audit"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
        help="Persistent research root",
    )

    args = parser.parse_args()

    try:
        return run(
            Path(args.root)
        )

    except Exception as exc:
        print(
            (
                f"[FAIL] "
                f"{type(exc).__name__}: {exc}"
            ),
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
