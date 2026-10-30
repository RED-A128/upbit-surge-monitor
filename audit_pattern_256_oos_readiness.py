# -*- coding: utf-8 -*-
"""
audit_pattern_256_oos_readiness.py
Clean V001

Pattern 256 Independent OOS Readiness Audit

Purpose
-------
1. Verify previous formula-risk audit evidence.
2. Inventory OHLCV / Feature / Signal 256 CSV files.
3. Inspect timestamp columns and chronological ordering.
4. Detect duplicate timestamps.
5. Identify candidate time boundaries.
6. Record schema information needed for independent OOS.
7. Never modify existing source datasets.
8. Never claim OOS performance has been proven.

Usage
-----
py audit_pattern_256_oos_readiness.py --root .

This is a readiness audit, NOT an OOS performance backtest.
"""

import argparse
import csv
import hashlib
import json
import sys

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


VERSION = "Clean V001"

TIMEFRAMES = ("h1", "h4", "d1")

DATASETS = {
    "ohlcv": Path("data/ohlcv"),
    "features": Path("data/features"),
    "signals_256": Path("data/signals/256"),
}

FORMULA_REPORT = Path(
    "data/reports/pattern_256_formula_risk"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_oos_readiness"
)

TIMESTAMP_CANDIDATES = (
    "timestamp",
    "datetime",
    "date",
    "time",
    "candle_date_time_utc",
    "candle_date_time_kst",
    "timestamp_utc",
    "open_time",
)

INVENTORY_COLUMNS = (
    "dataset",
    "timeframe",
    "market",
    "file",
    "file_sha256",
    "rows",
    "columns",
    "timestamp_column",
    "timestamp_valid_rows",
    "timestamp_invalid_rows",
    "duplicate_timestamps",
    "chronologically_sorted",
    "first_timestamp",
    "last_timestamp",
    "status",
    "error",
)


def now_utc():
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


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def save_json(path, data):
    with path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")


def save_csv(path, rows, columns):
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


def verify_formula_evidence(root):
    report_dir = root / FORMULA_REPORT

    result_path = (
        report_dir / "formula_risk_result.json"
    )

    manifest_path = (
        report_dir / "formula_risk_manifest.json"
    )

    require(
        result_path.is_file(),
        f"Missing formula result: {result_path}",
    )

    require(
        manifest_path.is_file(),
        f"Missing formula manifest: {manifest_path}",
    )

    result = load_json(result_path)
    manifest = load_json(manifest_path)

    require(
        result.get("execution") == "PASS",
        "Formula audit execution is not PASS",
    )

    require(
        result.get("replay_evidence_verified") is True,
        "Replay evidence was not verified",
    )

    require(
        result.get("source_sha256_verified") is True,
        "Formula source SHA256 was not verified",
    )

    require(
        result.get("high_risk_items") == 0,
        "Formula audit contains HIGH findings",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected research decision",
    )

    require(
        result.get("train_approved") is False,
        "Unexpected TRAIN approval",
    )

    require(
        result.get("live_trading_approved") is False,
        "Unexpected trading approval",
    )

    output_hashes = manifest.get(
        "output_sha256",
        {},
    )

    require(
        isinstance(output_hashes, dict),
        "Invalid formula output hash manifest",
    )

    required_outputs = (
        "formula_risks.csv",
        "formula_risk_result.json",
    )

    for filename in required_outputs:
        expected = output_hashes.get(filename)

        require(
            isinstance(expected, str)
            and len(expected) == 64,
            f"Missing SHA256 for {filename}",
        )

        path = report_dir / filename

        require(
            path.is_file(),
            f"Missing formula output: {path}",
        )

        require(
            sha256_file(path) == expected.lower(),
            f"Formula output SHA256 mismatch: {filename}",
        )

    source_hashes = manifest.get(
        "source_sha256",
        {},
    )

    for filename in (
        "build_features.py",
        "detect_256.py",
    ):
        expected = source_hashes.get(filename)

        require(
            isinstance(expected, str)
            and len(expected) == 64,
            f"Missing source SHA256: {filename}",
        )

        path = root / filename

        require(
            path.is_file(),
            f"Missing calculation script: {path}",
        )

        require(
            sha256_file(path) == expected.lower(),
            f"Calculation script changed: {filename}",
        )

    return {
        "verified": True,
        "formula_manifest_sha256": sha256_file(
            manifest_path
        ),
        "high_risk_items": result["high_risk_items"],
        "review_items": result["review_items"],
        "research_decision": "HOLD",
    }


def find_timestamp_column(columns):
    normalized = {
        str(column).strip().lower(): column
        for column in columns
    }

    for candidate in TIMESTAMP_CANDIDATES:
        if candidate in normalized:
            return normalized[candidate]

    return None


def parse_timestamps(series):
    """
    Parse timestamp values without modifying the source file.

    Numeric values are interpreted as Unix seconds or milliseconds
    using a magnitude-based distinction.
    """

    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    nonempty = series.notna().sum()

    if nonempty > 0 and numeric.notna().sum() == nonempty:
        median = numeric.dropna().abs().median()

        unit = "ms" if median >= 1e11 else "s"

        return pd.to_datetime(
            numeric,
            unit=unit,
            utc=True,
            errors="coerce",
        )

    return pd.to_datetime(
        series,
        utc=True,
        errors="coerce",
    )


def inspect_file(root, dataset, timeframe, path):
    relative = path.relative_to(root)

    record = {
        "dataset": dataset,
        "timeframe": timeframe,
        "market": path.stem,
        "file": str(relative),
        "file_sha256": "",
        "rows": 0,
        "columns": "",
        "timestamp_column": "",
        "timestamp_valid_rows": 0,
        "timestamp_invalid_rows": 0,
        "duplicate_timestamps": 0,
        "chronologically_sorted": False,
        "first_timestamp": "",
        "last_timestamp": "",
        "status": "ERROR",
        "error": "",
    }

    try:
        record["file_sha256"] = sha256_file(path)

        frame = pd.read_csv(
            path,
            encoding="utf-8-sig",
            low_memory=False,
        )

        record["rows"] = len(frame)

        columns = list(frame.columns)

        record["columns"] = json.dumps(
            columns,
            ensure_ascii=False,
        )

        if frame.empty:
            record["status"] = "EMPTY"
            return record

        timestamp_column = find_timestamp_column(
            columns
        )

        if timestamp_column is None:
            record["status"] = "NO_TIMESTAMP_COLUMN"
            return record

        record["timestamp_column"] = (
            timestamp_column
        )

        timestamps = parse_timestamps(
            frame[timestamp_column]
        )

        valid = timestamps.notna()

        record["timestamp_valid_rows"] = int(
            valid.sum()
        )

        record["timestamp_invalid_rows"] = int(
            (~valid).sum()
        )

        valid_timestamps = timestamps[valid]

        if valid_timestamps.empty:
            record["status"] = "INVALID_TIMESTAMPS"
            return record

        record["duplicate_timestamps"] = int(
            valid_timestamps.duplicated().sum()
        )

        record["chronologically_sorted"] = bool(
            valid_timestamps.is_monotonic_increasing
        )

        record["first_timestamp"] = (
            valid_timestamps.min().isoformat()
        )

        record["last_timestamp"] = (
            valid_timestamps.max().isoformat()
        )

        if record["timestamp_invalid_rows"] > 0:
            record["status"] = "INVALID_TIMESTAMPS"

        elif record["duplicate_timestamps"] > 0:
            record["status"] = "DUPLICATE_TIMESTAMPS"

        elif not record["chronologically_sorted"]:
            record["status"] = "UNSORTED"

        else:
            record["status"] = "PASS"

    except Exception as exc:
        record["status"] = "ERROR"
        record["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    return record


def discover_files(root):
    discovered = []

    for dataset, base in DATASETS.items():
        for timeframe in TIMEFRAMES:
            directory = root / base / timeframe

            require(
                directory.is_dir(),
                f"Missing data directory: {directory}",
            )

            files = sorted(
                directory.glob("KRW-*.csv")
            )

            require(
                len(files) > 0,
                f"No KRW CSV files: {directory}",
            )

            for path in files:
                discovered.append(
                    (dataset, timeframe, path)
                )

    return discovered


def summarize(inventory):
    statuses = Counter(
        item["status"]
        for item in inventory
    )

    datasets = {}

    for dataset in DATASETS:
        datasets[dataset] = {}

        for timeframe in TIMEFRAMES:
            selected = [
                item
                for item in inventory
                if item["dataset"] == dataset
                and item["timeframe"] == timeframe
            ]

            timestamps = [
                item["first_timestamp"]
                for item in selected
                if item["first_timestamp"]
            ]

            end_timestamps = [
                item["last_timestamp"]
                for item in selected
                if item["last_timestamp"]
            ]

            datasets[dataset][timeframe] = {
                "files": len(selected),
                "rows": sum(
                    item["rows"]
                    for item in selected
                ),
                "pass_files": sum(
                    item["status"] == "PASS"
                    for item in selected
                ),
                "first_timestamp": (
                    min(timestamps)
                    if timestamps
                    else None
                ),
                "last_timestamp": (
                    max(end_timestamps)
                    if end_timestamps
                    else None
                ),
            }

    return {
        "status_counts": dict(statuses),
        "datasets": datasets,
    }


def run(root):
    root = root.resolve()

    print("=" * 64)
    print("PATTERN 256 OOS READINESS AUDIT")
    print(f"Version: {VERSION}")
    print(f"Root: {root}")
    print("=" * 64)

    evidence = verify_formula_evidence(root)

    print("[PASS] Formula audit evidence verified")
    print("[PASS] Calculation script SHA256 verified")

    discovered = discover_files(root)

    print(
        f"[INFO] Discovered CSV files: {len(discovered)}"
    )

    inventory = []

    for index, (dataset, timeframe, path) in enumerate(
        discovered,
        start=1,
    ):
        record = inspect_file(
            root,
            dataset,
            timeframe,
            path,
        )

        inventory.append(record)

        if index % 50 == 0 or index == len(discovered):
            print(
                f"[PROGRESS] {index}/{len(discovered)} "
                f"| latest={record['status']}",
                flush=True,
            )

    summary = summarize(inventory)

    output_dir = root / OUTPUT_DIR

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    inventory_path = (
        output_dir / "oos_data_inventory.csv"
    )

    result_path = (
        output_dir / "oos_readiness_result.json"
    )

    manifest_path = (
        output_dir / "oos_readiness_manifest.json"
    )

    save_csv(
        inventory_path,
        inventory,
        INVENTORY_COLUMNS,
    )

    nonpass = sum(
        item["status"] != "PASS"
        for item in inventory
    )

    result = {
        "audit": "pattern_256_oos_readiness",
        "version": VERSION,
        "created_utc": now_utc(),
        "execution": (
            "PASS"
            if nonpass == 0
            else "REVIEW_REQUIRED"
        ),
        "formula_evidence": evidence,
        "discovered_files": len(discovered),
        "pass_files": len(discovered) - nonpass,
        "review_required_files": nonpass,
        "summary": summary,
        "temporal_split_defined": False,
        "historical_research_overlap_checked": False,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "This audit inventories data; it does not train a model.",
            "This audit does not calculate prediction performance.",
            "No independent OOS period is approved by this audit.",
            "Prior research exposure must be reviewed before defining OOS.",
            "A clean inventory does not prove absence of data leakage.",
        ],
    }

    save_json(
        result_path,
        result,
    )

    manifest = {
        "audit": "pattern_256_oos_readiness",
        "version": VERSION,
        "created_utc": now_utc(),
        "execution": result["execution"],
        "output_sha256": {
            "oos_data_inventory.csv": sha256_file(
                inventory_path
            ),
            "oos_readiness_result.json": sha256_file(
                result_path
            ),
        },
        "formula_manifest_sha256": evidence[
            "formula_manifest_sha256"
        ],
    }

    save_json(
        manifest_path,
        manifest,
    )

    print()
    print("===== AUDIT RESULT =====")
    print(
        "Discovered files:",
        len(discovered),
    )
    print(
        "PASS files:",
        len(discovered) - nonpass,
    )
    print(
        "Review required:",
        nonpass,
    )
    print(
        "Execution:",
        result["execution"],
    )
    print("Research decision: HOLD")
    print("Independent OOS proven: False")

    print()
    print("===== OUTPUT FILES =====")
    print(inventory_path)
    print(result_path)
    print(manifest_path)

    # REVIEW_REQUIRED is an expected research outcome,
    # not a script execution failure.
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Pattern 256 OOS readiness audit"
    )

    parser.add_argument(
        "--root",
        default=".",
        help="Persistent research data root",
    )

    args = parser.parse_args()

    try:
        return run(Path(args.root))

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
