
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Source Qualification
Clean V001

Purpose:
    Qualify event-level source candidates using a previously
    verified inventory_files.csv.

Safety:
    - Read-only input inspection
    - No OHLCV modification
    - No training approval
    - No live trading approval
    - No claim of event-level attribution
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
AUDIT_NAME = "pattern_256_event_source_qualification"

TARGETS = {
    "EVENT_GAP_INTEGRITY":
        "data/reports/pattern_256_event_gap_integrity/"
        "gap_integrity_events.csv",

    "INDEPENDENT_DETAIL":
        "data/reports/pattern_256_accumulation_independent/"
        "independent_detail.csv",

    "VALIDATION_DETAIL":
        "data/reports/pattern_256_accumulation_30pct/"
        "validation_detail.csv",

    "TRAIN_ONLY_EVENTS":
        "data/reports/pattern_256_train_only/"
        "train_only_events.csv",

    "CENSORING_MARKET":
        "data/reports/pattern_256_censoring_sensitivity/"
        "censoring_market.csv",
}

KEY_COLUMNS = {
    "timeframe",
    "market",
}

TIME_COLUMNS = {
    "signal_timestamp",
    "signal_utc",
}

ENTRY_COLUMNS = {
    "entry_timestamp",
    "entry_utc",
}

OUTCOME_COLUMNS = {
    "status",
    "original_outcome",
    "corrected_outcome",
    "hit_30pct",
}

GAP_COLUMNS = {
    "missing_candles",
    "first_gap_utc",
    "full_window_contiguous",
}

HORIZON_COLUMNS = {
    "horizon",
    "horizon_hours",
}


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def write_json(path, data):
    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )


def write_csv(path, rows, fields):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=fields,
        )
        writer.writeheader()
        writer.writerows(rows)


def load_inventory(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        required = {
            "relative_path",
            "size_bytes",
            "sha256",
            "columns",
            "status",
        }

        if not required.issubset(
            set(reader.fieldnames or [])
        ):
            raise RuntimeError(
                "Inventory schema mismatch"
            )

        records = {}

        for row in reader:
            key = row["relative_path"].replace(
                "\\", "/"
            )

            if key in records:
                raise RuntimeError(
                    f"Duplicate inventory path: {key}"
                )

            records[key] = row

    return records


def inspect_sample(path, max_rows):
    """
    Read a bounded sample only.
    This is not a full-row audit.
    """

    count = 0
    markets = set()
    timeframes = set()
    outcomes = Counter()
    missing_values = Counter()

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        for row in reader:
            if count >= max_rows:
                break

            count += 1

            market = row.get("market", "").strip()
            timeframe = row.get(
                "timeframe", ""
            ).strip()

            if market:
                markets.add(market)

            if timeframe:
                timeframes.add(timeframe)

            for column in OUTCOME_COLUMNS:
                if column in row:
                    value = row[column].strip()

                    if value:
                        outcomes[
                            f"{column}:{value}"
                        ] += 1

            for column in row:
                if not row[column].strip():
                    missing_values[column] += 1

    return {
        "sample_rows": count,
        "sample_market_count": len(markets),
        "sample_timeframes": sorted(timeframes),
        "sample_outcomes": dict(
            outcomes.most_common(20)
        ),
        "sample_missing_values": dict(
            missing_values.most_common(15)
        ),
    }


def classify(columns):
    column_set = set(columns)

    has_keys = KEY_COLUMNS.issubset(
        column_set
    )

    has_signal = bool(
        column_set & TIME_COLUMNS
    )

    has_entry = bool(
        column_set & ENTRY_COLUMNS
    )

    has_outcome = bool(
        column_set & OUTCOME_COLUMNS
    )

    has_gap = bool(
        column_set & GAP_COLUMNS
    )

    has_horizon = bool(
        column_set & HORIZON_COLUMNS
    )

    if (
        has_keys
        and has_signal
        and has_entry
        and has_outcome
        and has_horizon
    ):
        qualification = "EVENT_OUTCOME_CANDIDATE"

    elif (
        has_keys
        and has_signal
        and has_entry
        and has_gap
    ):
        qualification = "EVENT_GAP_CANDIDATE"

    elif has_keys and has_outcome:
        qualification = "AGGREGATE_OR_EVENT_CANDIDATE"

    else:
        qualification = "INSUFFICIENT_SCHEMA"

    return qualification


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--inventory",
        required=True,
    )

    parser.add_argument(
        "--output",
        required=True,
    )

    parser.add_argument(
        "--sample-rows",
        type=int,
        default=2000,
    )

    args = parser.parse_args()

    if not 1 <= args.sample_rows <= 10000:
        raise RuntimeError(
            "sample-rows must be 1..10000"
        )

    root = Path(args.root).resolve()
    inventory_path = Path(
        args.inventory
    ).resolve()

    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError(
            f"Data root missing: {root}"
        )

    if not inventory_path.is_file():
        raise RuntimeError(
            f"Inventory missing: {inventory_path}"
        )

    # Output is independent of research source data.
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    inventory = load_inventory(
        inventory_path
    )

    results = []
    details = []
    errors = []

    for role, relative_path in TARGETS.items():
        record = inventory.get(relative_path)

        if record is None:
            errors.append(
                f"Inventory target missing: {relative_path}"
            )
            continue

        path = (
            root / Path(relative_path)
        ).resolve()

        try:
            path.relative_to(root)
        except ValueError:
            errors.append(
                f"Unsafe path: {relative_path}"
            )
            continue

        if not path.is_file():
            errors.append(
                f"Source file missing: {relative_path}"
            )
            continue

        try:
            expected_size = int(
                record["size_bytes"]
            )

            actual_size = path.stat().st_size

            if expected_size != actual_size:
                raise RuntimeError(
                    "Source size differs from inventory"
                )

            columns = json.loads(
                record["columns"]
            )

            if not isinstance(columns, list):
                raise RuntimeError(
                    "Invalid inventory columns"
                )

            with path.open(
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as stream:
                reader = csv.reader(stream)
                current_columns = next(
                    reader,
                    [],
                )

            if current_columns != columns:
                raise RuntimeError(
                    "Source columns differ from inventory"
                )

            if record["status"] != "OK":
                raise RuntimeError(
                    "Inventory status is not OK"
                )

            qualification = classify(
                columns
            )

            sample = inspect_sample(
                path,
                args.sample_rows,
            )

            results.append({
                "role": role,
                "relative_path": relative_path,
                "size_bytes": actual_size,
                "column_count": len(columns),
                "qualification": qualification,
                "sample_rows": sample[
                    "sample_rows"
                ],
                "sample_market_count": sample[
                    "sample_market_count"
                ],
                "sample_timeframes": "|".join(
                    sample["sample_timeframes"]
                ),
                "source_sha256_from_inventory":
                    record["sha256"],
                "full_source_sha256_rechecked":
                    False,
                "status": "SCHEMA_SAMPLE_PASS",
            })

            details.append({
                "role": role,
                "path": relative_path,
                "columns": columns,
                "sample": sample,
                "qualification": qualification,
                "limitations": [
                    "Only a bounded sample was inspected",
                    "Full source SHA256 was not recomputed",
                    "No event-level join was performed",
                ],
            })

            print(
                f"[OK] {role}: "
                f"{qualification}, "
                f"sample={sample['sample_rows']}",
                flush=True,
            )

        except Exception as exc:
            errors.append(
                f"{relative_path}: {exc}"
            )

    result = {
        "audit": AUDIT_NAME,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": (
            "PASS" if not errors else "FAIL"
        ),
        "inventory_csv_sha256":
            sha256_file(inventory_path),
        "target_count": len(TARGETS),
        "qualified_files": len(results),
        "errors": errors,
        "details": details,
        "event_level_attribution_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    csv_path = output / (
        "qualification_sources.csv"
    )

    json_path = output / (
        "qualification_result.json"
    )

    manifest_path = output / (
        "qualification_manifest.json"
    )

    fields = [
        "role",
        "relative_path",
        "size_bytes",
        "column_count",
        "qualification",
        "sample_rows",
        "sample_market_count",
        "sample_timeframes",
        "source_sha256_from_inventory",
        "full_source_sha256_rechecked",
        "status",
    ]

    write_csv(
        csv_path,
        results,
        fields,
    )

    write_json(
        json_path,
        result,
    )

    manifest = {
        "audit": AUDIT_NAME,
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": sha256_file(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            csv_path.name: sha256_file(
                csv_path
            ),
            json_path.name: sha256_file(
                json_path
            ),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print(
        json.dumps({
            "execution": result["execution"],
            "targets": len(TARGETS),
            "qualified": len(results),
            "errors": errors,
            "research_decision": "HOLD",
        }, ensure_ascii=False, indent=2),
        flush=True,
    )

    return 0 if not errors else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
