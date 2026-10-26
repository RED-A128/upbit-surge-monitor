#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Delayed Entry Timing Audit
Clean V001

Purpose:
- Recheck every JOIN row, not just the first 1,000 reported issues.
- Investigate delayed entries after a signal candle.
- Identify whether entry timestamps align with timeframe boundaries.
- Separate plausible delayed entry from timing contradictions.
- Verify source audit SHA256 where available.
- Never modify source datasets.
- Never approve TRAIN or live trading automatically.
"""

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path


VERSION = "Clean V001"

PERIOD_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

SHARD_DIRECTORY = (
    "data/reports/"
    "pattern_256_train_feature_join/shards"
)

SOURCE_RESULT = (
    "data/reports/"
    "pattern_256_signal_timestamp_convention/"
    "signal_timestamp_convention_result.json"
)

SOURCE_MANIFEST = (
    "data/reports/"
    "pattern_256_signal_timestamp_convention/"
    "signal_timestamp_convention_manifest.json"
)

OUTPUT_DIRECTORY = (
    "data/reports/"
    "pattern_256_delayed_entry_timing"
)

REQUIRED_COLUMNS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "feature_candle_timestamp",
    "feature_candle_end_timestamp",
]

CLASSIFICATIONS = [
    "NORMAL_NEXT_OPEN",
    "DELAYED_ALIGNED_ENTRY",
    "DELAYED_UNALIGNED_ENTRY",
    "ENTRY_BEFORE_FEATURE_CLOSE",
    "FEATURE_END_MISMATCH",
    "INVALID_TIMESTAMP",
    "UNKNOWN_TIMEFRAME",
]


def file_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def parse_timestamp(value):
    if not value:
        raise ValueError("Empty timestamp")

    parsed = datetime.fromisoformat(
        str(value).strip().replace(
            "Z",
            "+00:00",
        )
    )

    if parsed.tzinfo is None:
        raise ValueError("Timezone missing")

    return parsed.astimezone(timezone.utc)


def save_json_atomic(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        suffix=".json",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                data,
                handle,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )

            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(
            temporary,
            path,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def inspect_row(row):
    timeframe = row["timeframe"]

    if timeframe not in PERIOD_SECONDS:
        return (
            "UNKNOWN_TIMEFRAME",
            None,
        )

    seconds = PERIOD_SECONDS[timeframe]

    try:
        signal = parse_timestamp(
            row["signal_timestamp"]
        )

        entry = parse_timestamp(
            row["entry_timestamp"]
        )

        feature_start = parse_timestamp(
            row["feature_candle_timestamp"]
        )

        feature_end = parse_timestamp(
            row["feature_candle_end_timestamp"]
        )

    except (ValueError, TypeError):
        return (
            "INVALID_TIMESTAMP",
            None,
        )

    if (
        feature_start >= feature_end
        or signal >= entry
    ):
        return (
            "INVALID_TIMESTAMP",
            None,
        )

    expected_end = feature_start + timedelta(
        seconds=seconds
    )

    if feature_end != expected_end:
        return (
            "FEATURE_END_MISMATCH",
            None,
        )

    if entry < feature_end:
        return (
            "ENTRY_BEFORE_FEATURE_CLOSE",
            None,
        )

    # This audit expects the recorded signal timestamp
    # to identify the opening of the feature candle.
    # Any other convention needs separate upstream review.
    if signal != feature_start:
        return (
            "DELAYED_UNALIGNED_ENTRY",
            None,
        )

    delay_seconds = (
        entry - feature_end
    ).total_seconds()

    if delay_seconds == 0:
        return (
            "NORMAL_NEXT_OPEN",
            0,
        )

    if delay_seconds < 0:
        return (
            "ENTRY_BEFORE_FEATURE_CLOSE",
            None,
        )

    # Delays of complete timeframe periods are compatible
    # with later candle openings. This does not establish
    # that an actual valid candle existed at that timestamp.
    periods = delay_seconds / seconds

    if periods.is_integer():
        return (
            "DELAYED_ALIGNED_ENTRY",
            int(periods),
        )

    return (
        "DELAYED_UNALIGNED_ENTRY",
        None,
    )


def verify_previous_outputs(
    root,
    previous_manifest,
):
    expected = previous_manifest.get(
        "output_sha256",
        {},
    )

    if not isinstance(expected, dict):
        raise ValueError(
            "Invalid previous output manifest"
        )

    previous_dir = (
        root
        / "data/reports/"
        "pattern_256_signal_timestamp_convention"
    )

    for filename, expected_hash in expected.items():
        if (
            Path(filename).name != filename
            or not filename
        ):
            raise ValueError(
                "Unsafe manifest filename"
            )

        path = previous_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Missing previous audit output: {path}"
            )

        actual = file_sha256(path)

        if actual.lower() != str(
            expected_hash
        ).lower():
            raise ValueError(
                f"Previous audit SHA256 mismatch: {path}"
            )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--max-issues",
        type=int,
        default=1000,
    )

    args = parser.parse_args()

    if args.max_issues < 0:
        raise ValueError(
            "max-issues must be >= 0"
        )

    root = Path(
        args.root
    ).expanduser().resolve()

    shard_root = root / SHARD_DIRECTORY
    output_root = root / OUTPUT_DIRECTORY

    previous_result_path = root / SOURCE_RESULT
    previous_manifest_path = root / SOURCE_MANIFEST

    for path in (
        previous_result_path,
        previous_manifest_path,
    ):
        if not path.is_file():
            raise FileNotFoundError(
                f"Required file missing: {path}"
            )

    previous_result = load_json(
        previous_result_path
    )

    previous_manifest = load_json(
        previous_manifest_path
    )

    if (
        previous_result.get(
            "execution_status"
        ) != "PASS"
    ):
        raise ValueError(
            "Previous timestamp audit not PASS"
        )

    if previous_result.get(
        "version"
    ) != "Clean V001":
        raise ValueError(
            "Unexpected previous audit version"
        )

    verify_previous_outputs(
        root,
        previous_manifest,
    )

    if not shard_root.is_dir():
        raise FileNotFoundError(
            f"JOIN shards missing: {shard_root}"
        )

    shard_files = sorted(
        shard_root.rglob("*.csv")
    )

    if not shard_files:
        raise ValueError(
            "No JOIN CSV shards discovered"
        )

    total = 0
    counts = Counter()
    timeframe_counts = Counter()
    delay_distribution = Counter()

    examples = []
    issues_total = 0

    for index, shard in enumerate(
        shard_files,
        1,
    ):
        with shard.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if not reader.fieldnames:
                raise ValueError(
                    f"Empty CSV header: {shard}"
                )

            missing = [
                col
                for col in REQUIRED_COLUMNS
                if col not in reader.fieldnames
            ]

            if missing:
                raise ValueError(
                    f"Missing columns {missing}: {shard}"
                )

            for line_number, row in enumerate(
                reader,
                2,
            ):
                classification, delay = inspect_row(
                    row
                )

                total += 1

                counts[classification] += 1

                timeframe_counts[
                    (
                        row["timeframe"],
                        classification,
                    )
                ] += 1

                if classification == (
                    "DELAYED_ALIGNED_ENTRY"
                ):
                    delay_distribution[
                        (
                            row["timeframe"],
                            delay,
                        )
                    ] += 1

                if classification != (
                    "NORMAL_NEXT_OPEN"
                ):
                    issues_total += 1

                    if len(examples) < args.max_issues:
                        examples.append(
                            {
                                "file": shard.relative_to(
                                    root
                                ).as_posix(),
                                "line": line_number,
                                "timeframe": row[
                                    "timeframe"
                                ],
                                "market": row[
                                    "market"
                                ],
                                "classification": classification,
                                "delay_periods": (
                                    ""
                                    if delay is None
                                    else delay
                                ),
                                "signal_timestamp": row[
                                    "signal_timestamp"
                                ],
                                "entry_timestamp": row[
                                    "entry_timestamp"
                                ],
                                "feature_candle_timestamp": row[
                                    "feature_candle_timestamp"
                                ],
                                "feature_candle_end_timestamp": row[
                                    "feature_candle_end_timestamp"
                                ],
                            }
                        )

        if (
            index % 25 == 0
            or index == len(shard_files)
        ):
            print(
                f"[AUDIT] {index}/{len(shard_files)} "
                f"rows={total}",
                flush=True,
            )

    expected_total = int(
        previous_result["source_rows"]
    )

    if total != expected_total:
        raise ValueError(
            "JOIN source row mismatch: "
            f"{total} != {expected_total}"
        )

    if sum(counts.values()) != total:
        raise AssertionError(
            "Classification reconciliation failed"
        )

    expected_normal = int(
        previous_result[
            "classification_counts"
        ]["CANDLE_OPEN_NEXT_OPEN_COMPATIBLE"]
    )

    expected_other = int(
        previous_result[
            "classification_counts"
        ]["OTHER_TIMING"]
    )

    if counts["NORMAL_NEXT_OPEN"] != expected_normal:
        raise ValueError(
            "Normal next-open count mismatch"
        )

    actual_other = (
        total
        - counts["NORMAL_NEXT_OPEN"]
    )

    if actual_other != expected_other:
        raise ValueError(
            "Other-timing count mismatch"
        )

    critical = (
        counts["ENTRY_BEFORE_FEATURE_CLOSE"]
        + counts["FEATURE_END_MISMATCH"]
        + counts["INVALID_TIMESTAMP"]
        + counts["UNKNOWN_TIMEFRAME"]
    )

    ambiguous = counts[
        "DELAYED_UNALIGNED_ENTRY"
    ]

    decision = (
        "BLOCK"
        if critical > 0
        else "HOLD"
    )

    output_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    comparison_path = (
        output_root
        / "delayed_entry_timing_comparison.csv"
    )

    distribution_path = (
        output_root
        / "delayed_entry_timing_distribution.csv"
    )

    issues_path = (
        output_root
        / "delayed_entry_timing_issues.csv"
    )

    result_path = (
        output_root
        / "delayed_entry_timing_result.json"
    )

    manifest_path = (
        output_root
        / "delayed_entry_timing_manifest.json"
    )

    with comparison_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.writer(handle)

        writer.writerow(
            [
                "timeframe",
                "classification",
                "rows",
            ]
        )

        for (
            timeframe,
            classification,
        ), count in sorted(
            timeframe_counts.items()
        ):
            writer.writerow(
                [
                    timeframe,
                    classification,
                    count,
                ]
            )

    with distribution_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.writer(handle)

        writer.writerow(
            [
                "timeframe",
                "delay_periods",
                "rows",
            ]
        )

        for (
            timeframe,
            delay,
        ), count in sorted(
            delay_distribution.items()
        ):
            writer.writerow(
                [
                    timeframe,
                    delay,
                    count,
                ]
            )

    issue_fields = [
        "file",
        "line",
        "timeframe",
        "market",
        "classification",
        "delay_periods",
        "signal_timestamp",
        "entry_timestamp",
        "feature_candle_timestamp",
        "feature_candle_end_timestamp",
    ]

    with issues_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=issue_fields,
        )

        writer.writeheader()
        writer.writerows(examples)

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": total,
        "source_shards": len(shard_files),
        "classification_counts": {
            key: counts[key]
            for key in CLASSIFICATIONS
        },
        "delayed_entry_rows": counts[
            "DELAYED_ALIGNED_ENTRY"
        ],
        "unaligned_entry_rows": ambiguous,
        "critical_timing_rows": critical,
        "issues_total": issues_total,
        "issues_recorded": len(examples),
        "issues_truncated": (
            issues_total > len(examples)
        ),
        "research_decision": decision,
        "delayed_entry_cause_proven": False,
        "actual_candle_existence_verified": False,
        "upstream_signal_formula_verified": False,
        "point_in_time_proven": False,
        "train_experiment_approved": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "notes": [
            "Aligned delay does not establish why entry was delayed.",
            "Actual market candle existence was not checked.",
            "Feature formulas and upstream signal code were not inspected.",
            "Source JOIN shard content hashes were not independently verified.",
            "No source datasets were modified.",
        ],
    }

    save_json_atomic(
        result_path,
        result,
    )

    input_paths = {
        SOURCE_RESULT: previous_result_path,
        SOURCE_MANIFEST: previous_manifest_path,
    }

    manifest = {
        "version": VERSION,
        "input_sha256": {
            relative: file_sha256(path)
            for relative, path in input_paths.items()
        },
        "source_shard_content_hashes_not_verified": True,
        "output_sha256": {
            path.name: file_sha256(path)
            for path in (
                comparison_path,
                distribution_path,
                issues_path,
                result_path,
            )
        },
    }

    save_json_atomic(
        manifest_path,
        manifest,
    )

    print(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        ),
        flush=True,
    )

    print(
        f"[DONE] {output_root}",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
