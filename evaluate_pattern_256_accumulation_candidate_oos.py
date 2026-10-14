
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_accumulation_candidate_oos.py
Clean V002

Candidate OOS Evaluation

Inputs:
    candidate_oos_summary.csv
    candidate_oos_yearly.csv
    candidate_oos_result.json

Outputs:
    oos_evaluation_summary.csv
    oos_evaluation_comparison.csv
    oos_evaluation_decision.csv
    oos_evaluation_result.json
    oos_evaluation_manifest.json

Safety:
    - Upstream data is read-only.
    - Existing output directories are never overwritten.
    - SHA256 integrity checks.
    - No trading or order execution.
    - Aggregate-level evaluation only.
    - No unsupported event-level statistical claims.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
import traceback

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


PROGRAM_NAME = (
    "evaluate_pattern_256_accumulation_candidate_oos.py"
)

PROGRAM_VERSION = "Clean V002"

DEFAULT_INPUT = (
    "data/reports/"
    "pattern_256_accumulation_candidate_oos"
)

DEFAULT_OUTPUT = (
    "data/reports/"
    "pattern_256_accumulation_candidate_oos_evaluation"
)

SUMMARY_FILE = "candidate_oos_summary.csv"
YEARLY_FILE = "candidate_oos_yearly.csv"
RESULT_FILE = "candidate_oos_result.json"

OUTPUT_SUMMARY = "oos_evaluation_summary.csv"
OUTPUT_COMPARISON = "oos_evaluation_comparison.csv"
OUTPUT_DECISION = "oos_evaluation_decision.csv"
OUTPUT_RESULT = "oos_evaluation_result.json"
OUTPUT_MANIFEST = "oos_evaluation_manifest.json"

SUMMARY_COLUMNS = [
    "timeframe",
    "rule_id",
    "horizon",
    "train_selected_events",
    "train_complete_events",
    "train_hit_events",
    "train_censored_events",
    "train_hit_rate_pct",
    "oos_selected_events",
    "oos_complete_events",
    "oos_hit_events",
    "oos_censored_events",
    "oos_hit_rate_pct",
    "oos_minus_train_pp",
    "status",
]

YEARLY_COLUMNS = [
    "timeframe",
    "year",
    "rule_id",
    "horizon",
    "selected_events",
    "complete_events",
    "hit_events",
    "miss_events",
    "censored_events",
    "hit_rate_pct",
    "status",
]

TIMEFRAMES = {"h1", "h4", "d1"}
HORIZONS = {24, 72, 168}

SUMMARY_OUTPUT_COLUMNS = [
    "timeframe",
    "rule_id",
    "horizon",
    "split",
    "selected_events",
    "complete_events",
    "hit_events",
    "miss_events",
    "censored_events",
    "completion_rate_pct",
    "hit_rate_pct",
    "status",
]

COMPARISON_OUTPUT_COLUMNS = [
    "timeframe",
    "rule_id",
    "horizon",
    "train_selected_events",
    "train_complete_events",
    "train_hit_events",
    "train_hit_rate_pct",
    "oos_selected_events",
    "oos_complete_events",
    "oos_hit_events",
    "oos_hit_rate_pct",
    "oos_minus_train_pp",
    "oos_completion_rate_pct",
    "yearly_periods",
    "yearly_hit_rate_min_pct",
    "yearly_hit_rate_max_pct",
    "yearly_hit_rate_range_pp",
    "status",
]

DECISION_OUTPUT_COLUMNS = [
    "timeframe",
    "rule_id",
    "horizon",
    "decision",
    "reasons",
    "oos_complete_events",
    "oos_hit_rate_pct",
    "oos_minus_train_pp",
    "approved_for_live_trading",
]


def log(message):
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"[{timestamp}] {message}", flush=True)


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig"
    ) as stream:
        result = json.load(stream)

    if not isinstance(result, dict):
        raise ValueError(
            f"Invalid JSON object: {path}"
        )

    return result


def read_csv(path, required_columns):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                f"Missing CSV header: {path}"
            )

        columns = [
            name.strip()
            for name in reader.fieldnames
        ]

        missing = [
            name
            for name in required_columns
            if name not in columns
        ]

        if missing:
            raise ValueError(
                f"Missing columns in {path.name}: "
                f"{missing}"
            )

        rows = []

        for line_number, row in enumerate(
            reader,
            start=2
        ):
            if None in row:
                raise ValueError(
                    f"Extra CSV fields at "
                    f"{path.name}:{line_number}"
                )

            if all(
                not str(value or "").strip()
                for value in row.values()
            ):
                continue

            rows.append(row)

    if not rows:
        raise ValueError(
            f"Empty CSV: {path}"
        )

    return rows


def parse_int(value, field):
    text = str(value).strip()

    try:
        number = int(text)
    except (ValueError, TypeError):
        raise ValueError(
            f"Invalid integer {field}: {value!r}"
        )

    if number < 0:
        raise ValueError(
            f"Negative integer {field}: {number}"
        )

    return number


def parse_float(value, field):
    try:
        number = float(
            str(value).strip()
        )
    except (ValueError, TypeError):
        raise ValueError(
            f"Invalid float {field}: {value!r}"
        )

    if not math.isfinite(number):
        raise ValueError(
            f"Non-finite value {field}"
        )

    return number


def percentage(numerator, denominator):
    if denominator == 0:
        return None

    return 100.0 * numerator / denominator


def close_enough(actual, expected, tolerance=0.0001):
    if actual is None or expected is None:
        return actual is None and expected is None

    return abs(actual - expected) <= tolerance


def validate_key(row):
    timeframe = str(
        row["timeframe"]
    ).strip().lower()

    rule_id = str(
        row["rule_id"]
    ).strip()

    horizon = parse_int(
        row["horizon"],
        "horizon"
    )

    if timeframe not in TIMEFRAMES:
        raise ValueError(
            f"Unsupported timeframe: {timeframe}"
        )

    if not rule_id:
        raise ValueError(
            "Empty rule_id"
        )

    if horizon not in HORIZONS:
        raise ValueError(
            f"Unsupported horizon: {horizon}"
        )

    return timeframe, rule_id, horizon


def validate_summary(rows):
    result = {}
    normalized = []

    for row in rows:
        key = validate_key(row)

        if key in result:
            raise ValueError(
                f"Duplicate summary key: {key}"
            )

        data = {
            "timeframe": key[0],
            "rule_id": key[1],
            "horizon": key[2],
            "status": str(
                row["status"]
            ).strip(),
        }

        for split in ("train", "oos"):
            for field in (
                "selected_events",
                "complete_events",
                "hit_events",
                "censored_events",
            ):
                name = f"{split}_{field}"

                data[name] = parse_int(
                    row[name],
                    name
                )

            selected = data[
                f"{split}_selected_events"
            ]
            complete = data[
                f"{split}_complete_events"
            ]
            hit = data[
                f"{split}_hit_events"
            ]
            censored = data[
                f"{split}_censored_events"
            ]

            if selected != complete + censored:
                raise ValueError(
                    f"Selection accounting mismatch: "
                    f"{key}, {split}"
                )

            if hit > complete:
                raise ValueError(
                    f"Hits exceed completed events: "
                    f"{key}, {split}"
                )

            rate = percentage(
                hit,
                complete
            )

            reported = parse_float(
                row[f"{split}_hit_rate_pct"],
                f"{split}_hit_rate_pct"
            )

            if rate is None:
                if reported != 0:
                    raise ValueError(
                        f"Undefined hit rate: "
                        f"{key}, {split}"
                    )
            elif not close_enough(rate, reported):
                raise ValueError(
                    f"Hit rate mismatch: "
                    f"{key}, {split}"
                )

            data[f"{split}_hit_rate_pct"] = rate

        train_rate = data[
            "train_hit_rate_pct"
        ]
        oos_rate = data[
            "oos_hit_rate_pct"
        ]

        difference = None

        if (
            train_rate is not None
            and oos_rate is not None
        ):
            difference = oos_rate - train_rate

            reported_difference = parse_float(
                row["oos_minus_train_pp"],
                "oos_minus_train_pp"
            )

            if not close_enough(
                difference,
                reported_difference
            ):
                raise ValueError(
                    f"Train/OOS difference mismatch: {key}"
                )

        data["oos_minus_train_pp"] = difference

        result[key] = data
        normalized.append(data)

    return result, normalized


def validate_yearly(rows, summary_map):
    grouped = defaultdict(list)
    seen = set()
    normalized = []

    for row in rows:
        key = validate_key(row)

        if key not in summary_map:
            raise ValueError(
                f"Yearly key absent from summary: {key}"
            )

        year = parse_int(
            row["year"],
            "year"
        )

        if year < 2000 or year > 2100:
            raise ValueError(
                f"Invalid year: {year}"
            )

        unique_key = (key, year)

        if unique_key in seen:
            raise ValueError(
                f"Duplicate yearly key: {unique_key}"
            )

        seen.add(unique_key)

        data = {
            "timeframe": key[0],
            "year": year,
            "rule_id": key[1],
            "horizon": key[2],
            "status": str(
                row["status"]
            ).strip(),
        }

        for field in (
            "selected_events",
            "complete_events",
            "hit_events",
            "miss_events",
            "censored_events",
        ):
            data[field] = parse_int(
                row[field],
                field
            )

        if (
            data["selected_events"]
            != data["complete_events"]
            + data["censored_events"]
        ):
            raise ValueError(
                f"Yearly selection mismatch: {unique_key}"
            )

        if (
            data["complete_events"]
            != data["hit_events"]
            + data["miss_events"]
        ):
            raise ValueError(
                f"Yearly hit/miss mismatch: {unique_key}"
            )

        rate = percentage(
            data["hit_events"],
            data["complete_events"]
        )

        reported = parse_float(
            row["hit_rate_pct"],
            "hit_rate_pct"
        )

        if rate is None:
            if reported != 0:
                raise ValueError(
                    f"Undefined yearly rate: {unique_key}"
                )
        elif not close_enough(rate, reported):
            raise ValueError(
                f"Yearly hit rate mismatch: {unique_key}"
            )

        data["hit_rate_pct"] = rate

        grouped[key].append(data)
        normalized.append(data)

    return grouped, normalized


def build_summary(normalized):
    output = []

    for item in normalized:
        for split in ("train", "oos"):
            selected = item[
                f"{split}_selected_events"
            ]
            complete = item[
                f"{split}_complete_events"
            ]
            hit = item[
                f"{split}_hit_events"
            ]

            output.append({
                "timeframe": item["timeframe"],
                "rule_id": item["rule_id"],
                "horizon": item["horizon"],
                "split": split.upper(),
                "selected_events": selected,
                "complete_events": complete,
                "hit_events": hit,
                "miss_events": complete - hit,
                "censored_events": item[
                    f"{split}_censored_events"
                ],
                "completion_rate_pct": percentage(
                    complete,
                    selected
                ),
                "hit_rate_pct": item[
                    f"{split}_hit_rate_pct"
                ],
                "status": item["status"],
            })

    return output


def build_comparisons(normalized, yearly_groups):
    output = []

    for item in normalized:
        key = (
            item["timeframe"],
            item["rule_id"],
            item["horizon"],
        )

        yearly = yearly_groups.get(
            key,
            []
        )

        yearly_rates = [
            row["hit_rate_pct"]
            for row in yearly
            if row["hit_rate_pct"] is not None
        ]

        minimum = (
            min(yearly_rates)
            if yearly_rates else None
        )

        maximum = (
            max(yearly_rates)
            if yearly_rates else None
        )

        rate_range = (
            maximum - minimum
            if minimum is not None
            else None
        )

        output.append({
            "timeframe": item["timeframe"],
            "rule_id": item["rule_id"],
            "horizon": item["horizon"],
            "train_selected_events": item[
                "train_selected_events"
            ],
            "train_complete_events": item[
                "train_complete_events"
            ],
            "train_hit_events": item[
                "train_hit_events"
            ],
            "train_hit_rate_pct": item[
                "train_hit_rate_pct"
            ],
            "oos_selected_events": item[
                "oos_selected_events"
            ],
            "oos_complete_events": item[
                "oos_complete_events"
            ],
            "oos_hit_events": item[
                "oos_hit_events"
            ],
            "oos_hit_rate_pct": item[
                "oos_hit_rate_pct"
            ],
            "oos_minus_train_pp": item[
                "oos_minus_train_pp"
            ],
            "oos_completion_rate_pct": percentage(
                item["oos_complete_events"],
                item["oos_selected_events"]
            ),
            "yearly_periods": len(yearly_rates),
            "yearly_hit_rate_min_pct": minimum,
            "yearly_hit_rate_max_pct": maximum,
            "yearly_hit_rate_range_pp": rate_range,
            "status": "DESCRIPTIVE_EVALUATION_ONLY",
        })

    return output


def build_decisions(comparisons):
    decisions = []

    for item in comparisons:
        reasons = []

        train_n = item[
            "train_complete_events"
        ]
        oos_n = item[
            "oos_complete_events"
        ]

        difference = item[
            "oos_minus_train_pp"
        ]

        if train_n == 0:
            reasons.append(
                "NO_TRAIN_COMPLETED_EVENTS"
            )

        if oos_n == 0:
            reasons.append(
                "NO_OOS_COMPLETED_EVENTS"
            )

        if oos_n < 100:
            reasons.append(
                "SMALL_OOS_SAMPLE"
            )

        if difference is not None:
            if difference < -5:
                reasons.append(
                    "OOS_HIT_RATE_DETERIORATION"
                )
            elif difference > 0:
                reasons.append(
                    "OOS_HIT_RATE_HIGHER_THAN_TRAIN"
                )
            else:
                reasons.append(
                    "OOS_HIT_RATE_NOT_HIGHER"
                )

        if item["yearly_periods"] < 2:
            reasons.append(
                "LIMITED_YEARLY_COVERAGE"
            )

        reasons.extend([
            "AGGREGATE_DATA_ONLY",
            "SIGNAL_DEPENDENCE_NOT_VERIFIED",
            "NET_RETURN_NOT_VERIFIED",
            "TRADING_RISK_NOT_VERIFIED",
            "MULTIPLE_TESTING_NOT_ADJUSTED",
        ])

        if train_n == 0 or oos_n == 0:
            decision = "INSUFFICIENT_DATA"
        elif oos_n < 100:
            decision = "INSUFFICIENT_SAMPLE"
        elif (
            difference is not None
            and difference < -5
        ):
            decision = "REVIEW_FOR_OVERFITTING"
        else:
            decision = "FURTHER_VALIDATION_REQUIRED"

        decisions.append({
            "timeframe": item["timeframe"],
            "rule_id": item["rule_id"],
            "horizon": item["horizon"],
            "decision": decision,
            "reasons": "|".join(reasons),
            "oos_complete_events": oos_n,
            "oos_hit_rate_pct": item[
                "oos_hit_rate_pct"
            ],
            "oos_minus_train_pp": difference,
            "approved_for_live_trading": False,
        })

    return decisions


def write_csv(path, rows, columns):
    with path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
            extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(rows)


def write_json(path, data):
    with path.open(
        "w",
        encoding="utf-8"
    ) as stream:
        json.dump(
            data,
            stream,
            ensure_ascii=False,
            indent=2
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate aggregated candidate OOS results."
        )
    )

    parser.add_argument(
        "--root",
        required=True
    )

    parser.add_argument(
        "--input-dir",
        default=None
    )

    parser.add_argument(
        "--output-dir",
        default=None
    )

    return parser.parse_args()


def main():
    args = parse_args()

    log("=" * 70)
    log(f"{PROGRAM_NAME} / {PROGRAM_VERSION}")
    log("=" * 70)

    root = Path(
        args.root
    ).expanduser().resolve(strict=True)

    if not root.is_dir():
        raise ValueError(
            "Project root is not a directory."
        )

    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else root / DEFAULT_INPUT
    ).expanduser().resolve(strict=True)

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else root / DEFAULT_OUTPUT
    ).expanduser().resolve(strict=False)

    if not input_dir.is_dir():
        raise FileNotFoundError(
            f"Input directory missing: {input_dir}"
        )

    if input_dir == output_dir:
        raise ValueError(
            "Input and output directories must differ."
        )

    if (
        input_dir in output_dir.parents
        or output_dir in input_dir.parents
    ):
        raise ValueError(
            "Input and output directories must not overlap."
        )

    if output_dir.exists():
        raise FileExistsError(
            "Output already exists. Refusing to overwrite: "
            f"{output_dir}"
        )

    summary_path = input_dir / SUMMARY_FILE
    yearly_path = input_dir / YEARLY_FILE
    result_path = input_dir / RESULT_FILE

    source_files = [
        summary_path,
        yearly_path,
        result_path,
    ]

    for path in source_files:
        if not path.is_file():
            raise FileNotFoundError(
                f"Required input missing: {path}"
            )

    log("[CHECK] Upstream OOS result")

    upstream = read_json(result_path)

    if str(
        upstream.get("status", "")
    ).strip().upper() != "PASS":
        raise ValueError(
            "Upstream OOS status is not PASS."
        )

    log("[PASS] Upstream OOS result")

    log("[CHECK] Input SHA256")

    source_hashes = {
        str(path): sha256_file(path)
        for path in source_files
    }

    log("[PASS] Input SHA256 captured")

    log("[READ] Candidate OOS summary")

    raw_summary = read_csv(
        summary_path,
        SUMMARY_COLUMNS
    )

    summary_map, normalized_summary = (
        validate_summary(raw_summary)
    )

    log(
        f"[PASS] Summary rows: "
        f"{len(normalized_summary):,}"
    )

    log("[READ] Candidate OOS yearly")

    raw_yearly = read_csv(
        yearly_path,
        YEARLY_COLUMNS
    )

    yearly_groups, normalized_yearly = (
        validate_yearly(
            raw_yearly,
            summary_map
        )
    )

    log(
        f"[PASS] Yearly rows: "
        f"{len(normalized_yearly):,}"
    )

    log("[EVALUATE] Building summary")

    summaries = build_summary(
        normalized_summary
    )

    log("[EVALUATE] Building comparisons")

    comparisons = build_comparisons(
        normalized_summary,
        yearly_groups
    )

    log("[EVALUATE] Building decisions")

    decisions = build_decisions(
        comparisons
    )

    if not summaries or not comparisons:
        raise ValueError(
            "No evaluation results generated."
        )

    log("[CHECK] Source SHA256 unchanged")

    for path in source_files:
        current = sha256_file(path)

        if current != source_hashes[str(path)]:
            raise ValueError(
                f"Source changed during evaluation: {path}"
            )

    log("[PASS] Source SHA256 unchanged")

    # Output is created only after input verification.
    # Existing directories are never deleted or replaced.

    output_dir.mkdir(
        parents=True,
        exist_ok=False
    )

    write_csv(
        output_dir / OUTPUT_SUMMARY,
        summaries,
        SUMMARY_OUTPUT_COLUMNS
    )

    write_csv(
        output_dir / OUTPUT_COMPARISON,
        comparisons,
        COMPARISON_OUTPUT_COLUMNS
    )

    write_csv(
        output_dir / OUTPUT_DECISION,
        decisions,
        DECISION_OUTPUT_COLUMNS
    )

    decision_counts = {}

    for row in decisions:
        name = row["decision"]

        decision_counts[name] = (
            decision_counts.get(name, 0) + 1
        )

    result = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "status": "PASS",
        "created_at_utc": utc_now(),
        "input_directory": str(input_dir),
        "output_directory": str(output_dir),
        "upstream_status": upstream["status"],
        "source_sha256": source_hashes,
        "source_summary_rows": len(
            normalized_summary
        ),
        "source_yearly_rows": len(
            normalized_yearly
        ),
        "summary_rows": len(summaries),
        "comparison_rows": len(comparisons),
        "decision_rows": len(decisions),
        "decision_counts": decision_counts,
        "approved_for_live_trading": False,
        "evaluation_level": "AGGREGATE",
        "limitations": [
            "Aggregate CSV files do not contain individual signal records.",
            "Event-level independence cannot be verified.",
            "Statistical significance is not established.",
            "Repeated or overlapping signals may be correlated.",
            "No multiple-testing adjustment is applied.",
            "OOS performance is descriptive only.",
            "Hit rates are not realized trading returns.",
            "Transaction costs and slippage are not verified.",
            "Risk-adjusted returns cannot be calculated.",
            "Further independent validation is required.",
        ],
    }

    write_json(
        output_dir / OUTPUT_RESULT,
        result
    )

    manifest = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "created_at_utc": utc_now(),
        "source_sha256": source_hashes,
        "output_sha256": {},
    }

    for filename in (
        OUTPUT_SUMMARY,
        OUTPUT_COMPARISON,
        OUTPUT_DECISION,
        OUTPUT_RESULT,
    ):
        path = output_dir / filename

        manifest["output_sha256"][filename] = (
            sha256_file(path)
        )

    write_json(
        output_dir / OUTPUT_MANIFEST,
        manifest
    )

    log("=" * 70)
    log("[PASS] Candidate OOS evaluation completed")
    log(f"Summary rows: {len(summaries):,}")
    log(f"Comparison rows: {len(comparisons):,}")
    log(f"Decision rows: {len(decisions):,}")
    log(f"Output: {output_dir}")
    log("=" * 70)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log(f"[FAIL] {exc}")
        traceback.print_exc()
        sys.exit(1)
