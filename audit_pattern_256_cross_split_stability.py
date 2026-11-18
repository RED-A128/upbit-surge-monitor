
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Cross-Split Stability Audit
Clean V001

Input:
 data/reports/pattern_256_statistical_robustness/
   robustness_result.json
   robustness_manifest.json

Output:
 data/reports/pattern_256_cross_split_stability/
   stability_result.json
   stability_comparison.csv
   stability_report.md
   stability_manifest.json

Research only.
No independent OOS or profitability approval.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import tempfile
from pathlib import Path

VERSION = "Clean V001"
INPUT_FOLDER = "pattern_256_statistical_robustness"
OUTPUT_FOLDER = "pattern_256_cross_split_stability"

TIMEFRAMES = ("h1", "h4", "d1")
SPLITS = ("DEV", "VALIDATION", "TEST")
HORIZONS = (24, 72, 168)

RETENTION_THRESHOLDS = (5.0, 10.0, 20.0, 30.0, 50.0)

CSV_FIELDS = [
    "timeframe",
    "horizon",
    "dev_pairs",
    "validation_pairs",
    "test_pairs",
    "dev_difference_pp",
    "validation_difference_pp",
    "test_difference_pp",
    "dev_retention_pct",
    "validation_retention_pct",
    "test_retention_pct",
    "all_splits_evaluable",
    "all_splits_positive",
    "all_splits_negative",
    "sign_consistency",
    "minimum_retention_pct",
    "test_holm_p",
    "test_holm_significant",
    "classification",
]


def sha256_file(path):
    digest = hashlib.sha256()

    with open(path, "rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(
        prefix=".partial_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temp_path, path)

    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def save_json(path, value):
    atomic_text(
        path,
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def save_csv(path, rows):
    stream = io.StringIO()

    writer = csv.DictWriter(
        stream,
        fieldnames=CSV_FIELDS,
        extrasaction="ignore",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_text(path, stream.getvalue())


def verify_input_manifest(folder):
    manifest_path = folder / "robustness_manifest.json"
    manifest = load_json(manifest_path)

    files = manifest.get("files")

    if not isinstance(files, dict):
        raise ValueError("Invalid input manifest")

    required = {
        "robustness_result.json",
        "robustness_comparison.csv",
        "robustness_report.md",
    }

    if not required.issubset(files):
        raise ValueError(
            "Required files missing from manifest"
        )

    for filename, expected_hash in files.items():
        if Path(filename).name != filename:
            raise ValueError(
                "Unsafe manifest path"
            )

        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        actual = sha256_file(path)

        if actual.lower() != str(
            expected_hash
        ).lower():
            raise ValueError(
                f"SHA256 mismatch: {filename}"
            )

    return {
        "manifest_sha256": sha256_file(
            manifest_path
        ),
        "verified_files": len(files),
    }


def finite_number(value, field):
    if isinstance(value, bool):
        raise ValueError(
            f"Invalid boolean number: {field}"
        )

    result = float(value)

    if not math.isfinite(result):
        raise ValueError(
            f"Non-finite number: {field}"
        )

    return result


def validate_source(source):
    if source.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected robustness version"
        )

    if source.get("execution") != "PASS":
        raise ValueError(
            "Previous execution not PASS"
        )

    if source.get("research_decision") != "HOLD":
        raise ValueError(
            "Previous research gate changed"
        )

    if source.get("independent_oos_proven") is not False:
        raise ValueError(
            "Unexpected OOS approval"
        )

    if source.get("live_trading_approved") is not False:
        raise ValueError(
            "Unexpected trading approval"
        )

    if source.get("comparison_groups") != 27:
        raise ValueError(
            "Expected 27 comparison groups"
        )

    rows = source.get("comparison")

    if not isinstance(rows, list):
        raise ValueError(
            "Missing comparison rows"
        )

    index = {}

    for row in rows:
        key = (
            row["timeframe"],
            row["split"],
            int(row["horizon"]),
        )

        if key in index:
            raise ValueError(
                f"Duplicate comparison group: {key}"
            )

        n = int(row["conservative_pairs"])
        baseline_n = int(row["baseline_pairs"])

        if n < 0 or baseline_n < n:
            raise ValueError(
                f"Invalid sample count: {key}"
            )

        retention = row["retention_pct"]

        if baseline_n > 0:
            expected = 100.0 * n / baseline_n

            actual = finite_number(
                retention, "retention_pct"
            )

            if not math.isclose(
                actual,
                expected,
                abs_tol=0.000002,
                rel_tol=0,
            ):
                raise ValueError(
                    f"Retention mismatch: {key}"
                )

        if n == 0:
            if row["conservative_difference_pp"] is not None:
                raise ValueError(
                    f"Zero-sample difference: {key}"
                )
        else:
            finite_number(
                row["conservative_difference_pp"],
                "conservative_difference_pp",
            )

            if row["holm_adjusted_p"] is not None:
                p = finite_number(
                    row["holm_adjusted_p"],
                    "holm_adjusted_p",
                )

                if not 0 <= p <= 1:
                    raise ValueError(
                        f"Invalid adjusted p: {key}"
                    )

        index[key] = row

    expected_keys = {
        (tf, split, horizon)
        for tf in TIMEFRAMES
        for split in SPLITS
        for horizon in HORIZONS
    }

    if set(index) != expected_keys:
        raise ValueError(
            "Unexpected comparison group keys"
        )

    return index


def direction(value):
    if value is None:
        return None

    value = float(value)

    if value > 0:
        return 1

    if value < 0:
        return -1

    return 0


def build_comparison(index):
    output = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            split_rows = {
                split: index[
                    (timeframe, split, horizon)
                ]
                for split in SPLITS
            }

            counts = {
                split: int(
                    split_rows[split]["conservative_pairs"]
                )
                for split in SPLITS
            }

            differences = {
                split: split_rows[split][
                    "conservative_difference_pp"
                ]
                for split in SPLITS
            }

            retentions = {
                split: split_rows[split]["retention_pct"]
                for split in SPLITS
            }

            all_evaluable = all(
                counts[split] > 0
                for split in SPLITS
            )

            signs = [
                direction(differences[split])
                for split in SPLITS
            ]

            all_positive = (
                all_evaluable
                and all(sign == 1 for sign in signs)
            )

            all_negative = (
                all_evaluable
                and all(sign == -1 for sign in signs)
            )

            if not all_evaluable:
                consistency = "NOT_EVALUABLE"
            elif all_positive:
                consistency = "CONSISTENT_POSITIVE"
            elif all_negative:
                consistency = "CONSISTENT_NEGATIVE"
            elif len(set(signs)) == 1:
                consistency = "CONSISTENT_ZERO"
            else:
                consistency = "MIXED_DIRECTION"

            minimum_retention = min(
                finite_number(
                    retentions[split],
                    "retention_pct",
                )
                for split in SPLITS
            )

            test_p = split_rows["TEST"][
                "holm_adjusted_p"
            ]

            test_significant = (
                test_p is not None
                and finite_number(
                    test_p, "holm_adjusted_p"
                ) < 0.05
            )

            if not all_evaluable:
                classification = "INSUFFICIENT_DATA"
            elif all_positive:
                if minimum_retention < 10:
                    classification = (
                        "POSITIVE_BUT_SEVERE_ATTRITION"
                    )
                elif minimum_retention < 20:
                    classification = (
                        "POSITIVE_BUT_HIGH_ATTRITION"
                    )
                else:
                    classification = (
                        "POSITIVE_EXPLORATORY"
                    )
            elif all_negative:
                classification = (
                    "NEGATIVE_ACROSS_SPLITS"
                )
            else:
                classification = (
                    "SPLIT_INSTABILITY"
                )

            output.append({
                "timeframe": timeframe,
                "horizon": horizon,
                "dev_pairs": counts["DEV"],
                "validation_pairs": counts["VALIDATION"],
                "test_pairs": counts["TEST"],
                "dev_difference_pp":
                    differences["DEV"],
                "validation_difference_pp":
                    differences["VALIDATION"],
                "test_difference_pp":
                    differences["TEST"],
                "dev_retention_pct":
                    retentions["DEV"],
                "validation_retention_pct":
                    retentions["VALIDATION"],
                "test_retention_pct":
                    retentions["TEST"],
                "all_splits_evaluable":
                    all_evaluable,
                "all_splits_positive":
                    all_positive,
                "all_splits_negative":
                    all_negative,
                "sign_consistency":
                    consistency,
                "minimum_retention_pct":
                    round(minimum_retention, 6),
                "test_holm_p": test_p,
                "test_holm_significant":
                    test_significant,
                "classification":
                    classification,
            })

    return output


def build_threshold_analysis(rows):
    analysis = []

    for threshold in RETENTION_THRESHOLDS:
        eligible = [
            row for row in rows
            if row["all_splits_evaluable"]
            and row["minimum_retention_pct"]
            >= threshold
        ]

        positive = [
            row for row in eligible
            if row["all_splits_positive"]
        ]

        significant_positive = [
            row for row in positive
            if row["test_holm_significant"]
        ]

        analysis.append({
            "minimum_retention_threshold_pct":
                threshold,
            "eligible_groups": len(eligible),
            "consistent_positive_groups":
                len(positive),
            "consistent_negative_groups": sum(
                1 for row in eligible
                if row["all_splits_negative"]
            ),
            "mixed_direction_groups": sum(
                1 for row in eligible
                if row["sign_consistency"]
                == "MIXED_DIRECTION"
            ),
            "positive_with_significant_test":
                len(significant_positive),
            "positive_groups": [
                {
                    "timeframe": row["timeframe"],
                    "horizon": row["horizon"],
                }
                for row in positive
            ],
        })

    return analysis


def build_report(result):
    lines = [
        "# Pattern 256 Cross-Split Stability Audit",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "## Cross-split comparison",
        "",
        "| TF | Horizon | DEV pp | VAL pp | "
        "TEST pp | Min retention % | Classification |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]

    for row in result["comparison"]:
        lines.append(
            "| {timeframe} | {horizon} | "
            "{dev_difference_pp} | "
            "{validation_difference_pp} | "
            "{test_difference_pp} | "
            "{minimum_retention_pct} | "
            "{classification} |".format(**row)
        )

    lines.extend([
        "",
        "## Retention sensitivity",
        "",
        "| Minimum retention % | Eligible groups | "
        "Positive groups | Significant positive TEST |",
        "|---:|---:|---:|---:|",
    ])

    for row in result["threshold_analysis"]:
        lines.append(
            "| {minimum_retention_threshold_pct} | "
            "{eligible_groups} | "
            "{consistent_positive_groups} | "
            "{positive_with_significant_test} |".format(
                **row
            )
        )

    lines.extend([
        "",
        "## Limitations",
        "",
    ])

    lines.extend(
        "- " + item
        for item in result["limitations"]
    )

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
        help="Persistent local research root",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    reports_root = root / "data" / "reports"

    input_dir = reports_root / INPUT_FOLDER
    output_dir = reports_root / OUTPUT_FOLDER

    integrity = verify_input_manifest(input_dir)

    source_path = (
        input_dir / "robustness_result.json"
    )

    source = load_json(source_path)
    index = validate_source(source)

    comparison = build_comparison(index)

    threshold_analysis = build_threshold_analysis(
        comparison
    )

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification":
            "CROSS_SPLIT_SELECTION_SENSITIVITY_AUDIT",
        "source_result_sha256":
            sha256_file(source_path),
        "source_integrity": integrity,
        "input_pairs": source["input_pairs"],
        "comparison_groups": len(comparison),
        "all_splits_evaluable_groups": sum(
            row["all_splits_evaluable"]
            for row in comparison
        ),
        "consistent_positive_groups": sum(
            row["all_splits_positive"]
            for row in comparison
        ),
        "consistent_negative_groups": sum(
            row["all_splits_negative"]
            for row in comparison
        ),
        "mixed_direction_groups": sum(
            row["sign_consistency"]
            == "MIXED_DIRECTION"
            for row in comparison
        ),
        "insufficient_data_groups": sum(
            not row["all_splits_evaluable"]
            for row in comparison
        ),
        "comparison": comparison,
        "threshold_analysis": threshold_analysis,
        "limitations": [
            "DEV, VALIDATION and TEST are historical splits",
            "TEST was previously exposed",
            "No independent OOS proof",
            "No event-level market regime matching",
            "No volatility matching",
            "No market-level clustered uncertainty",
            "No correction for selecting the 168 horizon",
            "Retention thresholds are sensitivity checks",
            "Low retention may introduce selection bias",
            "Aggregate counts cannot reveal market concentration",
            "Previous-only controls are not simultaneous",
            "Daily conservative comparison is unavailable",
            "Intrabar high does not establish executable returns",
            "No fees, slippage or market impact",
            "No profitability proof",
        ],
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    save_json(
        output_dir / "stability_result.json",
        result,
    )

    save_csv(
        output_dir / "stability_comparison.csv",
        comparison,
    )

    atomic_text(
        output_dir / "stability_report.md",
        build_report(result),
    )

    output_files = [
        "stability_result.json",
        "stability_comparison.csv",
        "stability_report.md",
    ]

    save_json(
        output_dir / "stability_manifest.json",
        {
            "version": VERSION,
            "files": {
                filename: sha256_file(
                    output_dir / filename
                )
                for filename in output_files
            },
        },
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "version": VERSION,
                "comparison_groups":
                    result["comparison_groups"],
                "consistent_positive_groups":
                    result["consistent_positive_groups"],
                "mixed_direction_groups":
                    result["mixed_direction_groups"],
                "threshold_analysis":
                    threshold_analysis,
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
