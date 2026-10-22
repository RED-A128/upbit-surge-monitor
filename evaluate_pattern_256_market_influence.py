
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-5
Market influence and year-to-year stability diagnostics.

Input:
  data/reports/pattern_256_market_sensitivity/

Output:
  data/reports/pattern_256_market_influence/

Purpose:
  Identify which excluded markets reverse the observed
  advantage of the combined signal.

No automatic trading.
No causal or independent OOS claims.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys

from collections import defaultdict
from pathlib import Path


VERSION = "Clean V003-5"
INPUT_VERSION = "Clean V003-4"

SOURCE_HASH = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

INPUT_DIR = (
    "data/reports/pattern_256_market_sensitivity"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_market_influence"
)

CANDIDATES = (
    ("h1", 24),
    ("h4", 24),
)

FOLDS = (
    "2024_to_2025",
    "2025_to_2026",
)

ROLES = (
    "TRAIN",
    "TEST",
)

INPUT_FILES = (
    "market_sensitivity_detail.csv",
    "market_sensitivity_summary.csv",
    "market_sensitivity_result.json",
)

INFLUENCE_FIELDS = [
    "fold",
    "role",
    "timeframe",
    "horizon",
    "excluded_market",
    "original_difference_pp",
    "excluded_difference_pp",
    "influence_pp",
    "reverses_positive_result",
    "baseline_n_after_exclusion",
    "combined_n_after_exclusion",
    "baseline_hits_after_exclusion",
    "combined_hits_after_exclusion",
]

SUMMARY_FIELDS = [
    "timeframe",
    "horizon",
    "test_year",
    "original_difference_pp",
    "top5_excluded_difference_pp",
    "market_exclusion_count",
    "negative_after_exclusion_count",
    "nonpositive_after_exclusion_count",
    "minimum_difference_pp",
    "maximum_difference_pp",
    "most_negative_market",
    "most_negative_market_difference_pp",
    "largest_positive_influence_market",
    "largest_positive_influence_pp",
    "largest_negative_influence_market",
    "largest_negative_influence_pp",
    "fragile",
    "decision",
]


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        return json.load(handle)


def load_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "Missing CSV header: " + str(path)
            )

        return list(reader)


def write_json(path, data):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )

        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def write_csv(path, fields, rows):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="raise",
        )

        writer.writeheader()
        writer.writerows(rows)

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def number(value, field):
    if value is None or str(value).strip() == "":
        return None

    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid number: " + field
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            "Non-finite number: " + field
        )

    return result


def integer(value, field):
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid integer: " + field
        ) from exc

    if result < 0:
        raise ValueError(
            "Negative integer: " + field
        )

    return result


def validate_inputs(folder):
    manifest_path = (
        folder / "market_sensitivity_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = load_json(manifest_path)

    if manifest.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input manifest version mismatch"
        )

    if manifest.get("source_sha256") != SOURCE_HASH:
        raise ValueError(
            "Source SHA256 mismatch"
        )

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError(
            "Invalid input manifest hashes"
        )

    if set(hashes) != set(INPUT_FILES):
        raise ValueError(
            "Input file set mismatch"
        )

    verified = {}

    for filename in INPUT_FILES:
        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        actual = sha256_file(path)

        if actual != hashes[filename]:
            raise ValueError(
                "Input SHA256 mismatch: " + filename
            )

        verified[filename] = actual

    result = load_json(
        folder / "market_sensitivity_result.json"
    )

    if result.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input result version mismatch"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Input execution status mismatch"
        )

    if result.get("overall_decision") != "HOLD":
        raise ValueError(
            "Input decision mismatch"
        )

    if result.get("source_sha256") != SOURCE_HASH:
        raise ValueError(
            "Input source hash mismatch"
        )

    if result.get("source_rows") != 7949826:
        raise ValueError(
            "Input source row mismatch"
        )

    if result.get("approved_for_live_trading") is not False:
        raise ValueError(
            "Unsafe input approval flag"
        )

    details = load_csv(
        folder / "market_sensitivity_detail.csv"
    )

    summaries = load_csv(
        folder / "market_sensitivity_summary.csv"
    )

    if len(details) != result.get("detail_rows"):
        raise ValueError(
            "Detail row count mismatch"
        )

    if len(summaries) != 2:
        raise ValueError(
            "Summary row count mismatch"
        )

    return (
        result,
        details,
        summaries,
        verified,
    )


def parse_details(rows):
    groups = defaultdict(
        lambda: {
            "original": None,
            "top5": None,
            "loo": [],
        }
    )

    seen = set()

    required = {
        "fold",
        "role",
        "timeframe",
        "horizon",
        "scenario",
        "excluded_market",
        "excluded_count",
        "baseline_n",
        "baseline_hits",
        "combined_n",
        "combined_hits",
        "difference_pp",
    }

    for row in rows:
        if not required.issubset(row):
            raise ValueError(
                "Missing detail columns"
            )

        fold = row["fold"]
        role = row["role"]
        timeframe = row["timeframe"]
        horizon = integer(
            row["horizon"],
            "horizon",
        )

        if (
            fold not in FOLDS
            or role not in ROLES
            or (timeframe, horizon) not in CANDIDATES
        ):
            raise ValueError(
                "Unexpected detail group"
            )

        scenario = row["scenario"]

        if scenario not in (
            "ORIGINAL",
            "EXCLUDE_TOP5_VOLUME",
            "LEAVE_ONE_MARKET_OUT",
        ):
            raise ValueError(
                "Unknown scenario: " + scenario
            )

        market = row["excluded_market"].strip()

        unique = (
            fold,
            role,
            timeframe,
            horizon,
            scenario,
            market,
        )

        if unique in seen:
            raise ValueError(
                "Duplicate detail record"
            )

        seen.add(unique)

        excluded_count = integer(
            row["excluded_count"],
            "excluded_count",
        )

        expected_excluded = {
            "ORIGINAL": 0,
            "EXCLUDE_TOP5_VOLUME": 5,
            "LEAVE_ONE_MARKET_OUT": 1,
        }[scenario]

        if excluded_count != expected_excluded:
            raise ValueError(
                "Excluded market count mismatch"
            )

        if scenario == "LEAVE_ONE_MARKET_OUT":
            if not market.startswith("KRW-"):
                raise ValueError(
                    "Invalid excluded market"
                )
        elif market:
            raise ValueError(
                "Unexpected excluded market"
            )

        baseline_n = integer(
            row["baseline_n"],
            "baseline_n",
        )

        baseline_hits = integer(
            row["baseline_hits"],
            "baseline_hits",
        )

        combined_n = integer(
            row["combined_n"],
            "combined_n",
        )

        combined_hits = integer(
            row["combined_hits"],
            "combined_hits",
        )

        if (
            baseline_hits > baseline_n
            or combined_hits > combined_n
        ):
            raise ValueError(
                "Hits exceed completed observations"
            )

        difference = number(
            row["difference_pp"],
            "difference_pp",
        )

        if baseline_n and combined_n:
            expected_difference = round(
                round(
                    100 * combined_hits / combined_n,
                    6,
                )
                -
                round(
                    100 * baseline_hits / baseline_n,
                    6,
                ),
                6,
            )

            if (
                difference is None
                or abs(
                    difference - expected_difference
                ) > 0.000002
            ):
                raise ValueError(
                    "Difference arithmetic mismatch"
                )
        elif difference is not None:
            raise ValueError(
                "Undefined comparison has numeric difference"
            )

        item = {
            "market": market,
            "difference": difference,
            "baseline_n": baseline_n,
            "baseline_hits": baseline_hits,
            "combined_n": combined_n,
            "combined_hits": combined_hits,
        }

        key = (
            fold,
            role,
            timeframe,
            horizon,
        )

        if scenario == "ORIGINAL":
            if groups[key]["original"] is not None:
                raise ValueError(
                    "Duplicate original"
                )

            groups[key]["original"] = item

        elif scenario == "EXCLUDE_TOP5_VOLUME":
            if groups[key]["top5"] is not None:
                raise ValueError(
                    "Duplicate top-five"
                )

            groups[key]["top5"] = item

        else:
            groups[key]["loo"].append(item)

    expected_groups = {
        (fold, role, timeframe, horizon)
        for fold in FOLDS
        for role in ROLES
        for timeframe, horizon in CANDIDATES
    }

    if set(groups) != expected_groups:
        raise ValueError(
            "Incomplete detail groups"
        )

    for key, group in groups.items():
        if (
            group["original"] is None
            or group["top5"] is None
            or not group["loo"]
        ):
            raise ValueError(
                "Incomplete scenario group: " + str(key)
            )

        original = group["original"]

        for item in group["loo"]:
            if (
                item["baseline_n"] > original["baseline_n"]
                or item["combined_n"] > original["combined_n"]
                or item["baseline_hits"] > original["baseline_hits"]
                or item["combined_hits"] > original["combined_hits"]
            ):
                raise ValueError(
                    "Exclusion increased event totals"
                )

    return groups


def build_outputs(groups):
    influence_rows = []
    summary_rows = []

    for fold in FOLDS:
        for role in ROLES:
            for timeframe, horizon in CANDIDATES:
                key = (
                    fold,
                    role,
                    timeframe,
                    horizon,
                )

                group = groups[key]
                original = group["original"]
                original_difference = original["difference"]

                if original_difference is None:
                    raise ValueError(
                        "Missing original difference"
                    )

                for item in sorted(
                    group["loo"],
                    key=lambda x: x["market"],
                ):
                    excluded_difference = item["difference"]

                    influence = (
                        round(
                            original_difference
                            - excluded_difference,
                            6,
                        )
                        if excluded_difference is not None
                        else None
                    )

                    influence_rows.append({
                        "fold": fold,
                        "role": role,
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "excluded_market": item["market"],
                        "original_difference_pp": original_difference,
                        "excluded_difference_pp": excluded_difference,
                        "influence_pp": influence,
                        "reverses_positive_result": (
                            original_difference > 0
                            and (
                                excluded_difference is None
                                or excluded_difference <= 0
                            )
                        ),
                        "baseline_n_after_exclusion": item["baseline_n"],
                        "combined_n_after_exclusion": item["combined_n"],
                        "baseline_hits_after_exclusion": item["baseline_hits"],
                        "combined_hits_after_exclusion": item["combined_hits"],
                    })

                if role != "TEST":
                    continue

                valid = [
                    item
                    for item in group["loo"]
                    if item["difference"] is not None
                ]

                if not valid:
                    raise ValueError(
                        "No valid leave-one-out observations"
                    )

                minimum = min(
                    valid,
                    key=lambda x: (
                        x["difference"],
                        x["market"],
                    ),
                )

                maximum = max(
                    valid,
                    key=lambda x: (
                        x["difference"],
                        x["market"],
                    ),
                )

                positive_influence = max(
                    valid,
                    key=lambda x: (
                        original_difference - x["difference"],
                        x["market"],
                    ),
                )

                negative_influence = min(
                    valid,
                    key=lambda x: (
                        original_difference - x["difference"],
                        x["market"],
                    ),
                )

                nonpositive = sum(
                    item["difference"] <= 0
                    for item in valid
                )

                negative = sum(
                    item["difference"] < 0
                    for item in valid
                )

                # Undefined LOO results are also fragile.
                undefined = (
                    len(group["loo"]) - len(valid)
                )

                fragile = (
                    nonpositive > 0
                    or undefined > 0
                )

                summary_rows.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "test_year": (
                        2025
                        if fold == "2024_to_2025"
                        else 2026
                    ),
                    "original_difference_pp": original_difference,
                    "top5_excluded_difference_pp": group["top5"]["difference"],
                    "market_exclusion_count": len(group["loo"]),
                    "negative_after_exclusion_count": negative,
                    "nonpositive_after_exclusion_count": (
                        nonpositive + undefined
                    ),
                    "minimum_difference_pp": minimum["difference"],
                    "maximum_difference_pp": maximum["difference"],
                    "most_negative_market": minimum["market"],
                    "most_negative_market_difference_pp": minimum["difference"],
                    "largest_positive_influence_market": positive_influence["market"],
                    "largest_positive_influence_pp": round(
                        original_difference
                        - positive_influence["difference"],
                        6,
                    ),
                    "largest_negative_influence_market": negative_influence["market"],
                    "largest_negative_influence_pp": round(
                        original_difference
                        - negative_influence["difference"],
                        6,
                    ),
                    "fragile": fragile,
                    "decision": "HOLD",
                })

    if len(summary_rows) != 4:
        raise RuntimeError(
            "Expected four TEST summaries"
        )

    return influence_rows, summary_rows


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    input_folder = (
        root / INPUT_DIR
    ).resolve()

    output_folder = (
        root / OUTPUT_DIR
    ).resolve()

    if (
        input_folder == output_folder
        or input_folder in output_folder.parents
        or output_folder in input_folder.parents
    ):
        raise RuntimeError(
            "Unsafe input/output directory"
        )

    print(
        "[START] " + VERSION,
        flush=True,
    )

    (
        input_result,
        detail_rows,
        previous_summary,
        input_hashes,
    ) = validate_inputs(input_folder)

    groups = parse_details(
        detail_rows
    )

    (
        influence_rows,
        summary_rows,
    ) = build_outputs(groups)

    # Cross-check against the prior published summary.
    previous_index = {}

    for row in previous_summary:
        key = (
            row["timeframe"],
            integer(row["horizon"], "summary horizon"),
        )

        if key in previous_index:
            raise ValueError(
                "Duplicate previous summary"
            )

        previous_index[key] = row

    if set(previous_index) != set(CANDIDATES):
        raise ValueError(
            "Previous candidate set mismatch"
        )

    for row in summary_rows:
        key = (
            row["timeframe"],
            row["horizon"],
        )

        year = row["test_year"]
        previous = previous_index[key]

        for current_field, previous_field in (
            (
                "original_difference_pp",
                "test_" + str(year) + "_original_pp",
            ),
            (
                "top5_excluded_difference_pp",
                "test_" + str(year) + "_top5_excluded_pp",
            ),
            (
                "minimum_difference_pp",
                "test_" + str(year) + "_loo_min_pp",
            ),
        ):
            expected = number(
                previous[previous_field],
                previous_field,
            )

            actual = row[current_field]

            if (
                actual is None
                or expected is None
                or abs(actual - expected) > 0.000002
            ):
                raise ValueError(
                    "Previous summary mismatch: "
                    + str(key)
                    + " "
                    + str(year)
                )

    for filename, expected in input_hashes.items():
        if sha256_file(
            input_folder / filename
        ) != expected:
            raise RuntimeError(
                "Input changed during analysis"
            )

    output_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    influence_path = (
        output_folder / "market_influence_detail.csv"
    )

    summary_path = (
        output_folder / "market_influence_summary.csv"
    )

    result_path = (
        output_folder / "market_influence_result.json"
    )

    manifest_path = (
        output_folder / "market_influence_manifest.json"
    )

    write_csv(
        influence_path,
        INFLUENCE_FIELDS,
        influence_rows,
    )

    write_csv(
        summary_path,
        SUMMARY_FIELDS,
        summary_rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_sha256": SOURCE_HASH,
        "source_rows": 7949826,
        "input_version": INPUT_VERSION,
        "candidate_count": 2,
        "influence_rows": len(influence_rows),
        "summary_rows": len(summary_rows),
        "summary": summary_rows,
        "method": (
            "Difference in percentage-point advantage "
            "before and after excluding one market"
        ),
        "limitations": [
            "Influence is descriptive, not causal.",
            "The cohorts are not matched.",
            "Leave-one-out scenarios are correlated.",
            "Markets were not randomly assigned.",
            "Candidate selection used prior test results.",
            "No independent future holdout is available.",
            "No trading profitability has been established.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    write_json(
        result_path,
        result,
    )

    output_files = (
        influence_path,
        summary_path,
        result_path,
    )

    output_hashes = {
        path.name: sha256_file(path)
        for path in output_files
    }

    manifest = {
        "version": VERSION,
        "source_sha256": SOURCE_HASH,
        "input_version": INPUT_VERSION,
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "influence_rows": len(influence_rows),
        "summary_rows": len(summary_rows),
    }

    write_json(
        manifest_path,
        manifest,
    )

    for path in output_files:
        if sha256_file(path) != output_hashes[path.name]:
            raise RuntimeError(
                "Output SHA256 mismatch: "
                + path.name
            )

    print(
        "[PASS] Influence rows: "
        + str(len(influence_rows)),
        flush=True,
    )

    print(
        "[PASS] TEST summaries: 4",
        flush=True,
    )

    for row in summary_rows:
        print(
            "[RESULT]",
            row["timeframe"],
            row["horizon"],
            row["test_year"],
            "worst_market=" + row["most_negative_market"],
            "minimum_pp=" + str(
                row["minimum_difference_pp"]
            ),
            flush=True,
        )

    print(
        "[PASS] SHA256 integrity",
        flush=True,
    )

    print(
        "[DECISION] HOLD",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
