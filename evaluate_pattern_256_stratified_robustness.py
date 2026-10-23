
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-8
Stratified threshold sensitivity and market-cluster bootstrap.

Input:
  data/reports/pattern_256_stratified_comparison/

Output:
  data/reports/pattern_256_stratified_robustness/

Research only. No automated trading.

Methods:
  - Minimum per-cohort thresholds: 1, 3, 5, 10, 20, 30, 50
  - Harmonic-weighted difference in +30% hit rates
  - Market-cluster bootstrap (1,000 repetitions)
  - Leave-one-market-out sensitivity
  - Exact SHA256 verification
  - Deterministic random seeds
  - Atomic output writes

Bootstrap intervals are descriptive and do not establish
causal improvement, independence, or profitability.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import random
import sys

from collections import defaultdict
from pathlib import Path


VERSION = "Clean V003-8"
INPUT_VERSION = "Clean V003-7"

SOURCE_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

INPUT_REL = (
    "data/reports/pattern_256_stratified_comparison"
)

OUTPUT_REL = (
    "data/reports/pattern_256_stratified_robustness"
)

INPUT_FILES = (
    "stratified_detail.csv",
    "stratified_summary.csv",
    "stratified_result.json",
)

THRESHOLDS = (1, 3, 5, 10, 20, 30, 50)
BOOTSTRAP_REPETITIONS = 1000
BASE_SEED = 2560038

GROUPS = (
    ("h1", 2025),
    ("h1", 2026),
    ("h4", 2025),
    ("h4", 2026),
)

DETAIL_FIELDS = [
    "timeframe",
    "test_year",
    "minimum_per_cohort",
    "total_strata",
    "eligible_strata",
    "excluded_strata",
    "eligible_markets",
    "baseline_n",
    "baseline_hits",
    "combined_n",
    "combined_hits",
    "weighted_difference_pp",
    "loo_min_difference_pp",
    "loo_max_difference_pp",
    "loo_nonpositive_count",
    "loo_undefined_count",
    "worst_excluded_market",
    "bootstrap_repetitions",
    "bootstrap_valid_repetitions",
    "bootstrap_ci_lower_pp",
    "bootstrap_ci_upper_pp",
    "bootstrap_positive_fraction",
    "decision",
]

SUMMARY_FIELDS = [
    "timeframe",
    "test_year",
    "threshold_10_difference_pp",
    "threshold_1_difference_pp",
    "threshold_3_difference_pp",
    "threshold_5_difference_pp",
    "threshold_20_difference_pp",
    "threshold_30_difference_pp",
    "threshold_50_difference_pp",
    "valid_thresholds",
    "nonpositive_thresholds",
    "threshold_10_eligible_markets",
    "threshold_10_ci_lower_pp",
    "threshold_10_ci_upper_pp",
    "threshold_10_loo_min_pp",
    "robust_positive_across_thresholds",
    "decision",
]


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "CSV header missing: " + str(path)
            )

        return list(reader)


def write_json(path, data):
    temp = path.with_suffix(path.suffix + ".tmp")

    with temp.open(
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

    os.replace(temp, path)


def write_csv(path, fields, rows):
    temp = path.with_suffix(path.suffix + ".tmp")

    with temp.open(
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

    os.replace(temp, path)


def parse_int(value, label):
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid integer: " + label
        ) from exc

    if parsed < 0:
        raise ValueError(
            "Negative integer: " + label
        )

    return parsed


def parse_float(value, label):
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid float: " + label
        ) from exc

    if not math.isfinite(parsed):
        raise ValueError(
            "Non-finite float: " + label
        )

    return parsed


def close(a, b, tolerance=0.000001):
    return math.isclose(
        a,
        b,
        rel_tol=0,
        abs_tol=tolerance,
    )


def validate_inputs(folder):
    manifest_path = (
        folder / "stratified_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = read_json(manifest_path)

    if manifest.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input manifest version mismatch"
        )

    if manifest.get("source_sha256") != SOURCE_SHA256:
        raise ValueError(
            "Input source hash mismatch"
        )

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError(
            "Missing input output hashes"
        )

    if set(hashes) != set(INPUT_FILES):
        raise ValueError(
            "Input file set mismatch"
        )

    for filename in INPUT_FILES:
        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        if sha256(path) != hashes[filename]:
            raise ValueError(
                "Input SHA256 mismatch: " + filename
            )

    result = read_json(
        folder / "stratified_result.json"
    )

    if result.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input result version mismatch"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Input execution not PASS"
        )

    if result.get("overall_decision") != "HOLD":
        raise ValueError(
            "Input decision mismatch"
        )

    if result.get("source_sha256") != SOURCE_SHA256:
        raise ValueError(
            "Result source hash mismatch"
        )

    if result.get("source_rows") != 7949826:
        raise ValueError(
            "Source row count mismatch"
        )

    if result.get("approved_for_live_trading") is not False:
        raise ValueError(
            "Unsafe input trading approval"
        )

    details = read_csv(
        folder / "stratified_detail.csv"
    )

    summaries = read_csv(
        folder / "stratified_summary.csv"
    )

    if len(details) != 6726:
        raise ValueError(
            "Unexpected input detail row count"
        )

    if len(details) != result.get("detail_rows"):
        raise ValueError(
            "Input detail count mismatch"
        )

    if len(summaries) != 4:
        raise ValueError(
            "Input summary count mismatch"
        )

    return details, summaries, hashes


def parse_strata(rows):
    required = {
        "timeframe",
        "test_year",
        "market",
        "month",
        "baseline_n",
        "baseline_hits",
        "combined_n",
        "combined_hits",
        "eligible_stratum",
        "difference_pp",
        "harmonic_weight",
    }

    groups = defaultdict(list)
    seen = set()

    for index, row in enumerate(rows, start=2):
        if not required.issubset(row):
            raise ValueError(
                "Missing detail columns"
            )

        tf = row["timeframe"].strip()
        year = parse_int(
            row["test_year"],
            "test_year",
        )

        market = row["market"].strip()
        month = row["month"].strip()

        if (tf, year) not in GROUPS:
            raise ValueError(
                "Unexpected timeframe/year"
            )

        if not market.startswith("KRW-"):
            raise ValueError(
                "Invalid market"
            )

        if (
            len(month) != 7
            or month[:4] != str(year)
            or month[4] != "-"
            or not month[5:7].isdigit()
            or not 1 <= int(month[5:7]) <= 12
        ):
            raise ValueError(
                "Invalid calendar month"
            )

        if year == 2026 and month > "2026-08":
            raise ValueError(
                "Unexpected 2026 month"
            )

        key = (tf, year, market, month)

        if key in seen:
            raise ValueError(
                "Duplicate market-month stratum"
            )

        seen.add(key)

        bn = parse_int(
            row["baseline_n"],
            "baseline_n",
        )

        bh = parse_int(
            row["baseline_hits"],
            "baseline_hits",
        )

        cn = parse_int(
            row["combined_n"],
            "combined_n",
        )

        ch = parse_int(
            row["combined_hits"],
            "combined_hits",
        )

        if bh > bn or ch > cn:
            raise ValueError(
                "Hits exceed observations"
            )

        eligible = (
            bn >= 10 and cn >= 10
        )

        declared = row[
            "eligible_stratum"
        ].strip().lower()

        if declared not in ("true", "false"):
            raise ValueError(
                "Invalid eligible_stratum"
            )

        if (declared == "true") != eligible:
            raise ValueError(
                "Eligibility mismatch"
            )

        difference_text = (
            row["difference_pp"].strip()
        )

        weight_text = (
            row["harmonic_weight"].strip()
        )

        if eligible:
            expected_difference = (
                100.0 * ch / cn
                - 100.0 * bh / bn
            )

            expected_weight = (
                2.0 * bn * cn / (bn + cn)
            )

            if not difference_text or not weight_text:
                raise ValueError(
                    "Missing eligible stratum metrics"
                )

            difference = parse_float(
                difference_text,
                "difference_pp",
            )

            weight = parse_float(
                weight_text,
                "harmonic_weight",
            )

            if not close(
                difference,
                expected_difference,
            ):
                raise ValueError(
                    "Difference mismatch at CSV row "
                    + str(index)
                )

            if not close(
                weight,
                expected_weight,
            ):
                raise ValueError(
                    "Weight mismatch at CSV row "
                    + str(index)
                )

        else:
            if difference_text or weight_text:
                raise ValueError(
                    "Ineligible stratum has metrics"
                )

        groups[(tf, year)].append(
            {
                "market": market,
                "month": month,
                "bn": bn,
                "bh": bh,
                "cn": cn,
                "ch": ch,
            }
        )

    if set(groups) != set(GROUPS):
        raise ValueError(
            "Missing timeframe/year groups"
        )

    return groups


def weighted_components(strata):
    numerator = 0.0
    denominator = 0.0

    for row in strata:
        bn = row["bn"]
        cn = row["cn"]

        weight = (
            2.0 * bn * cn / (bn + cn)
        )

        difference = (
            100.0 * row["ch"] / cn
            - 100.0 * row["bh"] / bn
        )

        numerator += weight * difference
        denominator += weight

    return numerator, denominator


def weighted_difference(components):
    numerator, denominator = components

    if denominator <= 0:
        return None

    return numerator / denominator


def percentile(sorted_values, fraction):
    if not sorted_values:
        return None

    position = (
        fraction * (len(sorted_values) - 1)
    )

    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return sorted_values[lower]

    weight = position - lower

    return (
        sorted_values[lower] * (1 - weight)
        + sorted_values[upper] * weight
    )


def bootstrap_markets(market_components, seed):
    markets = sorted(market_components)
    market_count = len(markets)

    if market_count < 2:
        return {
            "valid": 0,
            "lower": None,
            "upper": None,
            "positive_fraction": None,
        }

    components = [
        market_components[market]
        for market in markets
    ]

    rng = random.Random(seed)
    samples = []

    for _ in range(BOOTSTRAP_REPETITIONS):
        numerator = 0.0
        denominator = 0.0

        for _ in range(market_count):
            selected = components[
                rng.randrange(market_count)
            ]

            numerator += selected[0]
            denominator += selected[1]

        if denominator > 0:
            samples.append(
                numerator / denominator
            )

    samples.sort()

    if not samples:
        return {
            "valid": 0,
            "lower": None,
            "upper": None,
            "positive_fraction": None,
        }

    return {
        "valid": len(samples),
        "lower": percentile(samples, 0.025),
        "upper": percentile(samples, 0.975),
        "positive_fraction": (
            sum(value > 0 for value in samples)
            / len(samples)
        ),
    }


def analyze_group(tf, year, rows, threshold):
    eligible = [
        row
        for row in rows
        if (
            row["bn"] >= threshold
            and row["cn"] >= threshold
        )
    ]

    components = weighted_components(
        eligible
    )

    difference = weighted_difference(
        components
    )

    market_groups = defaultdict(list)

    for row in eligible:
        market_groups[row["market"]].append(row)

    market_components = {
        market: weighted_components(market_rows)
        for market, market_rows in market_groups.items()
    }

    loo_values = []
    loo_undefined = 0

    for market in sorted(market_components):
        market_numerator, market_denominator = (
            market_components[market]
        )

        remaining = (
            components[0] - market_numerator,
            components[1] - market_denominator,
        )

        if remaining[1] <= 1e-12:
            loo_undefined += 1
            continue

        loo_values.append(
            (
                market,
                remaining[0] / remaining[1],
            )
        )

    loo_min = (
        min(value for _, value in loo_values)
        if loo_values
        else None
    )

    loo_max = (
        max(value for _, value in loo_values)
        if loo_values
        else None
    )

    worst_market = (
        min(
            loo_values,
            key=lambda item: (
                item[1],
                item[0],
            ),
        )[0]
        if loo_values
        else None
    )

    seed = (
        BASE_SEED
        + (1 if tf == "h1" else 4) * 100000
        + year * 100
        + threshold
    )

    bootstrap = bootstrap_markets(
        market_components,
        seed,
    )

    return {
        "timeframe": tf,
        "test_year": year,
        "minimum_per_cohort": threshold,
        "total_strata": len(rows),
        "eligible_strata": len(eligible),
        "excluded_strata": len(rows) - len(eligible),
        "eligible_markets": len(market_components),
        "baseline_n": sum(
            row["bn"] for row in eligible
        ),
        "baseline_hits": sum(
            row["bh"] for row in eligible
        ),
        "combined_n": sum(
            row["cn"] for row in eligible
        ),
        "combined_hits": sum(
            row["ch"] for row in eligible
        ),
        "weighted_difference_pp": (
            round(difference, 8)
            if difference is not None
            else None
        ),
        "loo_min_difference_pp": (
            round(loo_min, 8)
            if loo_min is not None
            else None
        ),
        "loo_max_difference_pp": (
            round(loo_max, 8)
            if loo_max is not None
            else None
        ),
        "loo_nonpositive_count": sum(
            value <= 0
            for _, value in loo_values
        ),
        "loo_undefined_count": loo_undefined,
        "worst_excluded_market": worst_market,
        "bootstrap_repetitions": BOOTSTRAP_REPETITIONS,
        "bootstrap_valid_repetitions": bootstrap["valid"],
        "bootstrap_ci_lower_pp": (
            round(bootstrap["lower"], 8)
            if bootstrap["lower"] is not None
            else None
        ),
        "bootstrap_ci_upper_pp": (
            round(bootstrap["upper"], 8)
            if bootstrap["upper"] is not None
            else None
        ),
        "bootstrap_positive_fraction": (
            round(
                bootstrap["positive_fraction"],
                8,
            )
            if bootstrap["positive_fraction"] is not None
            else None
        ),
        "decision": "HOLD",
    }


def verify_previous_summary(groups, previous_rows):
    index = {}

    for row in previous_rows:
        key = (
            row["timeframe"],
            parse_int(
                row["test_year"],
                "test_year",
            ),
        )

        if key in index:
            raise ValueError(
                "Duplicate previous summary"
            )

        index[key] = row

    if set(index) != set(GROUPS):
        raise ValueError(
            "Previous summary groups mismatch"
        )

    for key in GROUPS:
        rows = groups[key]

        eligible = [
            row
            for row in rows
            if row["bn"] >= 10 and row["cn"] >= 10
        ]

        previous = index[key]

        checks = {
            "total_strata": len(rows),
            "eligible_strata": len(eligible),
            "excluded_strata": (
                len(rows) - len(eligible)
            ),
            "eligible_baseline_n": sum(
                row["bn"] for row in eligible
            ),
            "eligible_combined_n": sum(
                row["cn"] for row in eligible
            ),
            "eligible_baseline_hits": sum(
                row["bh"] for row in eligible
            ),
            "eligible_combined_hits": sum(
                row["ch"] for row in eligible
            ),
        }

        for field, expected in checks.items():
            actual = parse_int(
                previous[field],
                field,
            )

            if actual != expected:
                raise ValueError(
                    "Previous summary mismatch: "
                    + str(key)
                    + " "
                    + field
                )

        expected_difference = weighted_difference(
            weighted_components(eligible)
        )

        published_text = previous[
            "weighted_difference_pp"
        ].strip()

        if expected_difference is None:
            if published_text:
                raise ValueError(
                    "Unexpected published difference"
                )
        else:
            published = parse_float(
                published_text,
                "weighted_difference_pp",
            )

            if not close(
                published,
                expected_difference,
            ):
                raise ValueError(
                    "Published weighted difference mismatch"
                )


def build_summary(detail_rows):
    indexed = defaultdict(dict)

    for row in detail_rows:
        key = (
            row["timeframe"],
            row["test_year"],
        )

        indexed[key][
            row["minimum_per_cohort"]
        ] = row

    summaries = []

    for tf, year in GROUPS:
        records = indexed[(tf, year)]

        if set(records) != set(THRESHOLDS):
            raise ValueError(
                "Missing threshold records"
            )

        valid = [
            row
            for row in records.values()
            if row["weighted_difference_pp"] is not None
        ]

        threshold_10 = records[10]

        all_positive = (
            len(valid) == len(THRESHOLDS)
            and all(
                row["weighted_difference_pp"] > 0
                for row in valid
            )
        )

        summaries.append(
            {
                "timeframe": tf,
                "test_year": year,
                "threshold_10_difference_pp": records[10][
                    "weighted_difference_pp"
                ],
                "threshold_1_difference_pp": records[1][
                    "weighted_difference_pp"
                ],
                "threshold_3_difference_pp": records[3][
                    "weighted_difference_pp"
                ],
                "threshold_5_difference_pp": records[5][
                    "weighted_difference_pp"
                ],
                "threshold_20_difference_pp": records[20][
                    "weighted_difference_pp"
                ],
                "threshold_30_difference_pp": records[30][
                    "weighted_difference_pp"
                ],
                "threshold_50_difference_pp": records[50][
                    "weighted_difference_pp"
                ],
                "valid_thresholds": len(valid),
                "nonpositive_thresholds": sum(
                    row["weighted_difference_pp"] <= 0
                    for row in valid
                ),
                "threshold_10_eligible_markets": threshold_10[
                    "eligible_markets"
                ],
                "threshold_10_ci_lower_pp": threshold_10[
                    "bootstrap_ci_lower_pp"
                ],
                "threshold_10_ci_upper_pp": threshold_10[
                    "bootstrap_ci_upper_pp"
                ],
                "threshold_10_loo_min_pp": threshold_10[
                    "loo_min_difference_pp"
                ],
                "robust_positive_across_thresholds": all_positive,
                "decision": "HOLD",
            }
        )

    return summaries


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    input_folder = (root / INPUT_REL).resolve()
    output_folder = (root / OUTPUT_REL).resolve()

    if (
        input_folder == output_folder
        or input_folder in output_folder.parents
        or output_folder in input_folder.parents
        or output_folder == root
    ):
        raise ValueError(
            "Unsafe input/output location"
        )

    print("[START]", VERSION, flush=True)

    (
        raw_details,
        previous_summary,
        input_hashes,
    ) = validate_inputs(input_folder)

    groups = parse_strata(raw_details)

    verify_previous_summary(
        groups,
        previous_summary,
    )

    detail_rows = []

    for tf, year in GROUPS:
        for threshold in THRESHOLDS:
            result = analyze_group(
                tf,
                year,
                groups[(tf, year)],
                threshold,
            )

            detail_rows.append(result)

            print(
                "[PROGRESS]",
                tf,
                year,
                "threshold=" + str(threshold),
                "eligible=" + str(
                    result["eligible_strata"]
                ),
                "difference_pp=" + str(
                    result["weighted_difference_pp"]
                ),
                flush=True,
            )

    if len(detail_rows) != 28:
        raise ValueError(
            "Expected 28 threshold comparisons"
        )

    summary_rows = build_summary(
        detail_rows
    )

    if len(summary_rows) != 4:
        raise ValueError(
            "Expected four summaries"
        )

    # Check inputs again before writing results.
    for filename, expected in input_hashes.items():
        if sha256(
            input_folder / filename
        ) != expected:
            raise ValueError(
                "Input changed during evaluation: "
                + filename
            )

    output_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_folder
        / "stratified_robustness_detail.csv"
    )

    summary_path = (
        output_folder
        / "stratified_robustness_summary.csv"
    )

    result_path = (
        output_folder
        / "stratified_robustness_result.json"
    )

    manifest_path = (
        output_folder
        / "stratified_robustness_manifest.json"
    )

    write_csv(
        detail_path,
        DETAIL_FIELDS,
        detail_rows,
    )

    write_csv(
        summary_path,
        SUMMARY_FIELDS,
        summary_rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "input_version": INPUT_VERSION,
        "source_sha256": SOURCE_SHA256,
        "source_rows": 7949826,
        "input_detail_rows": len(raw_details),
        "detail_rows": len(detail_rows),
        "summary_rows": len(summary_rows),
        "thresholds": list(THRESHOLDS),
        "bootstrap_repetitions": BOOTSTRAP_REPETITIONS,
        "bootstrap_unit": "market",
        "bootstrap_seed": BASE_SEED,
        "summary": summary_rows,
        "method": (
            "Market-month stratified harmonic-weighted "
            "hit-rate difference, threshold sensitivity, "
            "market-cluster bootstrap and leave-one-market-out"
        ),
        "limitations": [
            "Bootstrap intervals are descriptive.",
            "Market clusters may share common market-wide shocks.",
            "Signals and horizons may overlap within markets.",
            "The analysis is observational, not randomized.",
            "Different signal timestamps are compared.",
            "The minimum threshold changes the target population.",
            "Candidate selection used prior test results.",
            "Bootstrap repetitions are not independent new data.",
            "No independent future out-of-sample test exists.",
            "No causal or profitability proof exists.",
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

    output_hashes = {
        path.name: sha256(path)
        for path in (
            detail_path,
            summary_path,
            result_path,
        )
    }

    manifest = {
        "version": VERSION,
        "input_version": INPUT_VERSION,
        "source_sha256": SOURCE_SHA256,
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "detail_rows": len(detail_rows),
        "summary_rows": len(summary_rows),
    }

    write_json(
        manifest_path,
        manifest,
    )

    for filename, expected in output_hashes.items():
        if sha256(
            output_folder / filename
        ) != expected:
            raise ValueError(
                "Output SHA256 mismatch: " + filename
            )

    print(
        "[PASS] 28 threshold comparisons",
        flush=True,
    )

    print(
        "[PASS] Four summary groups",
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
