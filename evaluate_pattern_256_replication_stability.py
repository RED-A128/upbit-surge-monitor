
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-10
Cross-year market-level replication stability.

Input:
  data/reports/pattern_256_cross_year_replication/

Output:
  data/reports/pattern_256_replication_stability/

Methods:
  - Market-level positive / negative / zero classifications
  - Two-year sign transition matrix
  - Minimum sample sensitivity
  - Paired market-cluster bootstrap
  - Leave-one-market-out sensitivity
  - Input/output SHA256 verification
  - Atomic output writes

Important:
  This is a retrospective diagnostic.
  It does not prove independent OOS performance,
  causal improvement, or profitability.
  Automated trading is never authorized.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import random
import sys

from collections import Counter
from pathlib import Path


VERSION = "Clean V003-10"
INPUT_VERSION = "Clean V003-9"

SOURCE_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

INPUT_REL = (
    "data/reports/pattern_256_cross_year_replication"
)

OUTPUT_REL = (
    "data/reports/pattern_256_replication_stability"
)

INPUT_FILES = (
    "cross_year_markets.csv",
    "cross_year_summary.csv",
    "cross_year_result.json",
)

THRESHOLDS = (0, 50, 100, 200, 500)
BOOTSTRAP_REPETITIONS = 2000
SEED = 25600310

DETAIL_FIELDS = [
    "minimum_n_each_cohort_each_year",
    "eligible_markets",
    "positive_both",
    "negative_both",
    "positive_2025_negative_2026",
    "negative_2025_positive_2026",
    "positive_2025_zero_2026",
    "zero_2025_positive_2026",
    "negative_2025_zero_2026",
    "zero_2025_negative_2026",
    "zero_both",
    "positive_both_fraction",
    "positive_2025_fraction",
    "positive_2026_fraction",
    "mean_market_difference_2025_pp",
    "mean_market_difference_2026_pp",
    "mean_two_year_difference_pp",
    "median_two_year_difference_pp",
    "bootstrap_valid",
    "bootstrap_positive_both_ci_lower",
    "bootstrap_positive_both_ci_upper",
    "bootstrap_mean_difference_ci_lower_pp",
    "bootstrap_mean_difference_ci_upper_pp",
    "leave_one_out_min_mean_pp",
    "leave_one_out_max_mean_pp",
    "decision",
]

TRANSITION_FIELDS = [
    "year_2025_sign",
    "year_2026_sign",
    "markets",
    "fraction_of_all_markets",
]

MARKET_FIELDS = [
    "market",
    "year_2025_sign",
    "year_2026_sign",
    "year_2025_difference_pp",
    "year_2026_difference_pp",
    "two_year_mean_difference_pp",
    "year_2025_baseline_n",
    "year_2025_combined_n",
    "year_2026_baseline_n",
    "year_2026_combined_n",
    "minimum_observations",
    "positive_both_years",
]


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return list(csv.DictReader(handle))


def write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")

    with temp.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            value,
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


def number(value, field):
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


def close(a, b, tolerance=0.000001):
    return math.isclose(
        a,
        b,
        rel_tol=0,
        abs_tol=tolerance,
    )


def verify_inputs(folder):
    manifest_path = folder / "cross_year_manifest.json"

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = read_json(manifest_path)

    if manifest.get("version") != INPUT_VERSION:
        raise ValueError("Input manifest version mismatch")

    if manifest.get("source_sha256") != SOURCE_SHA256:
        raise ValueError("Source SHA256 mismatch")

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError("Missing input SHA256 values")

    if set(hashes) != set(INPUT_FILES):
        raise ValueError("Unexpected input file set")

    for filename in INPUT_FILES:
        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        if sha256(path) != hashes[filename]:
            raise ValueError(
                "Input SHA256 mismatch: " + filename
            )

    result = read_json(
        folder / "cross_year_result.json"
    )

    if result.get("version") != INPUT_VERSION:
        raise ValueError("Input result version mismatch")

    if result.get("execution_status") != "PASS":
        raise ValueError("Input execution not PASS")

    if result.get("overall_decision") != "HOLD":
        raise ValueError("Input decision not HOLD")

    if result.get("approved_for_live_trading") is not False:
        raise ValueError("Unsafe trading approval")

    if result.get("source_sha256") != SOURCE_SHA256:
        raise ValueError("Result source hash mismatch")

    markets = read_csv(
        folder / "cross_year_markets.csv"
    )

    summaries = read_csv(
        folder / "cross_year_summary.csv"
    )

    if len(markets) != 209:
        raise ValueError("Expected 209 common markets")

    if len(summaries) != 2:
        raise ValueError("Expected two year summaries")

    if result.get("common_market_count") != len(markets):
        raise ValueError("Market count mismatch")

    if result.get("markets_positive_both_years") != 26:
        raise ValueError("Positive-both reference mismatch")

    return markets, summaries, hashes


def sign(value):
    # Input differences were published to 8 decimal places.
    # A value that rounds to zero is treated as zero.
    if value > 0:
        return "POSITIVE"
    if value < 0:
        return "NEGATIVE"
    return "ZERO"


def parse_markets(raw_rows):
    parsed = []
    seen = set()

    for row in raw_rows:
        market = row["market"].strip()

        if not market.startswith("KRW-"):
            raise ValueError("Invalid market")

        if market in seen:
            raise ValueError("Duplicate market")

        seen.add(market)

        d25 = number(
            row["year_2025_difference_pp"],
            "year_2025_difference_pp",
        )

        d26 = number(
            row["year_2026_difference_pp"],
            "year_2026_difference_pp",
        )

        counts = {}

        for field in (
            "year_2025_baseline_n",
            "year_2025_combined_n",
            "year_2026_baseline_n",
            "year_2026_combined_n",
        ):
            counts[field] = integer(
                row[field],
                field,
            )

            if counts[field] < 10:
                raise ValueError(
                    "Market observations below 10"
                )

        expected_positive = (
            d25 > 0 and d26 > 0
        )

        declared = (
            row["positive_both_years"]
            .strip()
            .lower()
        )

        if declared not in ("true", "false"):
            raise ValueError("Invalid positive_both_years")

        if (declared == "true") != expected_positive:
            raise ValueError("Positive-both mismatch")

        parsed.append({
            "market": market,
            "d25": d25,
            "d26": d26,
            "mean": (d25 + d26) / 2.0,
            "min_n": min(counts.values()),
            **counts,
        })

    parsed.sort(key=lambda item: item["market"])

    return parsed


def verify_summary(markets, summaries):
    indexed = {}

    for row in summaries:
        year = integer(row["year"], "year")

        if year in indexed:
            raise ValueError("Duplicate summary year")

        indexed[year] = row

    if set(indexed) != {2025, 2026}:
        raise ValueError("Invalid summary years")

    for year, key in (
        (2025, "d25"),
        (2026, "d26"),
    ):
        row = indexed[year]

        if integer(
            row["common_markets"],
            "common_markets",
        ) != len(markets):
            raise ValueError("Summary market count mismatch")

        checks = {
            "positive_markets": sum(
                item[key] > 0 for item in markets
            ),
            "negative_markets": sum(
                item[key] < 0 for item in markets
            ),
            "zero_markets": sum(
                item[key] == 0 for item in markets
            ),
        }

        for field, expected in checks.items():
            actual = integer(row[field], field)

            if actual != expected:
                raise ValueError(
                    "Summary mismatch: " + field
                )

        if row["decision"] != "HOLD":
            raise ValueError("Unexpected summary decision")


def percentile(sorted_values, fraction):
    if not sorted_values:
        return None

    position = fraction * (len(sorted_values) - 1)
    low = math.floor(position)
    high = math.ceil(position)

    if low == high:
        return sorted_values[low]

    weight = position - low

    return (
        sorted_values[low] * (1 - weight)
        + sorted_values[high] * weight
    )


def bootstrap(rows, seed):
    count = len(rows)

    if count < 2:
        return {
            "valid": 0,
            "fraction_lower": None,
            "fraction_upper": None,
            "mean_lower": None,
            "mean_upper": None,
        }

    rng = random.Random(seed)
    fractions = []
    means = []

    for _ in range(BOOTSTRAP_REPETITIONS):
        positive_both = 0
        total_mean = 0.0

        for _ in range(count):
            item = rows[rng.randrange(count)]

            positive_both += int(
                item["d25"] > 0
                and item["d26"] > 0
            )

            total_mean += item["mean"]

        fractions.append(positive_both / count)
        means.append(total_mean / count)

    fractions.sort()
    means.sort()

    return {
        "valid": BOOTSTRAP_REPETITIONS,
        "fraction_lower": percentile(
            fractions, 0.025
        ),
        "fraction_upper": percentile(
            fractions, 0.975
        ),
        "mean_lower": percentile(
            means, 0.025
        ),
        "mean_upper": percentile(
            means, 0.975
        ),
    }


def median(values):
    ordered = sorted(values)

    if not ordered:
        return None

    count = len(ordered)
    midpoint = count // 2

    if count % 2:
        return ordered[midpoint]

    return (
        ordered[midpoint - 1]
        + ordered[midpoint]
    ) / 2.0


def rounded(value):
    if value is None:
        return None

    return round(value, 8)


def analyze_threshold(markets, threshold):
    selected = [
        item for item in markets
        if item["min_n"] >= threshold
    ]

    count = len(selected)

    transitions = Counter(
        (sign(item["d25"]), sign(item["d26"]))
        for item in selected
    )

    positive_both = transitions[
        ("POSITIVE", "POSITIVE")
    ]

    means = [
        item["mean"] for item in selected
    ]

    mean25 = (
        sum(item["d25"] for item in selected) / count
        if count else None
    )

    mean26 = (
        sum(item["d26"] for item in selected) / count
        if count else None
    )

    overall_mean = (
        sum(means) / count
        if count else None
    )

    loo_values = []

    if count >= 2:
        total = sum(means)

        loo_values = [
            (total - value) / (count - 1)
            for value in means
        ]

    bootstrap_result = bootstrap(
        selected,
        SEED + threshold,
    )

    return {
        "minimum_n_each_cohort_each_year": threshold,
        "eligible_markets": count,
        "positive_both": positive_both,
        "negative_both": transitions[
            ("NEGATIVE", "NEGATIVE")
        ],
        "positive_2025_negative_2026": transitions[
            ("POSITIVE", "NEGATIVE")
        ],
        "negative_2025_positive_2026": transitions[
            ("NEGATIVE", "POSITIVE")
        ],
        "positive_2025_zero_2026": transitions[
            ("POSITIVE", "ZERO")
        ],
        "zero_2025_positive_2026": transitions[
            ("ZERO", "POSITIVE")
        ],
        "negative_2025_zero_2026": transitions[
            ("NEGATIVE", "ZERO")
        ],
        "zero_2025_negative_2026": transitions[
            ("ZERO", "NEGATIVE")
        ],
        "zero_both": transitions[
            ("ZERO", "ZERO")
        ],
        "positive_both_fraction": rounded(
            positive_both / count
            if count else None
        ),
        "positive_2025_fraction": rounded(
            sum(item["d25"] > 0 for item in selected) / count
            if count else None
        ),
        "positive_2026_fraction": rounded(
            sum(item["d26"] > 0 for item in selected) / count
            if count else None
        ),
        "mean_market_difference_2025_pp": rounded(
            mean25
        ),
        "mean_market_difference_2026_pp": rounded(
            mean26
        ),
        "mean_two_year_difference_pp": rounded(
            overall_mean
        ),
        "median_two_year_difference_pp": rounded(
            median(means)
        ),
        "bootstrap_valid": bootstrap_result["valid"],
        "bootstrap_positive_both_ci_lower": rounded(
            bootstrap_result["fraction_lower"]
        ),
        "bootstrap_positive_both_ci_upper": rounded(
            bootstrap_result["fraction_upper"]
        ),
        "bootstrap_mean_difference_ci_lower_pp": rounded(
            bootstrap_result["mean_lower"]
        ),
        "bootstrap_mean_difference_ci_upper_pp": rounded(
            bootstrap_result["mean_upper"]
        ),
        "leave_one_out_min_mean_pp": rounded(
            min(loo_values) if loo_values else None
        ),
        "leave_one_out_max_mean_pp": rounded(
            max(loo_values) if loo_values else None
        ),
        "decision": "HOLD",
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    input_dir = (root / INPUT_REL).resolve()
    output_dir = (root / OUTPUT_REL).resolve()

    if (
        input_dir == output_dir
        or input_dir in output_dir.parents
        or output_dir in input_dir.parents
        or output_dir == root
    ):
        raise ValueError("Unsafe output directory")

    print("[START]", VERSION, flush=True)

    raw_markets, summaries, hashes = verify_inputs(
        input_dir
    )

    markets = parse_markets(raw_markets)

    verify_summary(markets, summaries)

    if sum(
        item["d25"] > 0 and item["d26"] > 0
        for item in markets
    ) != 26:
        raise ValueError("Replication count mismatch")

    detail_rows = []

    for threshold in THRESHOLDS:
        result = analyze_threshold(
            markets,
            threshold,
        )

        detail_rows.append(result)

        print(
            "[PROGRESS]",
            "threshold=" + str(threshold),
            "markets=" + str(
                result["eligible_markets"]
            ),
            "positive_both=" + str(
                result["positive_both"]
            ),
            flush=True,
        )

    if len(detail_rows) != len(THRESHOLDS):
        raise ValueError("Threshold count mismatch")

    if detail_rows[0]["eligible_markets"] != 209:
        raise ValueError("Base market count mismatch")

    if detail_rows[0]["positive_both"] != 26:
        raise ValueError("Base replication mismatch")

    transition_counts = Counter(
        (sign(item["d25"]), sign(item["d26"]))
        for item in markets
    )

    transition_rows = []

    for first in ("POSITIVE", "NEGATIVE", "ZERO"):
        for second in ("POSITIVE", "NEGATIVE", "ZERO"):
            count = transition_counts[
                (first, second)
            ]

            transition_rows.append({
                "year_2025_sign": first,
                "year_2026_sign": second,
                "markets": count,
                "fraction_of_all_markets": rounded(
                    count / len(markets)
                ),
            })

    if sum(
        item["markets"] for item in transition_rows
    ) != 209:
        raise ValueError("Transition total mismatch")

    market_rows = []

    for item in markets:
        market_rows.append({
            "market": item["market"],
            "year_2025_sign": sign(item["d25"]),
            "year_2026_sign": sign(item["d26"]),
            "year_2025_difference_pp": item["d25"],
            "year_2026_difference_pp": item["d26"],
            "two_year_mean_difference_pp": rounded(
                item["mean"]
            ),
            "year_2025_baseline_n": item[
                "year_2025_baseline_n"
            ],
            "year_2025_combined_n": item[
                "year_2025_combined_n"
            ],
            "year_2026_baseline_n": item[
                "year_2026_baseline_n"
            ],
            "year_2026_combined_n": item[
                "year_2026_combined_n"
            ],
            "minimum_observations": item["min_n"],
            "positive_both_years": (
                item["d25"] > 0
                and item["d26"] > 0
            ),
        })

    # Verify the source has not changed during analysis.
    for filename, expected in hashes.items():
        if sha256(input_dir / filename) != expected:
            raise ValueError(
                "Input changed during evaluation: "
                + filename
            )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_dir / "replication_stability_detail.csv"
    )

    transition_path = (
        output_dir / "replication_stability_transitions.csv"
    )

    markets_path = (
        output_dir / "replication_stability_markets.csv"
    )

    result_path = (
        output_dir / "replication_stability_result.json"
    )

    manifest_path = (
        output_dir / "replication_stability_manifest.json"
    )

    write_csv(
        detail_path,
        DETAIL_FIELDS,
        detail_rows,
    )

    write_csv(
        transition_path,
        TRANSITION_FIELDS,
        transition_rows,
    )

    write_csv(
        markets_path,
        MARKET_FIELDS,
        market_rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "input_version": INPUT_VERSION,
        "source_sha256": SOURCE_SHA256,
        "common_markets": 209,
        "positive_both_years": 26,
        "thresholds": list(THRESHOLDS),
        "bootstrap_repetitions": BOOTSTRAP_REPETITIONS,
        "bootstrap_unit": "market",
        "bootstrap_seed": SEED,
        "detail_rows": len(detail_rows),
        "transition_rows": len(transition_rows),
        "market_rows": len(market_rows),
        "summary": detail_rows,
        "limitations": [
            "This is a retrospective diagnostic.",
            "Both years were already used in research.",
            "This is not an independent future OOS test.",
            "The common-market filter creates survivorship bias.",
            "The market-level differences were rounded upstream.",
            "Zero means a rounded difference of zero.",
            "Each market is equally weighted in this diagnostic.",
            "Market observations may have different precision.",
            "Signals may overlap within markets.",
            "Market-wide shocks can correlate different markets.",
            "Bootstrap intervals are descriptive only.",
            "Hit-rate differences do not prove profitability.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    write_json(result_path, result)

    output_hashes = {
        path.name: sha256(path)
        for path in (
            detail_path,
            transition_path,
            markets_path,
            result_path,
        )
    }

    manifest = {
        "version": VERSION,
        "input_version": INPUT_VERSION,
        "source_sha256": SOURCE_SHA256,
        "input_sha256": hashes,
        "output_sha256": output_hashes,
        "detail_rows": len(detail_rows),
        "transition_rows": len(transition_rows),
        "market_rows": len(market_rows),
    }

    write_json(
        manifest_path,
        manifest,
    )

    for filename, expected in output_hashes.items():
        if sha256(output_dir / filename) != expected:
            raise ValueError(
                "Output SHA256 mismatch: " + filename
            )

    print("[PASS]", VERSION, flush=True)
    print("[PASS] Common markets: 209", flush=True)
    print("[PASS] Positive both years: 26", flush=True)
    print("[PASS] SHA256 integrity", flush=True)
    print("[DECISION] HOLD", flush=True)

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
