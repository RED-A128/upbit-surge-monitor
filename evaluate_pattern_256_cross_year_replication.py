
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-9
Pattern 256 cross-year common-market replication.

Research only. No automated trading.

Uses validated V003-7 and V003-8 results.
Restricts h1 comparisons to markets eligible in both
2025 and 2026, with >=10 observations per cohort
within each market-month stratum.

This is a retrospective replication diagnostic,
NOT an independent out-of-sample evaluation.
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

VERSION = "Clean V003-9"
ROOT_SOURCE_HASH = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

V7 = "data/reports/pattern_256_stratified_comparison"
V8 = "data/reports/pattern_256_stratified_robustness"
OUTPUT = "data/reports/pattern_256_cross_year_replication"

INPUTS = {
    "Clean V003-7": (
        V7,
        "stratified_manifest.json",
        [
            "stratified_detail.csv",
            "stratified_summary.csv",
            "stratified_result.json",
        ],
    ),
    "Clean V003-8": (
        V8,
        "stratified_robustness_manifest.json",
        [
            "stratified_robustness_detail.csv",
            "stratified_robustness_summary.csv",
            "stratified_robustness_result.json",
        ],
    ),
}

MARKET_FIELDS = [
    "market",
    "year_2025_strata",
    "year_2026_strata",
    "year_2025_baseline_n",
    "year_2025_combined_n",
    "year_2026_baseline_n",
    "year_2026_combined_n",
    "year_2025_difference_pp",
    "year_2026_difference_pp",
    "positive_both_years",
]

SUMMARY_FIELDS = [
    "year",
    "common_markets",
    "eligible_strata",
    "baseline_n",
    "baseline_hits",
    "combined_n",
    "combined_hits",
    "weighted_difference_pp",
    "positive_markets",
    "negative_markets",
    "zero_markets",
    "decision",
]


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(
            value, f, indent=2, ensure_ascii=False,
            sort_keys=True, allow_nan=False,
        )
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def write_csv(path, fields, rows):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, extrasaction="raise"
        )
        writer.writeheader()
        writer.writerows(rows)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def verify_input(root, version, folder_rel, manifest_name, filenames):
    folder = (root / folder_rel).resolve()
    manifest = load_json(folder / manifest_name)

    if manifest.get("version") != version:
        raise ValueError("Input version mismatch: " + version)

    if manifest.get("source_sha256") != ROOT_SOURCE_HASH:
        raise ValueError("Source hash mismatch: " + version)

    hashes = manifest.get("output_sha256")
    if not isinstance(hashes, dict) or set(hashes) != set(filenames):
        raise ValueError("Invalid manifest files: " + version)

    for filename in filenames:
        path = folder / filename
        if not path.is_file() or sha256(path) != hashes[filename]:
            raise ValueError("SHA256 mismatch: " + str(path))

    result_name = next(
        name for name in filenames if name.endswith("_result.json")
    )
    result = load_json(folder / result_name)

    if (
        result.get("version") != version
        or result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
        or result.get("approved_for_live_trading") is not False
    ):
        raise ValueError("Input result validation failed: " + version)

    return folder, hashes


def counts(row):
    values = tuple(
        int(row[field]) for field in (
            "baseline_n",
            "baseline_hits",
            "combined_n",
            "combined_hits",
        )
    )
    bn, bh, cn, ch = values
    if min(values) < 0 or bh > bn or ch > cn:
        raise ValueError("Invalid cohort counts")
    return values


def components(row):
    bn, bh, cn, ch = counts(row)
    if bn < 10 or cn < 10:
        raise ValueError("Ineligible stratum")
    weight = 2.0 * bn * cn / (bn + cn)
    difference = 100.0 * (ch / cn - bh / bn)
    return weight * difference, weight


def weighted(rows):
    numerator = 0.0
    denominator = 0.0
    for row in rows:
        n, d = components(row)
        numerator += n
        denominator += d
    if denominator <= 0:
        return None
    return numerator / denominator


def rounded(value):
    return None if value is None else round(value, 8)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    output = (root / OUTPUT).resolve()

    if output == root:
        raise ValueError("Unsafe output path")

    validated = {}
    for version, (folder, manifest_name, files) in INPUTS.items():
        validated[version] = verify_input(
            root, version, folder, manifest_name, files
        )

    v7_folder, v7_hashes = validated["Clean V003-7"]
    v8_folder, v8_hashes = validated["Clean V003-8"]

    detail = load_csv(v7_folder / "stratified_detail.csv")
    robust = load_csv(
        v8_folder / "stratified_robustness_detail.csv"
    )

    if len(detail) != 6726 or len(robust) != 28:
        raise ValueError("Unexpected input row count")

    grouped = defaultdict(list)
    seen = set()

    for row in detail:
        tf = row["timeframe"]
        year = int(row["test_year"])
        market = row["market"]
        month = row["month"]
        key = (tf, year, market, month)

        if key in seen:
            raise ValueError("Duplicate stratum")
        seen.add(key)

        bn, bh, cn, ch = counts(row)
        eligible = bn >= 10 and cn >= 10

        if (
            row["eligible_stratum"].strip().lower()
            != str(eligible).lower()
        ):
            raise ValueError("Eligibility mismatch")

        if eligible:
            if not row["difference_pp"] or not row["harmonic_weight"]:
                raise ValueError("Missing published metrics")

            expected_difference = 100.0 * (ch / cn - bh / bn)
            expected_weight = 2.0 * bn * cn / (bn + cn)

            if not math.isclose(
                float(row["difference_pp"]),
                expected_difference,
                abs_tol=1e-6,
                rel_tol=0,
            ):
                raise ValueError("Published difference mismatch")

            if not math.isclose(
                float(row["harmonic_weight"]),
                expected_weight,
                abs_tol=1e-6,
                rel_tol=0,
            ):
                raise ValueError("Published weight mismatch")

            if tf == "h1" and year in (2025, 2026):
                grouped[(year, market)].append(row)

    markets_2025 = {
        market for year, market in grouped if year == 2025
    }
    markets_2026 = {
        market for year, market in grouped if year == 2026
    }
    common = sorted(markets_2025 & markets_2026)

    if not common:
        raise ValueError("No common eligible markets")

    # Independently reproduce V003-8 threshold=10
    # before calculating the common-market subset.
    for year in (2025, 2026):
        reference = [
            r for r in robust
            if (
                r["timeframe"] == "h1"
                and int(r["test_year"]) == year
                and int(r["minimum_per_cohort"]) == 10
            )
        ]
        if len(reference) != 1:
            raise ValueError("Missing robustness reference")

        all_rows = [
            row for (y, _), rows in grouped.items()
            if y == year for row in rows
        ]
        reproduced = weighted(all_rows)
        published = float(reference[0]["weighted_difference_pp"])

        if reproduced is None or not math.isclose(
            reproduced, published, abs_tol=1e-6, rel_tol=0
        ):
            raise ValueError(
                f"V003-8 threshold=10 mismatch: {year}"
            )

    market_rows = []
    year_rows = {2025: [], 2026: []}
    positive_both = 0
    negative_either = 0
    zero_either = 0

    for market in common:
        first = grouped[(2025, market)]
        second = grouped[(2026, market)]
        d25 = weighted(first)
        d26 = weighted(second)

        if d25 is None or d26 is None:
            raise ValueError("Missing market weighted difference")

        both = d25 > 0 and d26 > 0
        positive_both += int(both)
        negative_either += int(d25 < 0 or d26 < 0)
        zero_either += int(d25 == 0 or d26 == 0)

        for year, rows in ((2025, first), (2026, second)):
            year_rows[year].extend(rows)

        def totals(rows):
            return (
                sum(int(r["baseline_n"]) for r in rows),
                sum(int(r["combined_n"]) for r in rows),
            )

        b25, c25 = totals(first)
        b26, c26 = totals(second)

        market_rows.append({
            "market": market,
            "year_2025_strata": len(first),
            "year_2026_strata": len(second),
            "year_2025_baseline_n": b25,
            "year_2025_combined_n": c25,
            "year_2026_baseline_n": b26,
            "year_2026_combined_n": c26,
            "year_2025_difference_pp": rounded(d25),
            "year_2026_difference_pp": rounded(d26),
            "positive_both_years": both,
        })

    summary = []
    for year in (2025, 2026):
        rows = year_rows[year]
        per_market = [
            weighted(grouped[(year, market)])
            for market in common
        ]

        summary.append({
            "year": year,
            "common_markets": len(common),
            "eligible_strata": len(rows),
            "baseline_n": sum(int(r["baseline_n"]) for r in rows),
            "baseline_hits": sum(int(r["baseline_hits"]) for r in rows),
            "combined_n": sum(int(r["combined_n"]) for r in rows),
            "combined_hits": sum(int(r["combined_hits"]) for r in rows),
            "weighted_difference_pp": rounded(weighted(rows)),
            "positive_markets": sum(x > 0 for x in per_market),
            "negative_markets": sum(x < 0 for x in per_market),
            "zero_markets": sum(x == 0 for x in per_market),
            "decision": "HOLD",
        })

    # Detect input mutation before writing outputs.
    for folder, hashes in (validated["Clean V003-7"],
                           validated["Clean V003-8"]):
        for name, expected in hashes.items():
            if sha256(folder / name) != expected:
                raise ValueError("Input changed during execution")

    output.mkdir(parents=True, exist_ok=True)
    markets_path = output / "cross_year_markets.csv"
    summary_path = output / "cross_year_summary.csv"
    result_path = output / "cross_year_result.json"
    manifest_path = output / "cross_year_manifest.json"

    write_csv(markets_path, MARKET_FIELDS, market_rows)
    write_csv(summary_path, SUMMARY_FIELDS, summary)

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_sha256": ROOT_SOURCE_HASH,
        "input_versions": ["Clean V003-7", "Clean V003-8"],
        "timeframe": "h1",
        "minimum_per_cohort_per_market_month": 10,
        "common_market_count": len(common),
        "market_rows": len(market_rows),
        "summary_rows": 2,
        "markets_positive_both_years": positive_both,
        "markets_negative_either_year": negative_either,
        "markets_zero_either_year": zero_either,
        "summary": summary,
        "limitations": [
            "Both years were previously used in research.",
            "This is not a fresh independent holdout.",
            "Common-market filtering creates survivorship bias.",
            "The cohorts use different signal timestamps.",
            "Market-month strata may contain overlapping events.",
            "Shared market shocks and selection bias remain.",
            "Hit rate does not establish net profitability.",
        ],
        "independent_oos_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }
    write_json(result_path, result)

    hashes = {
        path.name: sha256(path)
        for path in (markets_path, summary_path, result_path)
    }

    write_json(manifest_path, {
        "version": VERSION,
        "source_sha256": ROOT_SOURCE_HASH,
        "input_sha256": {
            "Clean V003-7": v7_hashes,
            "Clean V003-8": v8_hashes,
        },
        "output_sha256": hashes,
        "market_rows": len(market_rows),
        "summary_rows": 2,
    })

    for name, expected in hashes.items():
        if sha256(output / name) != expected:
            raise ValueError("Output SHA256 mismatch: " + name)

    print(f"[PASS] {VERSION}")
    print(f"[PASS] Common markets: {len(common)}")
    print(f"[PASS] Positive both years: {positive_both}")
    for row in summary:
        print(
            f"[RESULT] {row['year']} "
            f"weighted_difference_pp="
            f"{row['weighted_difference_pp']}"
        )
    print("[PASS] SHA256 integrity")
    print("[DECISION] HOLD")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("[INTERRUPTED]", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print("[FAIL]", str(exc), file=sys.stderr)
        sys.exit(1)
