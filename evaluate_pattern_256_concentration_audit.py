
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-11
Pattern 256 concentration and outlier sensitivity audit.

Input:
    data/reports/pattern_256_replication_stability/

Output:
    data/reports/pattern_256_concentration_audit/

Purpose:
    Check whether a small number of markets dominate
    the observed average improvement.

This is a retrospective diagnostic only.
No independent OOS or profitability claims.
No automated trading.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import statistics
import sys

from pathlib import Path


VERSION = "Clean V003-11"
INPUT_VERSION = "Clean V003-10"

INPUT_REL = (
    "data/reports/pattern_256_replication_stability"
)

OUTPUT_REL = (
    "data/reports/pattern_256_concentration_audit"
)

INPUT_FILES = (
    "replication_stability_detail.csv",
    "replication_stability_markets.csv",
    "replication_stability_result.json",
    "replication_stability_transitions.csv",
)

EXCLUSION_COUNTS = (0, 1, 3, 5, 10, 20, 30, 50)


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return list(csv.DictReader(handle))


def atomic_write(path, content):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temp.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temp, path)


def write_json(path, data):
    content = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )

    atomic_write(path, content + "\n")


def write_csv(path, fields, rows):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        lineterminator="\n",
        extrasaction="raise",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(path, buffer.getvalue())


def validate_inputs(folder):
    manifest_path = (
        folder / "replication_stability_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = read_json(manifest_path)

    if manifest.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input manifest version mismatch"
        )

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError(
            "Missing input SHA256 manifest"
        )

    if set(hashes) != set(INPUT_FILES):
        raise ValueError(
            "Unexpected input file set"
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
        folder / "replication_stability_result.json"
    )

    if result.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input result version mismatch"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Input execution status not PASS"
        )

    if result.get("overall_decision") != "HOLD":
        raise ValueError(
            "Input decision not HOLD"
        )

    if result.get("approved_for_live_trading") is not False:
        raise ValueError(
            "Unexpected trading approval"
        )

    if result.get("common_markets") != 209:
        raise ValueError(
            "Expected 209 markets"
        )

    if result.get("positive_both_years") != 26:
        raise ValueError(
            "Expected 26 positive-both markets"
        )

    markets = read_csv(
        folder / "replication_stability_markets.csv"
    )

    if len(markets) != 209:
        raise ValueError(
            "Market row count mismatch"
        )

    return markets, result, hashes


def parse_markets(raw_rows):
    parsed = []
    seen = set()

    for row in raw_rows:
        market = row["market"].strip()

        if not market.startswith("KRW-"):
            raise ValueError(
                "Invalid market: " + market
            )

        if market in seen:
            raise ValueError(
                "Duplicate market: " + market
            )

        seen.add(market)

        d25 = float(
            row["year_2025_difference_pp"]
        )

        d26 = float(
            row["year_2026_difference_pp"]
        )

        minimum_n = int(
            row["minimum_observations"]
        )

        if not (
            math.isfinite(d25)
            and math.isfinite(d26)
        ):
            raise ValueError(
                "Non-finite difference"
            )

        if minimum_n < 10:
            raise ValueError(
                "Insufficient observations"
            )

        mean_difference = (
            d25 + d26
        ) / 2.0

        declared_mean = float(
            row["two_year_mean_difference_pp"]
        )

        if not math.isclose(
            mean_difference,
            declared_mean,
            rel_tol=0,
            abs_tol=0.000001,
        ):
            raise ValueError(
                "Market mean mismatch: " + market
            )

        positive_both = (
            d25 > 0 and d26 > 0
        )

        declared_positive = (
            row["positive_both_years"]
            .strip()
            .lower()
        )

        if declared_positive not in (
            "true",
            "false",
        ):
            raise ValueError(
                "Invalid positive-both value"
            )

        if (
            declared_positive == "true"
        ) != positive_both:
            raise ValueError(
                "Positive-both mismatch: " + market
            )

        parsed.append({
            "market": market,
            "d25": d25,
            "d26": d26,
            "mean": mean_difference,
            "min_n": minimum_n,
        })

    parsed.sort(
        key=lambda item: item["market"]
    )

    if len(parsed) != 209:
        raise ValueError(
            "Parsed market count mismatch"
        )

    if sum(
        item["d25"] > 0
        and item["d26"] > 0
        for item in parsed
    ) != 26:
        raise ValueError(
            "Positive-both count mismatch"
        )

    return parsed


def rounded(value):
    return round(value, 8)


def build_exclusion_rows(markets):
    ordered = sorted(
        markets,
        key=lambda item: (
            -item["mean"],
            item["market"],
        ),
    )

    rows = []

    for excluded in EXCLUSION_COUNTS:
        remaining = ordered[excluded:]

        if not remaining:
            raise ValueError(
                "Empty exclusion cohort"
            )

        values = [
            item["mean"]
            for item in remaining
        ]

        mean = (
            sum(values) / len(values)
        )

        median = statistics.median(
            values
        )

        positive_both = sum(
            item["d25"] > 0
            and item["d26"] > 0
            for item in remaining
        )

        rows.append({
            "excluded_top_k": excluded,
            "remaining_markets": len(remaining),
            "remaining_mean_pp": rounded(mean),
            "remaining_median_pp": rounded(median),
            "positive_both": positive_both,
            "mean_still_positive": mean > 0,
            "decision": "HOLD",
        })

    return ordered, rows


def build_market_rows(ordered):
    positive_sum = sum(
        max(0.0, item["mean"])
        for item in ordered
    )

    rows = []

    for rank, item in enumerate(
        ordered,
        start=1,
    ):
        contribution = (
            max(0.0, item["mean"])
            / positive_sum
            if positive_sum > 0
            else 0.0
        )

        rows.append({
            "rank": rank,
            "market": item["market"],
            "difference_2025_pp": rounded(
                item["d25"]
            ),
            "difference_2026_pp": rounded(
                item["d26"]
            ),
            "mean_difference_pp": rounded(
                item["mean"]
            ),
            "minimum_observations": item["min_n"],
            "share_of_positive_sum": rounded(
                contribution
            ),
            "positive_both_years": (
                item["d25"] > 0
                and item["d26"] > 0
            ),
        })

    return rows, positive_sum


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
        raise ValueError(
            "Unsafe output directory"
        )

    print(
        "[START]",
        VERSION,
        flush=True,
    )

    raw_markets, source_result, input_hashes = (
        validate_inputs(input_dir)
    )

    markets = parse_markets(
        raw_markets
    )

    ordered, exclusion_rows = (
        build_exclusion_rows(markets)
    )

    market_rows, positive_sum = (
        build_market_rows(ordered)
    )

    all_values = [
        item["mean"]
        for item in markets
    ]

    base_mean = (
        sum(all_values) / len(all_values)
    )

    expected_mean = source_result[
        "summary"
    ][0]["mean_two_year_difference_pp"]

    if not math.isclose(
        base_mean,
        expected_mean,
        rel_tol=0,
        abs_tol=0.000001,
    ):
        raise ValueError(
            "Source mean mismatch"
        )

    base_median = statistics.median(
        all_values
    )

    def top_share(count):
        if positive_sum <= 0:
            return None

        return rounded(
            sum(
                max(0.0, item["mean"])
                for item in ordered[:count]
            ) / positive_sum
        )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "input_version": INPUT_VERSION,
        "markets": len(markets),
        "base_mean_pp": rounded(
            base_mean
        ),
        "base_median_pp": rounded(
            base_median
        ),
        "positive_market_count": sum(
            value > 0
            for value in all_values
        ),
        "negative_market_count": sum(
            value < 0
            for value in all_values
        ),
        "zero_market_count": sum(
            value == 0
            for value in all_values
        ),
        "positive_both_years": 26,
        "top_5_share_of_positive_sum": (
            top_share(5)
        ),
        "top_10_share_of_positive_sum": (
            top_share(10)
        ),
        "total_signed_sum_pp": rounded(
            sum(all_values)
        ),
        "top_exclusion_results": (
            exclusion_rows
        ),
        "limitations": [
            "Retrospective data reused from research.",
            "Market-level averages do not prove causality.",
            "Concentration uses equal-market weighting.",
            "Different cohorts and overlapping signals remain.",
            "This is not an independent OOS test.",
            "Net profitability has not been established.",
        ],
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    # Recheck source before writing outputs.
    for filename, expected in input_hashes.items():
        if sha256(
            input_dir / filename
        ) != expected:
            raise ValueError(
                "Input changed: " + filename
            )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    exclusion_path = (
        output_dir / "concentration_exclusion.csv"
    )

    markets_path = (
        output_dir / "concentration_markets.csv"
    )

    result_path = (
        output_dir / "concentration_result.json"
    )

    manifest_path = (
        output_dir / "concentration_manifest.json"
    )

    exclusion_fields = [
        "excluded_top_k",
        "remaining_markets",
        "remaining_mean_pp",
        "remaining_median_pp",
        "positive_both",
        "mean_still_positive",
        "decision",
    ]

    market_fields = [
        "rank",
        "market",
        "difference_2025_pp",
        "difference_2026_pp",
        "mean_difference_pp",
        "minimum_observations",
        "share_of_positive_sum",
        "positive_both_years",
    ]

    write_csv(
        exclusion_path,
        exclusion_fields,
        exclusion_rows,
    )

    write_csv(
        markets_path,
        market_fields,
        market_rows,
    )

    write_json(
        result_path,
        result,
    )

    output_files = (
        exclusion_path,
        markets_path,
        result_path,
    )

    output_hashes = {
        path.name: sha256(path)
        for path in output_files
    }

    manifest = {
        "version": VERSION,
        "input_version": INPUT_VERSION,
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "market_rows": len(market_rows),
        "exclusion_rows": len(exclusion_rows),
    }

    write_json(
        manifest_path,
        manifest,
    )

    for filename, expected in output_hashes.items():
        if sha256(
            output_dir / filename
        ) != expected:
            raise ValueError(
                "Output SHA256 mismatch: " + filename
            )

    print(
        "[PASS]",
        VERSION,
        flush=True,
    )

    print(
        "[PASS] Markets:",
        len(markets),
        flush=True,
    )

    print(
        "[PASS] Base mean:",
        rounded(base_mean),
        flush=True,
    )

    print(
        "[PASS] Base median:",
        rounded(base_median),
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
        raise SystemExit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
