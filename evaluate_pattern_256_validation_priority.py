
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-15
Pattern 256 independent validation priority audit.

Input:
  data/reports/pattern_256_candidate_stability/

Output:
  data/reports/pattern_256_validation_priority/

This is a deterministic research queue, not a trading signal.
No future information, price prediction, or live trading.
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

from collections import Counter
from pathlib import Path


VERSION = "Clean V003-15"
SOURCE_VERSION = "Clean V003-14"

SOURCE_REL = (
    "data/reports/pattern_256_candidate_stability"
)
OUTPUT_REL = (
    "data/reports/pattern_256_validation_priority"
)

SOURCE_FILES = (
    "candidate_stability_markets.csv",
    "candidate_stability_summary.csv",
    "candidate_stability_result.json",
)

OUTPUT_FILES = (
    "validation_priority_markets.csv",
    "validation_priority_summary.csv",
    "validation_priority_result.json",
)

MARKET_FIELDS = [
    "market",
    "previous_classification",
    "minimum_observations",
    "year_2025_difference_pp",
    "year_2026_difference_pp",
    "minimum_yearly_difference_pp",
    "absolute_yearly_gap_pp",
    "original_candidate",
    "positive_both_years",
    "passes_strict_200_05",
    "priority_tier",
    "priority_reason",
    "sample_deficit_to_200",
    "sample_deficit_to_500",
    "effect_deficit_to_05_pp",
    "yearly_effect_asymmetry_ratio",
    "requires_new_independent_data",
    "independent_oos_proven",
    "approved_for_live_trading",
]

SUMMARY_FIELDS = [
    "priority_tier",
    "market_count",
    "original_candidate_count",
    "median_minimum_observations",
    "median_minimum_yearly_effect_pp",
    "median_yearly_gap_pp",
    "decision",
]


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        return list(csv.DictReader(f))


def write_atomic(path, content):
    tmp = path.with_name(path.name + ".tmp")
    try:
        with tmp.open(
            "w", encoding="utf-8", newline=""
        ) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def write_json(path, data):
    write_atomic(
        path,
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def write_csv(path, fields, rows):
    buf = io.StringIO(newline="")
    writer = csv.DictWriter(
        buf,
        fieldnames=fields,
        extrasaction="raise",
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    write_atomic(path, buf.getvalue())


def numeric(value, name):
    try:
        x = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Invalid numeric field: " + name
        ) from exc
    if not math.isfinite(x):
        raise ValueError(
            "Nonfinite numeric field: " + name
        )
    return x


def integer(value, name):
    try:
        x = int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Invalid integer: " + name
        ) from exc
    if x < 0:
        raise ValueError(
            "Negative integer: " + name
        )
    return x


def boolean(value, name):
    if value is True or value == "True":
        return True
    if value is False or value == "False":
        return False
    raise ValueError("Invalid boolean: " + name)


def round8(x):
    return None if x is None else round(x, 8)


def verify_source(folder):
    manifest_file = (
        folder / "candidate_stability_manifest.json"
    )
    if not manifest_file.is_file():
        raise FileNotFoundError(manifest_file)

    manifest = read_json(manifest_file)
    result = read_json(
        folder / "candidate_stability_result.json"
    )

    if manifest.get("version") != SOURCE_VERSION:
        raise ValueError("Source manifest version")

    if result.get("version") != SOURCE_VERSION:
        raise ValueError("Source result version")

    if result.get("execution_status") != "PASS":
        raise ValueError("Source execution status")

    if result.get("overall_decision") != "HOLD":
        raise ValueError("Source decision")

    for field in (
        "approved_for_live_trading",
        "independent_oos_proven",
        "statistical_independence_proven",
        "causal_improvement_proven",
        "profitability_proven",
    ):
        if result.get(field) is not False:
            raise ValueError("Unsafe source: " + field)

    if result.get("market_count") != 209:
        raise ValueError("Source market count")

    if result.get("original_candidate_count") != 22:
        raise ValueError("Source candidate count")

    if result.get("strict_candidate_count") != 1:
        raise ValueError("Source strict count")

    if result.get("strict_candidate_markets") != [
        "KRW-DKA"
    ]:
        raise ValueError("Source strict market")

    hashes = manifest.get("output_sha256")
    if not isinstance(hashes, dict):
        raise ValueError("Source SHA256 missing")

    if set(hashes) != set(SOURCE_FILES):
        raise ValueError("Source file set mismatch")

    for name in SOURCE_FILES:
        path = folder / name
        if not path.is_file():
            raise FileNotFoundError(path)
        if sha256(path) != hashes[name]:
            raise ValueError(
                "Source SHA256 mismatch: " + name
            )

    markets = read_csv(
        folder / "candidate_stability_markets.csv"
    )
    summary = read_csv(
        folder / "candidate_stability_summary.csv"
    )

    if len(markets) != 209 or len(summary) != 8:
        raise ValueError("Source CSV row counts")

    if len({r["market"] for r in markets}) != 209:
        raise ValueError("Duplicate source markets")

    if len({r["group"] for r in summary}) != 8:
        raise ValueError("Duplicate summary groups")

    if any(r["decision"] != "HOLD" for r in summary):
        raise ValueError("Unsafe source summary")

    return markets, hashes


def parse_markets(raw):
    parsed = []

    valid_classes = {
        "RESEARCH_CANDIDATE",
        "INSUFFICIENT_SAMPLE",
        "MIXED_OR_UNSTABLE",
        "NO_REPLICATED_IMPROVEMENT",
    }

    for r in raw:
        market = r["market"].strip()
        if not market.startswith("KRW-"):
            raise ValueError("Invalid market: " + market)

        cls = r["previous_classification"]
        if cls not in valid_classes:
            raise ValueError(
                "Invalid classification: " + market
            )

        n = integer(
            r["minimum_observations"],
            "minimum_observations",
        )
        d25 = numeric(
            r["year_2025_difference_pp"],
            "year_2025_difference_pp",
        )
        d26 = numeric(
            r["year_2026_difference_pp"],
            "year_2026_difference_pp",
        )
        minimum = numeric(
            r["minimum_yearly_difference_pp"],
            "minimum_yearly_difference_pp",
        )
        gap = numeric(
            r["absolute_yearly_gap_pp"],
            "absolute_yearly_gap_pp",
        )

        if abs(minimum - min(d25, d26)) > 2e-8:
            raise ValueError(
                "Effect mismatch: " + market
            )
        if abs(gap - abs(d25 - d26)) > 2e-8:
            raise ValueError(
                "Yearly gap mismatch: " + market
            )

        original = (
            cls == "RESEARCH_CANDIDATE"
        )
        positive = d25 > 0 and d26 > 0
        strict = n >= 200 and minimum > 0.5

        expected = {
            "original_candidate": original,
            "positive_both_years": positive,
            "passes_strict_200_05": strict,
            "approved_for_live_trading": False,
        }

        for field, value in expected.items():
            if boolean(r[field], field) != value:
                raise ValueError(
                    "Flag mismatch: "
                    + market + "/" + field
                )

        if original != (n >= 100 and positive):
            raise ValueError(
                "Original candidate mismatch: " + market
            )

        if integer(
            r["additional_observations_to_200"],
            "additional_observations_to_200",
        ) != max(0, 200 - n):
            raise ValueError(
                "Sample deficit mismatch: " + market
            )

        if integer(
            r["additional_observations_to_500"],
            "additional_observations_to_500",
        ) != max(0, 500 - n):
            raise ValueError(
                "Sample deficit mismatch: " + market
            )

        parsed.append({
            "market": market,
            "classification": cls,
            "n": n,
            "d25": d25,
            "d26": d26,
            "minimum": minimum,
            "gap": gap,
            "original": original,
            "positive": positive,
            "strict": strict,
        })

    parsed.sort(key=lambda x: x["market"])

    if len(parsed) != 209:
        raise ValueError("Parsed market count")

    if sum(x["original"] for x in parsed) != 22:
        raise ValueError("Parsed candidate count")

    if sum(x["positive"] for x in parsed) != 26:
        raise ValueError("Parsed positive count")

    if [
        x["market"] for x in parsed if x["strict"]
    ] != ["KRW-DKA"]:
        raise ValueError("Parsed strict market")

    return parsed


def assign_priority(x):
    """
    These tiers prioritize independent verification work,
    not buying, selling, or expected return.
    """
    if x["strict"]:
        return (
            "P1_STRICT_REPLICATION",
            "Historical strict threshold passed; "
            "fresh independent OOS required",
        )

    if x["original"] and x["minimum"] > 0.5:
        return (
            "P2_SAMPLE_EXTENSION",
            "Both years exceed 0.5pp but "
            "minimum sample below 200",
        )

    if x["original"]:
        return (
            "P3_EFFECT_RECHECK",
            "Original candidate with weak "
            "minimum yearly improvement",
        )

    if x["positive"]:
        return (
            "P4_SMALL_SAMPLE",
            "Both years positive but fewer "
            "than 100 minimum observations",
        )

    if x["d25"] * x["d26"] < 0:
        return (
            "P5_DIRECTION_CONFLICT",
            "Yearly effect directions disagree",
        )

    return (
        "P6_NO_REPLICATED_EFFECT",
        "No replicated positive yearly effect",
    )


def build_rows(markets):
    rows = []
    for x in markets:
        tier, reason = assign_priority(x)
        asymmetry = (
            x["gap"] / x["minimum"]
            if x["minimum"] > 0 else None
        )
        rows.append({
            "market": x["market"],
            "previous_classification": x[
                "classification"
            ],
            "minimum_observations": x["n"],
            "year_2025_difference_pp": round8(
                x["d25"]
            ),
            "year_2026_difference_pp": round8(
                x["d26"]
            ),
            "minimum_yearly_difference_pp": round8(
                x["minimum"]
            ),
            "absolute_yearly_gap_pp": round8(
                x["gap"]
            ),
            "original_candidate": x["original"],
            "positive_both_years": x["positive"],
            "passes_strict_200_05": x["strict"],
            "priority_tier": tier,
            "priority_reason": reason,
            "sample_deficit_to_200": max(
                0, 200 - x["n"]
            ),
            "sample_deficit_to_500": max(
                0, 500 - x["n"]
            ),
            "effect_deficit_to_05_pp": round8(
                max(0, 0.5 - x["minimum"])
            ),
            "yearly_effect_asymmetry_ratio": round8(
                asymmetry
            ),
            "requires_new_independent_data": True,
            "independent_oos_proven": False,
            "approved_for_live_trading": False,
        })

    return rows


def build_summary(markets):
    tiers = sorted({
        assign_priority(x)[0] for x in markets
    })
    rows = []

    for tier in tiers:
        selected = [
            x for x in markets
            if assign_priority(x)[0] == tier
        ]
        rows.append({
            "priority_tier": tier,
            "market_count": len(selected),
            "original_candidate_count": sum(
                x["original"] for x in selected
            ),
            "median_minimum_observations": round8(
                statistics.median(
                    x["n"] for x in selected
                )
            ),
            "median_minimum_yearly_effect_pp": round8(
                statistics.median(
                    x["minimum"] for x in selected
                )
            ),
            "median_yearly_gap_pp": round8(
                statistics.median(
                    x["gap"] for x in selected
                )
            ),
            "decision": "HOLD",
        })

    return rows


def validate_output(folder, source_hashes):
    manifest_path = (
        folder / "validation_priority_manifest.json"
    )
    if not manifest_path.is_file():
        return False

    try:
        manifest = read_json(manifest_path)
        result = read_json(
            folder / "validation_priority_result.json"
        )

        if manifest.get("version") != VERSION:
            return False
        if manifest.get("input_sha256") != source_hashes:
            return False
        if result.get("version") != VERSION:
            return False
        if result.get("execution_status") != "PASS":
            return False
        if result.get("overall_decision") != "HOLD":
            return False
        if result.get("approved_for_live_trading") is not False:
            return False
        if result.get("independent_oos_proven") is not False:
            return False
        if result.get("market_count") != 209:
            return False
        if result.get("original_candidate_count") != 22:
            return False

        hashes = manifest.get("output_sha256")
        if not isinstance(hashes, dict):
            return False
        if set(hashes) != set(OUTPUT_FILES):
            return False

        for name in OUTPUT_FILES:
            path = folder / name
            if not path.is_file():
                return False
            if sha256(path) != hashes[name]:
                return False

        rows = read_csv(
            folder / "validation_priority_markets.csv"
        )
        summary = read_csv(
            folder / "validation_priority_summary.csv"
        )

        if len(rows) != 209 or len(summary) != 6:
            return False
        if len({r["market"] for r in rows}) != 209:
            return False

        counts = Counter(
            r["priority_tier"] for r in rows
        )
        if counts != Counter(
            result["priority_counts"]
        ):
            return False

        for row in rows:
            if row["approved_for_live_trading"] != "False":
                return False
            if row["independent_oos_proven"] != "False":
                return False
            if row[
                "requires_new_independent_data"
            ] != "True":
                return False

        for row in summary:
            if row["decision"] != "HOLD":
                return False
            if int(row["market_count"]) != counts[
                row["priority_tier"]
            ]:
                return False

        return True

    except (
        OSError, ValueError, KeyError,
        TypeError, json.JSONDecodeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = (root / SOURCE_REL).resolve()
    output = (root / OUTPUT_REL).resolve()

    if (
        source == output
        or source in output.parents
        or output in source.parents
        or output == root
    ):
        raise ValueError("Unsafe output directory")

    print("[START]", VERSION, flush=True)

    raw, source_hashes = verify_source(source)

    if validate_output(output, source_hashes):
        print(
            "[SKIP] Verified completed result",
            flush=True,
        )
        print("[DECISION] HOLD", flush=True)
        return 0

    markets = parse_markets(raw)
    market_rows = build_rows(markets)
    summary_rows = build_summary(markets)

    counts = dict(sorted(Counter(
        r["priority_tier"] for r in market_rows
    ).items()))

    result = {
        "version": VERSION,
        "input_version": SOURCE_VERSION,
        "execution_status": "PASS",
        "market_count": 209,
        "original_candidate_count": 22,
        "strict_candidate_count": 1,
        "strict_candidate_markets": ["KRW-DKA"],
        "priority_counts": counts,
        "market_rows": len(market_rows),
        "summary_rows": len(summary_rows),
        "method": (
            "Deterministic historical validation "
            "priority classification"
        ),
        "next_required_validation": (
            "Freeze candidate criteria and perform "
            "new, independent, non-overlapping "
            "out-of-sample event evaluation"
        ),
        "limitations": [
            "Priorities use the same historical evidence.",
            "No independent effective sample size was estimated.",
            "No new market observations were collected.",
            "A high priority is not a high success probability.",
            "Sample deficits are arithmetic, not guaranteed future observations.",
            "Yearly differences are not causal effects.",
            "No trading fees, slippage or net PnL analysis.",
            "No live trading or scanner authorization.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    for name, expected in source_hashes.items():
        if sha256(source / name) != expected:
            raise ValueError(
                "Source changed during analysis: " + name
            )

    output.mkdir(parents=True, exist_ok=True)

    write_csv(
        output / OUTPUT_FILES[0],
        MARKET_FIELDS,
        market_rows,
    )
    write_csv(
        output / OUTPUT_FILES[1],
        SUMMARY_FIELDS,
        summary_rows,
    )
    write_json(
        output / OUTPUT_FILES[2],
        result,
    )

    hashes = {
        name: sha256(output / name)
        for name in OUTPUT_FILES
    }

    manifest = {
        "version": VERSION,
        "input_version": SOURCE_VERSION,
        "input_sha256": source_hashes,
        "output_sha256": hashes,
        "market_rows": len(market_rows),
        "summary_rows": len(summary_rows),
    }

    write_json(
        output / "validation_priority_manifest.json",
        manifest,
    )

    if not validate_output(output, source_hashes):
        raise ValueError(
            "Final integrity verification failed"
        )

    print("[PASS]", VERSION, flush=True)
    print("[PASS] Markets: 209", flush=True)
    print("[PASS] Priority counts:", counts, flush=True)
    print("[PASS] SHA256 integrity", flush=True)
    print("[DECISION] HOLD", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("[INTERRUPTED]", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
