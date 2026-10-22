
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-4
Market-level leave-one-out and top-five exclusion diagnostics.

Uses existing Walk-Forward and robustness outputs.
Does not modify source files or place trades.
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

VERSION = "Clean V003-4"
SOURCE_HASH = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)
ROOT_WF = "data/reports/pattern_256_walk_forward"
ROOT_RB = "data/reports/pattern_256_statistical_robustness"
ROOT_OUT = "data/reports/pattern_256_market_sensitivity"

CANDIDATES = (("h1", 24), ("h4", 24))
FOLDS = ("2024_to_2025", "2025_to_2026")
ROLES = ("TRAIN", "TEST")
CLASSES = ("BASELINE", "COMBINED")

DETAIL_FIELDS = [
    "fold", "role", "timeframe", "horizon",
    "scenario", "excluded_market", "excluded_count",
    "baseline_n", "baseline_hits", "baseline_rate_pct",
    "combined_n", "combined_hits", "combined_rate_pct",
    "difference_pp", "remaining_baseline_markets",
    "remaining_combined_markets",
]

SUMMARY_FIELDS = [
    "timeframe", "horizon",
    "test_2025_original_pp", "test_2026_original_pp",
    "test_2025_top5_excluded_pp",
    "test_2026_top5_excluded_pp",
    "test_2025_loo_min_pp",
    "test_2026_loo_min_pp",
    "test_2025_loo_positive_fraction",
    "test_2026_loo_positive_fraction",
    "both_years_original_positive",
    "both_years_top5_positive",
    "both_years_all_loo_positive",
    "decision",
]


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def load_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def atomic_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False,
                  sort_keys=True, allow_nan=False)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_csv(path, fields, rows):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, extrasaction="raise"
        )
        writer.writeheader()
        writer.writerows(rows)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def verify_manifest(folder, name, version, files):
    manifest = load_json(folder / name)
    if manifest.get("version") != version:
        raise ValueError("Manifest version mismatch: " + name)
    if manifest.get("source_sha256") != SOURCE_HASH:
        raise ValueError("Original source hash mismatch")
    recorded = manifest.get("output_sha256")
    if not isinstance(recorded, dict) or set(recorded) != set(files):
        raise ValueError("Manifest file set mismatch")
    hashes = {}
    for filename in files:
        path = folder / filename
        if not path.is_file():
            raise FileNotFoundError(path)
        value = digest(path)
        if value != recorded[filename]:
            raise ValueError("SHA256 mismatch: " + filename)
        hashes[filename] = value
    return hashes


def parse_count(value, label):
    try:
        n = int(value)
    except (TypeError, ValueError) as e:
        raise ValueError("Invalid integer: " + label) from e
    if n < 0:
        raise ValueError("Negative count: " + label)
    return n


def pct(hits, n):
    return round(100.0 * hits / n, 6) if n else 0.0


def calculate(fold, role, tf, horizon, scenario, excluded, records):
    totals = {"BASELINE": [0, 0], "COMBINED": [0, 0]}
    markets = {"BASELINE": set(), "COMBINED": set()}

    for market, classification, n, hits in records:
        if market in excluded:
            continue
        totals[classification][0] += n
        totals[classification][1] += hits
        markets[classification].add(market)

    bn, bh = totals["BASELINE"]
    cn, ch = totals["COMBINED"]

    # A missing comparison group is not treated as 0% performance.
    diff = round(pct(ch, cn) - pct(bh, bn), 6) if bn and cn else None

    return {
        "fold": fold,
        "role": role,
        "timeframe": tf,
        "horizon": horizon,
        "scenario": scenario,
        "excluded_market": (
            next(iter(excluded)) if len(excluded) == 1 else ""
        ),
        "excluded_count": len(excluded),
        "baseline_n": bn,
        "baseline_hits": bh,
        "baseline_rate_pct": pct(bh, bn) if bn else None,
        "combined_n": cn,
        "combined_hits": ch,
        "combined_rate_pct": pct(ch, cn) if cn else None,
        "difference_pp": diff,
        "remaining_baseline_markets": len(markets["BASELINE"]),
        "remaining_combined_markets": len(markets["COMBINED"]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    wf = (root / ROOT_WF).resolve()
    rb = (root / ROOT_RB).resolve()
    out = (root / ROOT_OUT).resolve()

    if wf == rb or out in (wf, rb):
        raise ValueError("Input and output paths must differ")
    if wf in out.parents or rb in out.parents:
        raise ValueError("Unsafe output directory")

    print("[START]", VERSION, flush=True)

    wf_hashes = verify_manifest(
        wf, "walk_forward_manifest.json", "Clean V003-2",
        (
            "walk_forward_comparison.csv",
            "walk_forward_by_market.csv",
            "walk_forward_result.json",
        ),
    )
    rb_hashes = verify_manifest(
        rb, "robustness_manifest.json", "Clean V003-3",
        (
            "robustness_comparison.csv",
            "robustness_market_concentration.csv",
            "robustness_result.json",
        ),
    )

    wf_result = load_json(wf / "walk_forward_result.json")
    rb_result = load_json(rb / "robustness_result.json")

    if wf_result.get("execution_status") != "PASS":
        raise ValueError("Walk-Forward input not PASS")
    if rb_result.get("execution_status") != "PASS":
        raise ValueError("Robustness input not PASS")
    if wf_result.get("source_rows") != 7949826:
        raise ValueError("Unexpected original row count")
    if rb_result.get("source_sha256") != SOURCE_HASH:
        raise ValueError("Robustness source mismatch")
    if wf_result.get("approved_for_live_trading") is not False:
        raise ValueError("Unsafe Walk-Forward approval flag")
    if rb_result.get("approved_for_live_trading") is not False:
        raise ValueError("Unsafe robustness approval flag")

    approved_candidates = {
        (item["timeframe"], int(item["horizon"]))
        for item in rb_result.get("summary", [])
        if item.get("research_candidate") is True
    }
    if approved_candidates != set(CANDIDATES):
        raise ValueError(
            "Candidate set changed; manual review required"
        )

    comparison_rows = load_csv(wf / "walk_forward_comparison.csv")
    market_rows = load_csv(wf / "walk_forward_by_market.csv")

    comparisons = {}
    for row in comparison_rows:
        key = (
            row["fold"], row["role"],
            row["timeframe"], int(row["horizon"]),
        )
        if key in comparisons:
            raise ValueError("Duplicate comparison key")
        comparisons[key] = row

    grouped = defaultdict(list)
    seen = set()
    for row in market_rows:
        key = (
            row["fold"], row["role"],
            row["timeframe"], int(row["horizon"]),
        )
        cls = row["classification"]
        market = row["market"]
        if cls not in CLASSES or not market.startswith("KRW-"):
            raise ValueError("Unexpected classification or market")
        unique = key + (cls, market)
        if unique in seen:
            raise ValueError("Duplicate market row")
        seen.add(unique)
        n = parse_count(row["completed"], "completed")
        hits = parse_count(row["hits"], "hits")
        if not 0 <= hits <= n or n == 0:
            raise ValueError("Invalid market hit counts")
        grouped[key].append((market, cls, n, hits))

    details = []
    summaries = []

    for tf, horizon in CANDIDATES:
        test_results = {}
        for fold in FOLDS:
            for role in ROLES:
                key = (fold, role, tf, horizon)
                records = grouped.get(key, [])
                if key not in comparisons:
                    raise ValueError("Missing comparison: " + str(key))

                original = calculate(
                    fold, role, tf, horizon, "ORIGINAL", set(), records
                )
                ref = comparisons[key]
                if (
                    original["baseline_n"] != int(ref["baseline_completed"])
                    or original["baseline_hits"] != int(ref["baseline_hits"])
                    or original["combined_n"] != int(ref["combined_completed"])
                    or original["combined_hits"] != int(ref["combined_hits"])
                ):
                    raise ValueError("Market totals do not match comparison")
                details.append(original)

                # Rank markets by total number of observations
                # across both classes, without using future hit results.
                market_counts = defaultdict(int)
                for market, cls, n, hits in records:
                    market_counts[market] += n

                ranked = sorted(
                    market_counts,
                    key=lambda m: (-market_counts[m], m),
                )
                top5 = set(ranked[:5])
                top5_row = calculate(
                    fold, role, tf, horizon,
                    "EXCLUDE_TOP5_VOLUME", top5, records
                )
                details.append(top5_row)

                loo_rows = []
                for market in sorted(market_counts):
                    item = calculate(
                        fold, role, tf, horizon,
                        "LEAVE_ONE_MARKET_OUT",
                        {market}, records
                    )
                    details.append(item)
                    loo_rows.append(item)

                if role == "TEST":
                    valid_loo = [
                        r for r in loo_rows
                        if r["difference_pp"] is not None
                    ]
                    test_results[fold] = {
                        "original": original,
                        "top5": top5_row,
                        "loo": loo_rows,
                        "valid_loo": valid_loo,
                    }

        first = test_results[FOLDS[0]]
        second = test_results[FOLDS[1]]

        def positive(row):
            return (
                row["difference_pp"] is not None
                and row["difference_pp"] > 0
            )

        def min_loo(group):
            values = [r["difference_pp"] for r in group["valid_loo"]]
            return min(values) if values else None

        def loo_fraction(group):
            rows = group["loo"]
            if not rows:
                return None
            return round(
                sum(positive(r) for r in rows) / len(rows), 6
            )

        original_positive = (
            positive(first["original"])
            and positive(second["original"])
        )
        top5_positive = (
            positive(first["top5"])
            and positive(second["top5"])
        )
        all_loo_positive = all(
            group["loo"]
            and len(group["valid_loo"]) == len(group["loo"])
            and all(positive(r) for r in group["loo"])
            for group in (first, second)
        )

        summaries.append({
            "timeframe": tf,
            "horizon": horizon,
            "test_2025_original_pp": first["original"]["difference_pp"],
            "test_2026_original_pp": second["original"]["difference_pp"],
            "test_2025_top5_excluded_pp": first["top5"]["difference_pp"],
            "test_2026_top5_excluded_pp": second["top5"]["difference_pp"],
            "test_2025_loo_min_pp": min_loo(first),
            "test_2026_loo_min_pp": min_loo(second),
            "test_2025_loo_positive_fraction": loo_fraction(first),
            "test_2026_loo_positive_fraction": loo_fraction(second),
            "both_years_original_positive": original_positive,
            "both_years_top5_positive": top5_positive,
            "both_years_all_loo_positive": all_loo_positive,
            "decision": "HOLD",
        })

    # Recheck inputs before publishing output.
    for folder, hashes in ((wf, wf_hashes), (rb, rb_hashes)):
        for filename, expected in hashes.items():
            if digest(folder / filename) != expected:
                raise ValueError("Input changed during analysis")

    out.mkdir(parents=True, exist_ok=True)
    detail_path = out / "market_sensitivity_detail.csv"
    summary_path = out / "market_sensitivity_summary.csv"
    result_path = out / "market_sensitivity_result.json"
    manifest_path = out / "market_sensitivity_manifest.json"

    atomic_csv(detail_path, DETAIL_FIELDS, details)
    atomic_csv(summary_path, SUMMARY_FIELDS, summaries)

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_sha256": SOURCE_HASH,
        "source_rows": 7949826,
        "candidate_count": len(CANDIDATES),
        "detail_rows": len(details),
        "summary_rows": len(summaries),
        "summary": summaries,
        "method": (
            "Leave-one-market-out and exclusion of the five "
            "highest-observation markets separately in each fold/role"
        ),
        "limitations": [
            "Baseline and combined cohorts are not matched.",
            "Leave-one-out scenarios are correlated.",
            "Top-five markets are selected by observation count.",
            "Market exclusion does not eliminate time dependence.",
            "2025 and 2026 are not independently randomized trials.",
            "No causal inference or trading profitability proof.",
            "Candidate selection used prior observed test results.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }
    atomic_json(result_path, result)

    hashes = {
        path.name: digest(path)
        for path in (detail_path, summary_path, result_path)
    }
    manifest = {
        "version": VERSION,
        "source_sha256": SOURCE_HASH,
        "walk_forward_input_sha256": wf_hashes,
        "robustness_input_sha256": rb_hashes,
        "output_sha256": hashes,
        "detail_rows": len(details),
        "summary_rows": len(summaries),
    }
    atomic_json(manifest_path, manifest)

    for filename, expected in hashes.items():
        if digest(out / filename) != expected:
            raise ValueError("Output hash mismatch: " + filename)

    print("[PASS] Candidate groups:", len(summaries), flush=True)
    print("[PASS] Sensitivity rows:", len(details), flush=True)
    print("[PASS] SHA256 integrity", flush=True)
    print("[DECISION] HOLD", flush=True)
    print("[OUTPUT]", out, flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("[INTERRUPTED]", file=sys.stderr, flush=True)
        sys.exit(130)
    except Exception as exc:
        print("[FAIL]", str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
