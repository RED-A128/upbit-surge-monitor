
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Regime Statistical Stability Audit
Clean V003

Input:
  data/reports/pattern_256_market_regime/
    result.json
    manifest.json

Output:
  data/reports/pattern_256_regime_statistical_stability/
    result.json
    report.md
    manifest.json

Principles:
- Read-only input
- SHA256 input verification
- Recompute all seed-level aggregates
- No invented independent observations
- No formal significance claims from aggregated data
- No training or trading approval
- Research decision always HOLD
"""

import argparse
import hashlib
import json
import math
import statistics
import sys

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V003"

TIMEFRAMES = ("h4", "d1")
PERIODS = ("train", "test")
REGIMES = ("UP", "SIDE", "DOWN")
VOLS = ("LOW", "HIGH")

STAT_FIELDS = (
    "signal_events",
    "signal_hits",
    "baseline_events",
    "baseline_hits",
    "unmatched",
    "strata_without_baseline",
)

TOLERANCE = 0.000002


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        while True:
            block = f.read(1024 * 1024)

            if not block:
                break

            h.update(block)

    return h.hexdigest()


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as f:
        return json.load(f)


def save_json(path, obj):
    with path.open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            obj,
            f,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        f.write("\n")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def key_for(tf, period, regime, vol):
    return f"{tf}|{period}|{regime}|{vol}"


def all_keys():
    return [
        key_for(tf, period, regime, vol)
        for tf in TIMEFRAMES
        for period in PERIODS
        for regime in REGIMES
        for vol in VOLS
    ]


def rate(hits, events):
    if events == 0:
        return None

    return 100.0 * hits / events


def close_enough(a, b):
    if a is None or b is None:
        return a is None and b is None

    return (
        math.isfinite(float(a))
        and math.isfinite(float(b))
        and abs(float(a) - float(b))
        <= TOLERANCE
    )


def percentile(values, fraction):
    values = sorted(values)

    if not values:
        return None

    position = (len(values) - 1) * fraction

    lower = math.floor(position)
    upper = math.ceil(position)

    weight = position - lower

    return (
        values[lower] * (1 - weight)
        + values[upper] * weight
    )


def wilson_interval(hits, events):
    """
    Wilson 95% binomial interval.

    Descriptive only:
    Events may be correlated across markets/time.
    This interval is NOT treated as a formal
    confidence guarantee.
    """

    if events == 0:
        return None

    z = 1.959963984540054
    p = hits / events
    z2 = z * z

    denominator = 1 + z2 / events

    center = (
        p + z2 / (2 * events)
    ) / denominator

    margin = (
        z
        * math.sqrt(
            p * (1 - p) / events
            + z2 / (4 * events * events)
        )
        / denominator
    )

    return [
        round(100 * (center - margin), 6),
        round(100 * (center + margin), 6),
    ]


def verify_inputs(result_path, manifest_path):
    require(
        result_path.is_file(),
        f"Missing input: {result_path}",
    )

    require(
        manifest_path.is_file(),
        f"Missing manifest: {manifest_path}",
    )

    manifest = load_json(manifest_path)

    expected = manifest.get(
        "sha256", {}
    ).get("result.json")

    require(
        isinstance(expected, str)
        and len(expected) == 64,
        "Missing input SHA256",
    )

    actual = sha256(result_path)

    require(
        actual.lower() == expected.lower(),
        "INPUT_SHA256_MISMATCH",
    )

    result = load_json(result_path)

    require(
        result.get("version") == "Clean V002",
        "Expected Clean V002 input",
    )

    require(
        result.get("execution") == "PASS",
        "Input execution is not PASS",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected research decision",
    )

    require(
        result.get("train_approved") is False,
        "Unexpected training approval",
    )

    require(
        result.get("live_trading_approved") is False,
        "Unexpected trading approval",
    )

    require(
        result.get("seeds") == 30,
        "Expected 30 baseline seeds",
    )

    require(
        result.get("market_counts") == {
            "h4": 290,
            "d1": 290,
        },
        "Unexpected market counts",
    )

    return result, actual


def validate_run(run, expected_keys):
    require(
        isinstance(run, dict),
        "Run is not an object",
    )

    require(
        set(run) == expected_keys,
        "Run keys mismatch",
    )

    for key in expected_keys:
        item = run[key]

        require(
            isinstance(item, dict),
            f"Invalid stratum: {key}",
        )

        for field in STAT_FIELDS:
            value = item.get(field)

            require(
                type(value) is int
                and value >= 0,
                f"Invalid {field}: {key}",
            )

        require(
            item["signal_hits"]
            <= item["signal_events"],
            f"Signal hits exceed events: {key}",
        )

        require(
            item["baseline_hits"]
            <= item["baseline_events"],
            f"Baseline hits exceed events: {key}",
        )

        require(
            item["baseline_events"]
            + item["unmatched"]
            == item["signal_events"],
            f"Baseline accounting mismatch: {key}",
        )


def get_summary_item(result, key):
    tf, period, regime, vol = key.split("|")

    return (
        result["summary"][tf]
        [period][regime][vol]
    )


def recompute_stratum(result, key):
    runs = result["runs"]

    values = [run[key] for run in runs]

    first = values[0]

    events = first["signal_events"]
    hits = first["signal_hits"]

    require(
        all(
            x["signal_events"] == events
            and x["signal_hits"] == hits
            for x in values
        ),
        f"Signal totals differ by seed: {key}",
    )

    signal_rate = rate(hits, events)

    baseline_rates = [
        rate(
            x["baseline_hits"],
            x["baseline_events"],
        )
        for x in values
    ]

    valid_rates = [
        x for x in baseline_rates
        if x is not None
    ]

    baseline_mean = (
        statistics.mean(valid_rates)
        if valid_rates else None
    )

    lift = (
        signal_rate - baseline_mean
        if (
            signal_rate is not None
            and baseline_mean is not None
        )
        else None
    )

    positive_seeds = (
        sum(
            signal_rate > x
            for x in valid_rates
        )
        if signal_rate is not None
        else 0
    )

    original = get_summary_item(result, key)

    checks = {
        "signal_events": events,
        "signal_hits": hits,
        "positive_seed_count": positive_seeds,
        "valid_baseline_seeds": len(valid_rates),
        "baseline_events_min": min(
            x["baseline_events"]
            for x in values
        ),
        "baseline_events_max": max(
            x["baseline_events"]
            for x in values
        ),
        "unmatched_min": min(
            x["unmatched"]
            for x in values
        ),
        "unmatched_max": max(
            x["unmatched"]
            for x in values
        ),
    }

    for field, calculated in checks.items():
        require(
            original.get(field) == calculated,
            f"SUMMARY_MISMATCH: {key}/{field}",
        )

    floating_checks = {
        "signal_rate_pct": signal_rate,
        "baseline_mean_pct": baseline_mean,
        "mean_lift_pp": lift,
    }

    for field, calculated in floating_checks.items():
        require(
            close_enough(
                original.get(field),
                calculated,
            ),
            f"SUMMARY_MISMATCH: {key}/{field}",
        )

    unmatched_max = max(
        x["unmatched"]
        for x in values
    )

    complete_matching = (
        unmatched_max == 0
        and len(valid_rates) == len(runs)
    )

    baseline_p025 = percentile(
        valid_rates, 0.025
    )

    baseline_p975 = percentile(
        valid_rates, 0.975
    )

    lift_values = (
        [
            signal_rate - x
            for x in valid_rates
        ]
        if signal_rate is not None
        else []
    )

    flags = []

    if events < 100:
        flags.append("LOW_SAMPLE")

    if not complete_matching:
        flags.append("INCOMPLETE_MATCHING")

    if len(valid_rates) < len(runs):
        flags.append("MISSING_BASELINE_SEEDS")

    if not lift_values:
        flags.append("NO_COMPARISON")

    if lift_values and min(lift_values) <= 0:
        flags.append("NOT_POSITIVE_ALL_SEEDS")

    if lift is not None and lift <= 0:
        flags.append("NON_POSITIVE_MEAN_LIFT")

    return {
        "key": key,
        "signal_events": events,
        "signal_hits": hits,
        "signal_rate_pct": round(
            signal_rate, 6
        ) if signal_rate is not None else None,
        "signal_wilson95_descriptive_pct":
            wilson_interval(hits, events),
        "baseline_mean_pct": round(
            baseline_mean, 6
        ) if baseline_mean is not None else None,
        "baseline_seed_p025_pct": round(
            baseline_p025, 6
        ) if baseline_p025 is not None else None,
        "baseline_seed_p975_pct": round(
            baseline_p975, 6
        ) if baseline_p975 is not None else None,
        "mean_lift_pp": round(
            lift, 6
        ) if lift is not None else None,
        "lift_seed_min_pp": round(
            min(lift_values), 6
        ) if lift_values else None,
        "lift_seed_max_pp": round(
            max(lift_values), 6
        ) if lift_values else None,
        "positive_seed_count": positive_seeds,
        "baseline_seed_count": len(valid_rates),
        "unmatched_min": min(
            x["unmatched"] for x in values
        ),
        "unmatched_max": unmatched_max,
        "complete_matching": complete_matching,
        "flags": flags,
    }


def compare_periods(strata):
    comparisons = []

    for tf in TIMEFRAMES:
        for regime in REGIMES:
            for vol in VOLS:
                train_key = key_for(
                    tf, "train", regime, vol
                )

                test_key = key_for(
                    tf, "test", regime, vol
                )

                train = strata[train_key]
                test = strata[test_key]

                train_lift = train["mean_lift_pp"]
                test_lift = test["mean_lift_pp"]

                same_positive = (
                    train_lift is not None
                    and test_lift is not None
                    and train_lift > 0
                    and test_lift > 0
                )

                stable_all_seeds = (
                    same_positive
                    and test["positive_seed_count"] == 30
                    and test["complete_matching"]
                    and test["signal_events"] >= 100
                )

                comparisons.append({
                    "timeframe": tf,
                    "regime": regime,
                    "volatility": vol,
                    "train_events":
                        train["signal_events"],
                    "test_events":
                        test["signal_events"],
                    "train_lift_pp": train_lift,
                    "test_lift_pp": test_lift,
                    "test_positive_seeds":
                        test["positive_seed_count"],
                    "same_positive_direction":
                        same_positive,
                    "descriptive_stability_candidate":
                        stable_all_seeds,
                    "formal_significance_proven":
                        False,
                    "independent_oos_proven":
                        False,
                })

    return comparisons


def create_report(output):
    lines = [
        "# Pattern 256 Statistical Stability Audit",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "## Validation",
        "",
        "- Input SHA256: verified",
        "- Seed aggregates: recomputed",
        "- Original summaries: verified",
        "- Formal statistical significance: NOT PROVEN",
        "",
        "## Train / Test comparison",
        "",
        "| TF | Regime | Vol | Train events | "
        "Test events | Train lift pp | "
        "Test lift pp | Positive seeds | Candidate |",
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]

    def fmt(value):
        if value is None:
            return "N/A"

        return f"{value:.4f}"

    for item in output["period_comparisons"]:
        lines.append(
            f"| {item['timeframe']} "
            f"| {item['regime']} "
            f"| {item['volatility']} "
            f"| {item['train_events']} "
            f"| {item['test_events']} "
            f"| {fmt(item['train_lift_pp'])} "
            f"| {fmt(item['test_lift_pp'])} "
            f"| {item['test_positive_seeds']}/30 "
            f"| {item['descriptive_stability_candidate']} |"
        )

    lines.extend([
        "",
        "## Test strata diagnostics",
        "",
        "| Stratum | Events | Lift pp | "
        "Seed lift min | Seed lift max | Flags |",
        "|---|---:|---:|---:|---:|---|",
    ])

    for key, item in output["strata"].items():
        if "|test|" not in key:
            continue

        flags = ", ".join(item["flags"]) or "NONE"

        lines.append(
            f"| {key} "
            f"| {item['signal_events']} "
            f"| {fmt(item['mean_lift_pp'])} "
            f"| {fmt(item['lift_seed_min_pp'])} "
            f"| {fmt(item['lift_seed_max_pp'])} "
            f"| {flags} |"
        )

    lines.extend([
        "",
        "## Interpretation limits",
        "",
        "- Baseline seeds are repeated random samples, "
        "not 30 independent market experiments.",
        "- Wilson intervals are descriptive only; "
        "market and time dependence is not modeled.",
        "- No valid formal p-values or multiple-testing "
        "adjustment can be established from these "
        "aggregated records alone.",
        "- Volatility buckets in Clean V002 used "
        "retrospective period medians.",
        "- Positive train/test lifts do not prove "
        "independent out-of-sample performance.",
        "- No fees, slippage, drawdown or realized "
        "profitability assessment.",
        "- Research decision remains HOLD.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source = (
        root
        / "data/reports/pattern_256_market_regime"
    )

    result_path = source / "result.json"
    manifest_path = source / "manifest.json"

    result, input_hash = verify_inputs(
        result_path,
        manifest_path,
    )

    runs = result.get("runs")

    require(
        isinstance(runs, list)
        and len(runs) == 30,
        "Expected exactly 30 runs",
    )

    expected_keys = set(all_keys())

    for number, run in enumerate(runs, 1):
        validate_run(
            run,
            expected_keys,
        )

        print(
            f"[CHECK] Seed {number}/30 verified",
            flush=True,
        )

    strata = {}

    for key in all_keys():
        strata[key] = recompute_stratum(
            result,
            key,
        )

    total_events = sum(
        strata[key]["signal_events"]
        for key in all_keys()
    )

    require(
        total_events > 0,
        "ZERO_SIGNAL_AUDIT",
    )

    require(
        total_events
        == result.get("total_signal_events"),
        "TOTAL_SIGNAL_MISMATCH",
    )

    for tf in TIMEFRAMES:
        tf_events = sum(
            strata[key]["signal_events"]
            for key in all_keys()
            if key.startswith(tf + "|")
        )

        expected = result["diagnostics"][tf][
            "deduplicated_signals"
        ]

        require(
            tf_events == expected,
            f"DIAGNOSTIC_MISMATCH: {tf}",
        )

    comparisons = compare_periods(strata)

    candidates = [
        item for item in comparisons
        if item[
            "descriptive_stability_candidate"
        ]
    ]

    output = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "source_version": "Clean V002",
        "source_result_sha256": input_hash,
        "verified_seed_count": len(runs),
        "verified_strata_count": len(strata),
        "verified_signal_events": total_events,
        "period_comparisons": comparisons,
        "descriptive_candidate_count":
            len(candidates),
        "descriptive_candidates": candidates,
        "strata": strata,
        "formal_significance_proven": False,
        "multiple_testing_adjusted": False,
        "independent_oos_proven": False,
        "point_in_time_regime_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    folder = (
        root
        / "data/reports/"
        "pattern_256_regime_statistical_stability"
    )

    folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    rp = folder / "result.json"
    md = folder / "report.md"
    mf = folder / "manifest.json"

    save_json(rp, output)

    md.write_text(
        create_report(output),
        encoding="utf-8",
    )

    save_json(mf, {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_result_sha256": input_hash,
        "sha256": {
            "result.json": sha256(rp),
            "report.md": sha256(md),
        },
    })

    print(
        "[PASS] Statistical stability audit",
        flush=True,
    )

    print(
        f"Verified signals: {total_events}",
        flush=True,
    )

    print(
        f"Descriptive candidates: {len(candidates)}",
        flush=True,
    )

    print(
        "Formal significance: NOT PROVEN",
        flush=True,
    )

    print(
        "Research decision: HOLD",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
