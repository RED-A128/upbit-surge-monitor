
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Candidate Sensitivity Audit
Clean V004

Input:
  data/reports/pattern_256_regime_statistical_stability/
    result.json
    manifest.json

Output:
  data/reports/pattern_256_candidate_sensitivity/
    result.json
    report.md
    manifest.json

Purpose:
  Descriptive threshold sensitivity only.

Not proven:
  Independent OOS
  Statistical significance
  Multiple-testing adjustment
  Point-in-time regime classification
  Realized profitability

No trading or model training.
No source files are modified.
"""

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V004"
SOURCE_VERSION = "Clean V003"

SOURCE_RELATIVE = Path(
    "data/reports/"
    "pattern_256_regime_statistical_stability"
)

OUTPUT_RELATIVE = Path(
    "data/reports/"
    "pattern_256_candidate_sensitivity"
)

TIMEFRAMES = ("h4", "d1")
REGIMES = ("UP", "SIDE", "DOWN")
VOLATILITIES = ("LOW", "HIGH")
PERIODS = ("train", "test")

# 사전에 고정한 탐색적 민감도 기준.
# 이 기준을 통과해도 통계적 유의성이 입증되지는 않는다.
SCENARIOS = (
    {
        "name": "lenient",
        "min_train_events": 100,
        "min_test_events": 100,
        "min_train_lift_pp": 0.0,
        "min_test_lift_pp": 0.0,
        "min_positive_seeds": 24,
        "require_complete_matching": False,
    },
    {
        "name": "standard",
        "min_train_events": 200,
        "min_test_events": 200,
        "min_train_lift_pp": 1.0,
        "min_test_lift_pp": 1.0,
        "min_positive_seeds": 30,
        "require_complete_matching": True,
    },
    {
        "name": "strict",
        "min_train_events": 250,
        "min_test_events": 250,
        "min_train_lift_pp": 3.0,
        "min_test_lift_pp": 3.0,
        "min_positive_seeds": 30,
        "require_complete_matching": True,
    },
    {
        "name": "very_strict",
        "min_train_events": 300,
        "min_test_events": 300,
        "min_train_lift_pp": 5.0,
        "min_test_lift_pp": 5.0,
        "min_positive_seeds": 30,
        "require_complete_matching": True,
    },
)


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def sha256_file(path):
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
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def write_json(path, value):
    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            value,
            handle,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        handle.write("\n")


def write_text(path, value):
    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        handle.write(value)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def finite_number(value, label):
    require(
        type(value) in (int, float),
        "Invalid numeric field: " + label,
    )

    number = float(value)

    require(
        math.isfinite(number),
        "Non-finite numeric field: " + label,
    )

    return number


def exact_integer(value, label):
    require(
        type(value) is int,
        "Invalid integer field: " + label,
    )
    return value


def validate_source(source, manifest, result_path):
    require(
        source.get("version") == SOURCE_VERSION,
        "Unexpected source version",
    )
    require(
        source.get("execution") == "PASS",
        "Source execution not PASS",
    )
    require(
        source.get("research_decision") == "HOLD",
        "Source research decision not HOLD",
    )

    require(
        exact_integer(
            source.get("verified_seed_count"),
            "verified_seed_count",
        ) == 30,
        "Unexpected seed count",
    )

    require(
        exact_integer(
            source.get("verified_strata_count"),
            "verified_strata_count",
        ) == 24,
        "Unexpected strata count",
    )

    require(
        exact_integer(
            source.get("verified_signal_events"),
            "verified_signal_events",
        ) == 41311,
        "Unexpected signal count",
    )

    require(
        manifest.get("version") == SOURCE_VERSION,
        "Manifest version mismatch",
    )

    expected_hash = manifest.get(
        "sha256", {}
    ).get("result.json")

    require(
        isinstance(expected_hash, str)
        and len(expected_hash) == 64,
        "Missing source result hash",
    )

    actual_hash = sha256_file(result_path)

    require(
        actual_hash == expected_hash.lower(),
        "Source result SHA256 mismatch",
    )

    require(
        source.get("source_result_sha256")
        == manifest.get("source_result_sha256"),
        "Upstream source hash mismatch",
    )

    for field in (
        "formal_significance_proven",
        "multiple_testing_adjusted",
        "independent_oos_proven",
        "point_in_time_regime_proven",
        "profitability_proven",
        "train_approved",
        "live_trading_approved",
    ):
        require(
            source.get(field) is False,
            "Unexpected research approval: " + field,
        )

    comparisons = source.get("period_comparisons")
    strata = source.get("strata")

    require(
        isinstance(comparisons, list)
        and len(comparisons) == 12,
        "Expected 12 period comparisons",
    )
    require(
        isinstance(strata, dict)
        and len(strata) == 24,
        "Expected 24 strata",
    )

    expected_keys = {
        f"{tf}|{period}|{regime}|{vol}"
        for tf in TIMEFRAMES
        for period in PERIODS
        for regime in REGIMES
        for vol in VOLATILITIES
    }

    require(
        set(strata) == expected_keys,
        "Strata keys mismatch",
    )

    total_events = 0

    for key in sorted(expected_keys):
        item = strata[key]

        require(
            item.get("key") == key,
            "Stratum key mismatch: " + key,
        )

        events = exact_integer(
            item.get("signal_events"),
            key + ".signal_events",
        )
        hits = exact_integer(
            item.get("signal_hits"),
            key + ".signal_hits",
        )

        require(
            events >= 0 and 0 <= hits <= events,
            "Invalid event counts: " + key,
        )

        total_events += events

        for field in (
            "signal_rate_pct",
            "baseline_mean_pct",
            "mean_lift_pp",
            "lift_seed_min_pp",
            "lift_seed_max_pp",
        ):
            finite_number(
                item.get(field),
                key + "." + field,
            )

        positive = exact_integer(
            item.get("positive_seed_count"),
            key + ".positive_seed_count",
        )

        require(
            0 <= positive <= 30,
            "Invalid positive seeds: " + key,
        )

        require(
            item.get("baseline_seed_count") == 30,
            "Invalid baseline seed count: " + key,
        )

        require(
            type(item.get("complete_matching")) is bool,
            "Invalid matching status: " + key,
        )

        require(
            abs(
                item["signal_rate_pct"]
                - item["baseline_mean_pct"]
                - item["mean_lift_pp"]
            ) < 0.00001,
            "Lift arithmetic mismatch: " + key,
        )

    require(
        total_events == 41311,
        "Strata event total mismatch",
    )

    seen = set()

    for item in comparisons:
        tf = item.get("timeframe")
        regime = item.get("regime")
        vol = item.get("volatility")

        key = f"{tf}|{regime}|{vol}"

        require(
            tf in TIMEFRAMES
            and regime in REGIMES
            and vol in VOLATILITIES,
            "Invalid comparison group",
        )
        require(
            key not in seen,
            "Duplicate comparison: " + key,
        )
        seen.add(key)

        train = strata[
            f"{tf}|train|{regime}|{vol}"
        ]
        test = strata[
            f"{tf}|test|{regime}|{vol}"
        ]

        require(
            item.get("train_events")
            == train["signal_events"],
            "Train count mismatch: " + key,
        )
        require(
            item.get("test_events")
            == test["signal_events"],
            "Test count mismatch: " + key,
        )
        require(
            abs(
                finite_number(
                    item.get("train_lift_pp"),
                    key + ".train_lift_pp",
                )
                - train["mean_lift_pp"]
            ) < 0.00001,
            "Train lift mismatch: " + key,
        )
        require(
            abs(
                finite_number(
                    item.get("test_lift_pp"),
                    key + ".test_lift_pp",
                )
                - test["mean_lift_pp"]
            ) < 0.00001,
            "Test lift mismatch: " + key,
        )
        require(
            item.get("test_positive_seeds")
            == test["positive_seed_count"],
            "Positive seed mismatch: " + key,
        )

    require(
        len(seen) == 12,
        "Incomplete comparison coverage",
    )

    return actual_hash


def evaluate(comparison, strata, scenario):
    tf = comparison["timeframe"]
    regime = comparison["regime"]
    vol = comparison["volatility"]

    train = strata[
        f"{tf}|train|{regime}|{vol}"
    ]
    test = strata[
        f"{tf}|test|{regime}|{vol}"
    ]

    checks = {
        "train_sample": (
            train["signal_events"]
            >= scenario["min_train_events"]
        ),
        "test_sample": (
            test["signal_events"]
            >= scenario["min_test_events"]
        ),
        "train_lift": (
            train["mean_lift_pp"]
            >= scenario["min_train_lift_pp"]
        ),
        "test_lift": (
            test["mean_lift_pp"]
            >= scenario["min_test_lift_pp"]
        ),
        "positive_seeds": (
            test["positive_seed_count"]
            >= scenario["min_positive_seeds"]
        ),
        "matching": (
            not scenario["require_complete_matching"]
            or (
                train["complete_matching"]
                and test["complete_matching"]
            )
        ),
    }

    passed = all(checks.values())

    return {
        "timeframe": tf,
        "regime": regime,
        "volatility": vol,
        "train_events": train["signal_events"],
        "test_events": test["signal_events"],
        "train_lift_pp": train["mean_lift_pp"],
        "test_lift_pp": test["mean_lift_pp"],
        "test_positive_seeds": test[
            "positive_seed_count"
        ],
        "complete_matching": (
            train["complete_matching"]
            and test["complete_matching"]
        ),
        "checks": checks,
        "passed": passed,
        "failed_checks": [
            name
            for name, success in checks.items()
            if not success
        ],
    }


def build_report(result):
    lines = [
        "# Pattern 256 Candidate Sensitivity Audit",
        "",
        f"Version: {VERSION}",
        "",
        "Execution: PASS",
        "",
        "Research decision: HOLD",
        "",
        "## Threshold sensitivity",
        "",
        "| Scenario | Passing groups |",
        "|---|---:|",
    ]

    for scenario in result["scenarios"]:
        lines.append(
            f"| {scenario['name']} "
            f"| {scenario['passing_group_count']} |"
        )

    lines.extend([
        "",
        "## Original candidate",
        "",
        "D1 / DOWN / LOW",
        "",
        "| Scenario | Passed | Failed checks |",
        "|---|---|---|",
    ])

    for scenario in result["scenarios"]:
        candidate = next(
            item
            for item in scenario["groups"]
            if item["timeframe"] == "d1"
            and item["regime"] == "DOWN"
            and item["volatility"] == "LOW"
        )

        failed = ", ".join(
            candidate["failed_checks"]
        ) or "none"

        lines.append(
            f"| {scenario['name']} "
            f"| {candidate['passed']} "
            f"| {failed} |"
        )

    lines.extend([
        "",
        "## Important limitations",
        "",
        "- This audit reuses historical summary data.",
        "- Thresholds are descriptive and heuristic.",
        "- Passing thresholds is not statistical proof.",
        "- The candidate was selected after exploration.",
        "- Seeds are not independent experiments.",
        "- No new independent OOS data were tested.",
        "- Market-regime labels are not proven point-in-time.",
        "- Multiple-testing adjustment is not proven.",
        "- No realized trading profit is proven.",
        "- No training or live trading approval.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
        help="Persistent project root",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    source_folder = root / SOURCE_RELATIVE
    output_folder = root / OUTPUT_RELATIVE

    source_path = source_folder / "result.json"
    manifest_path = source_folder / "manifest.json"

    require(
        source_path.is_file(),
        "Source result.json missing",
    )
    require(
        manifest_path.is_file(),
        "Source manifest.json missing",
    )

    source = read_json(source_path)
    manifest = read_json(manifest_path)

    source_hash = validate_source(
        source,
        manifest,
        source_path,
    )

    scenarios = []

    for config in SCENARIOS:
        groups = [
            evaluate(
                item,
                source["strata"],
                config,
            )
            for item in source["period_comparisons"]
        ]

        groups.sort(
            key=lambda item: (
                item["timeframe"],
                item["regime"],
                item["volatility"],
            )
        )

        scenarios.append({
            "name": config["name"],
            "thresholds": config,
            "passing_group_count": sum(
                item["passed"]
                for item in groups
            ),
            "groups": groups,
        })

    candidate_results = {}

    for scenario in scenarios:
        candidate = next(
            item
            for item in scenario["groups"]
            if item["timeframe"] == "d1"
            and item["regime"] == "DOWN"
            and item["volatility"] == "LOW"
        )

        candidate_results[
            scenario["name"]
        ] = candidate["passed"]

    result = {
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": "PASS",
        "source_version": SOURCE_VERSION,
        "source_result_sha256": source_hash,
        "verified_strata_count": 24,
        "verified_signal_events": 41311,
        "scenario_count": len(SCENARIOS),
        "scenarios": scenarios,
        "original_candidate": "d1|DOWN|LOW",
        "candidate_scenario_pass": candidate_results,
        "analysis_type": (
            "DESCRIPTIVE_THRESHOLD_SENSITIVITY"
        ),
        "formal_significance_proven": False,
        "multiple_testing_adjusted": False,
        "independent_oos_proven": False,
        "point_in_time_regime_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    output_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_output = output_folder / "result.json"
    report_output = output_folder / "report.md"
    manifest_output = output_folder / "manifest.json"

    write_json(result_output, result)
    write_text(
        report_output,
        build_report(result),
    )

    output_manifest = {
        "version": VERSION,
        "created_utc": utc_now(),
        "source_result_sha256": source_hash,
        "sha256": {
            "result.json": sha256_file(result_output),
            "report.md": sha256_file(report_output),
        },
    }

    write_json(
        manifest_output,
        output_manifest,
    )

    print("[PASS] Candidate sensitivity audit")
    print("Version:", VERSION)
    print("Source SHA256:", source_hash)

    for scenario in scenarios:
        print(
            scenario["name"],
            "passing groups:",
            scenario["passing_group_count"],
        )

    print(
        "Candidate scenario pass:",
        candidate_results,
    )
    print("Research decision: HOLD")
    print("Reports:", output_folder)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
