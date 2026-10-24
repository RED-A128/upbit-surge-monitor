#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Feature Redesign Research
Clean V001

Purpose:
    Build a reproducible feature-redesign research plan
    from the completed research redesign gate.

This stage DOES NOT:
    - calculate new trading signals
    - run a new backtest
    - claim statistical independence
    - approve live scanner deployment
    - execute automated trades

Outputs:
    feature_redesign_plan.csv
    feature_redesign_protocol.json
    feature_redesign_result.json
    feature_redesign_manifest.json

Safety:
    - Validate source SHA256
    - Validate nine original research groups
    - Preserve negative results
    - Never overwrite source files
    - Atomic output writes
    - Resume only after integrity validation
"""

import argparse
import csv
import hashlib
import io
import json
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_DIRECTORY = (
    "data/reports/pattern_256_research_redesign"
)

OUTPUT_DIRECTORY = (
    "data/reports/pattern_256_feature_redesign"
)

SOURCE_CSV = "research_redesign_comparison.csv"
SOURCE_RESULT = "research_redesign_result.json"
SOURCE_MANIFEST = "research_redesign_manifest.json"

OUTPUT_CSV = "feature_redesign_plan.csv"
OUTPUT_PROTOCOL = "feature_redesign_protocol.json"
OUTPUT_RESULT = "feature_redesign_result.json"
OUTPUT_MANIFEST = "feature_redesign_manifest.json"

EXPECTED_GROUPS = {
    ("h1", 24),
    ("h1", 72),
    ("h1", 168),
    ("h4", 24),
    ("h4", 72),
    ("h4", 168),
    ("d1", 24),
    ("d1", 72),
    ("d1", 168),
}

PLAN_COLUMNS = [
    "research_id",
    "timeframe",
    "horizon",
    "source_classification",
    "priority",
    "feature_family",
    "hypothesis",
    "parameter_candidates",
    "required_inputs",
    "validation_design",
    "minimum_independent_events",
    "acceptance_rule",
    "status",
    "approved_for_live_scanner",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        return json.load(stream)


def load_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)
        return reader.fieldnames, list(reader)


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=".feature_redesign_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(path, payload):
    content = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )

    atomic_write(
        path,
        (content + "\n").encode("utf-8"),
    )


def save_csv(path, rows):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=PLAN_COLUMNS,
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(
        path,
        buffer.getvalue().encode("utf-8"),
    )


def validate_safety(result):
    for field in (
        "statistical_independence_proven",
        "independent_oos_proven",
        "profitability_proven",
        "approved_for_live_trading",
    ):
        require(
            result.get(field) is False,
            f"Invalid safety field: {field}",
        )


def validate_source(source_dir):
    paths = {
        SOURCE_CSV: source_dir / SOURCE_CSV,
        SOURCE_RESULT: source_dir / SOURCE_RESULT,
        SOURCE_MANIFEST: source_dir / SOURCE_MANIFEST,
    }

    for name, path in paths.items():
        require(
            path.is_file(),
            f"Missing source: {name}",
        )

    result = load_json(paths[SOURCE_RESULT])
    manifest = load_json(paths[SOURCE_MANIFEST])

    require(
        result.get("version") == "Clean V001",
        "Unexpected source version",
    )

    require(
        result.get("execution_status") == "PASS",
        "Source execution not PASS",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Source research decision not HOLD",
    )

    require(
        result.get("comparison_rows") == 9,
        "Unexpected source comparison count",
    )

    require(
        result.get("recommended_next_stage")
        == "FEATURE_REDESIGN_RESEARCH",
        "Unexpected source next stage",
    )

    validate_safety(result)

    require(
        manifest.get("version") == "Clean V001",
        "Unexpected source manifest version",
    )

    hashes = manifest.get(
        "output_sha256",
        {},
    )

    for name in (SOURCE_CSV, SOURCE_RESULT):
        require(
            hashes.get(name) == sha256(paths[name]),
            f"Source integrity failure: {name}",
        )

    return paths, result


def validate_groups(csv_path):
    fieldnames, rows = load_csv(csv_path)

    required = {
        "timeframe",
        "horizon",
        "redesign_classification",
        "recommended_research_action",
        "research_decision",
        "eligible_for_live_scanner",
    }

    require(
        required.issubset(set(fieldnames or [])),
        "Missing source columns",
    )

    require(
        len(rows) == 9,
        "Expected exactly nine source rows",
    )

    groups = {}

    for row in rows:
        key = (
            row["timeframe"],
            int(row["horizon"]),
        )

        require(
            key not in groups,
            f"Duplicate group: {key}",
        )

        require(
            row["research_decision"] == "HOLD",
            f"Unexpected decision: {key}",
        )

        require(
            row["eligible_for_live_scanner"] == "false",
            f"Unexpected scanner approval: {key}",
        )

        groups[key] = row

    require(
        set(groups) == EXPECTED_GROUPS,
        "Source research groups mismatch",
    )

    return groups


def create_hypotheses():
    """
    Candidate parameter values are exploratory hypotheses.

    They are NOT optimized values and NOT proven signals.

    Parameter definitions must be implemented and frozen
    before any new independent validation.
    """

    return {
        "ACCUMULATION_VOLUME": {
            "hypothesis": (
                "Relative volume and turnover persistence "
                "may distinguish accumulation from noise."
            ),
            "parameters": {
                "volume_lookback_bars": [20, 40, 60],
                "relative_volume_threshold": [1.2, 1.5, 2.0],
                "persistence_bars": [2, 3, 5],
            },
            "inputs": [
                "OHLCV",
                "trade_value",
            ],
        },

        "PRICE_COMPRESSION": {
            "hypothesis": (
                "Volatility contraction before a breakout "
                "may improve candidate selection."
            ),
            "parameters": {
                "compression_lookback_bars": [10, 20, 30],
                "atr_period": [14],
                "atr_ratio_threshold": [0.6, 0.8, 1.0],
            },
            "inputs": [
                "OHLCV",
            ],
        },

        "TREND_STRUCTURE": {
            "hypothesis": (
                "Trend alignment and resistance proximity "
                "may filter weak pattern-256 events."
            ),
            "parameters": {
                "short_ma": [20],
                "medium_ma": [60],
                "long_ma": [120],
                "breakout_lookback_bars": [20, 40],
            },
            "inputs": [
                "OHLCV",
            ],
        },

        "REGIME_FILTER": {
            "hypothesis": (
                "Separating market regimes may explain "
                "TRAIN and TEST direction reversals."
            ),
            "parameters": {
                "regime_lookback_bars": [30, 60, 90],
                "up_threshold": [0.08, 0.10],
                "down_threshold": [0.08, 0.10],
            },
            "inputs": [
                "OHLCV",
                "market_regime",
            ],
        },
    }


def select_families(classification):
    if classification == "POSITIVE_BUT_UNCERTAIN":
        return (
            "HIGH",
            [
                "ACCUMULATION_VOLUME",
                "PRICE_COMPRESSION",
                "TREND_STRUCTURE",
            ],
        )

    if classification == "DIRECTION_UNSTABLE":
        return (
            "MEDIUM",
            [
                "REGIME_FILTER",
            ],
        )

    if classification == "INSUFFICIENT_SAMPLE":
        return (
            "DATA_REQUIRED",
            [],
        )

    if classification == "CONSISTENT_NONPOSITIVE":
        return (
            "REJECT_CURRENT",
            [],
        )

    raise ValueError(
        f"Unknown classification: {classification}"
    )


def build_plan(groups, hypotheses):
    plan = []

    for timeframe, horizon in sorted(
        EXPECTED_GROUPS
    ):
        source = groups[
            (timeframe, horizon)
        ]

        classification = source[
            "redesign_classification"
        ]

        priority, families = select_families(
            classification
        )

        if not families:
            families = ["NO_FEATURE_PROPOSAL"]

        for family in families:
            if family == "NO_FEATURE_PROPOSAL":
                hypothesis = (
                    "No new feature is proposed until "
                    "the identified research limitation "
                    "has been resolved."
                )

                parameters = {}
                inputs = []

            else:
                definition = hypotheses[family]

                hypothesis = definition["hypothesis"]
                parameters = definition["parameters"]
                inputs = definition["inputs"]

            research_id = (
                f"P256_{timeframe.upper()}_"
                f"{horizon}_{family}"
            )

            if priority == "DATA_REQUIRED":
                status = "WAIT_FOR_INDEPENDENT_DATA"

            elif priority == "REJECT_CURRENT":
                status = "CURRENT_COMBINATION_REJECTED"

            else:
                status = "HYPOTHESIS_ONLY"

            plan.append({
                "research_id": research_id,
                "timeframe": timeframe,
                "horizon": horizon,
                "source_classification": classification,
                "priority": priority,
                "feature_family": family,
                "hypothesis": hypothesis,
                "parameter_candidates": json.dumps(
                    parameters,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "required_inputs": "|".join(inputs),
                "validation_design": (
                    "TRAIN_ONLY_SELECTION_THEN_FROZEN_"
                    "FUTURE_OOS"
                ),
                "minimum_independent_events": 100,
                "acceptance_rule": (
                    "POSITIVE_FUTURE_OOS_EFFECT_AND_"
                    "CLUSTER_ROBUST_UNCERTAINTY_"
                    "AND_MULTIPLE_TESTING_CONTROL"
                ),
                "status": status,
                "approved_for_live_scanner": "false",
            })

    require(
        len(plan) == 15,
        "Unexpected hypothesis count",
    )

    require(
        len({
            row["research_id"]
            for row in plan
        }) == len(plan),
        "Duplicate research IDs",
    )

    return plan


def build_protocol():
    return {
        "version": VERSION,
        "purpose": "FEATURE_REDESIGN_RESEARCH",

        "design_status": "EXPLORATORY_NOT_VALIDATED",

        "original_target": {
            "surge_threshold_pct": 30.0,
            "timeframes": ["h1", "h4", "d1"],
            "horizons": [24, 72, 168],
        },

        "feature_rules": {
            "no_future_information": True,
            "past_and_current_closed_candles_only": True,
            "all_parameters_frozen_before_oos": True,
            "train_only_parameter_selection": True,
            "no_test_set_tuning": True,
            "no_reuse_of_prior_test_as_new_independent_oos": True,
            "market_listing_age_must_be_checked": True,
            "ohlcv_integrity_required": True,
            "delisted_market_bias_must_be_reviewed": True,
        },

        "validation_requirements": {
            "prospective_or_genuinely_untouched_oos": True,
            "nonoverlapping_event_analysis": True,
            "cross_market_dependence_review": True,
            "cluster_robust_uncertainty": True,
            "multiple_testing_adjustment": True,
            "report_all_attempted_hypotheses": True,
            "retain_failed_hypotheses": True,
            "minimum_independent_events_per_group": 100,
            "market_regime_stratification": True,
            "trading_cost_analysis_before_profitability_claim": True,
        },

        "important_note": (
            "A minimum of 100 independent events is a "
            "planning floor, not a guarantee of adequate "
            "statistical power."
        ),

        "research_decision": "HOLD",
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "automated_trading_enabled": False,
    }


def validate_existing(
    output_dir,
    input_hashes,
):
    paths = {
        OUTPUT_CSV: output_dir / OUTPUT_CSV,
        OUTPUT_PROTOCOL: output_dir / OUTPUT_PROTOCOL,
        OUTPUT_RESULT: output_dir / OUTPUT_RESULT,
        OUTPUT_MANIFEST: output_dir / OUTPUT_MANIFEST,
    }

    if not all(
        path.is_file()
        for path in paths.values()
    ):
        return False

    try:
        result = load_json(paths[OUTPUT_RESULT])
        manifest = load_json(paths[OUTPUT_MANIFEST])
        protocol = load_json(paths[OUTPUT_PROTOCOL])

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        if result.get("hypothesis_rows") != 15:
            return False

        validate_safety(result)
        validate_safety(protocol)

        if protocol.get("automated_trading_enabled") is not False:
            return False

        if manifest.get("version") != VERSION:
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        hashes = manifest.get("output_sha256", {})

        for name in (
            OUTPUT_CSV,
            OUTPUT_PROTOCOL,
            OUTPUT_RESULT,
        ):
            if hashes.get(name) != sha256(paths[name]):
                return False

        fields, rows = load_csv(paths[OUTPUT_CSV])

        if fields != PLAN_COLUMNS:
            return False

        if len(rows) != 15:
            return False

        if any(
            row["approved_for_live_scanner"] != "false"
            for row in rows
        ):
            return False

        return True

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()

    source_dir = root / SOURCE_DIRECTORY
    output_dir = root / OUTPUT_DIRECTORY

    require(
        source_dir.is_dir(),
        f"Missing source directory: {source_dir}",
    )

    paths, source_result = validate_source(
        source_dir
    )

    input_hashes = {
        name: sha256(path)
        for name, path in paths.items()
    }

    if (
        not args.force
        and validate_existing(
            output_dir,
            input_hashes,
        )
    ):
        print("PASS: existing outputs verified")
        print("SKIP: completed research plan")
        return 0

    groups = validate_groups(
        paths[SOURCE_CSV]
    )

    hypotheses = create_hypotheses()

    plan = build_plan(
        groups,
        hypotheses,
    )

    protocol = build_protocol()

    priorities = dict(
        Counter(
            row["priority"]
            for row in plan
        )
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",

        "source_rows": source_result["source_rows"],
        "selected_events": source_result["selected_events"],

        "source_research_groups": 9,
        "hypothesis_rows": len(plan),

        "priority_counts": priorities,

        "new_backtest_completed": False,
        "new_feature_performance_proven": False,

        "research_decision": "HOLD",

        "recommended_next_stage": (
            "IMPLEMENT_FEATURE_CANDIDATES_AND_"
            "TRAIN_ONLY_EXPERIMENT"
        ),

        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,

        "limitations": [
            "This stage creates research hypotheses only.",
            "No new feature performance has been measured.",
            "Candidate parameters are not optimized.",
            "Historical test data cannot become fresh OOS.",
            "No live scanner is approved.",
            "No automated trading is enabled.",
        ],

        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_paths = {
        OUTPUT_CSV: output_dir / OUTPUT_CSV,
        OUTPUT_PROTOCOL: output_dir / OUTPUT_PROTOCOL,
        OUTPUT_RESULT: output_dir / OUTPUT_RESULT,
        OUTPUT_MANIFEST: output_dir / OUTPUT_MANIFEST,
    }

    save_csv(
        output_paths[OUTPUT_CSV],
        plan,
    )

    save_json(
        output_paths[OUTPUT_PROTOCOL],
        protocol,
    )

    save_json(
        output_paths[OUTPUT_RESULT],
        result,
    )

    manifest = {
        "version": VERSION,
        "input_sha256": input_hashes,

        "output_sha256": {
            name: sha256(output_paths[name])
            for name in (
                OUTPUT_CSV,
                OUTPUT_PROTOCOL,
                OUTPUT_RESULT,
            )
        },
    }

    save_json(
        output_paths[OUTPUT_MANIFEST],
        manifest,
    )

    require(
        validate_existing(
            output_dir,
            input_hashes,
        ),
        "Final integrity verification failed",
    )

    print("=" * 65)
    print("PATTERN 256 FEATURE REDESIGN RESEARCH")
    print("=" * 65)

    print("Execution: PASS")
    print("Version:", VERSION)
    print("Source groups: 9")
    print("Hypothesis rows:", len(plan))

    for priority, count in sorted(
        priorities.items()
    ):
        print(f"{priority}: {count}")

    print("New backtest completed: False")
    print("Research decision: HOLD")
    print("Live trading approved: False")
    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")

    print("=" * 65)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
