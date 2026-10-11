
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
research_pattern_256_accumulation_candidate_rules.py
Clean V001

Purpose
-------
Research descriptive candidate-selection rules using previously
validated, risk-adjusted Pattern 256 + Accumulation reports.

IMPORTANT
---------
1. Research only. No live trading or order execution.
2. All upstream reports are read-only.
3. No OHLCV modification.
4. No future outcome may become a real-time signal feature.
5. Historical outcome statistics are used only to evaluate rules.
6. No statistical significance or out-of-sample claim.
7. No automatic threshold optimization on the same evaluation data.
8. Existing output files are never silently overwritten.

Required upstream files
-----------------------
data/reports/pattern_256_accumulation_risk_adjusted/
    risk_adjusted_groups.csv
    risk_adjusted_comparison.csv
    risk_adjusted_result.json
    risk_adjusted_manifest.json

Outputs
-------
data/reports/pattern_256_accumulation_candidate_rules/
    candidate_rule_research.csv
    candidate_rule_comparison.csv
    candidate_rule_result.json
    candidate_rule_manifest.json

Execution
---------
py research_pattern_256_accumulation_candidate_rules.py

Optional isolated root
----------------------
py research_pattern_256_accumulation_candidate_rules.py --root PATH
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROGRAM = "research_pattern_256_accumulation_candidate_rules.py"
VERSION = "Clean V001"

SOURCE_RELATIVE = Path(
    "data/reports/pattern_256_accumulation_risk_adjusted"
)

OUTPUT_RELATIVE = Path(
    "data/reports/pattern_256_accumulation_candidate_rules"
)

INPUT_FILES = (
    "risk_adjusted_groups.csv",
    "risk_adjusted_comparison.csv",
    "risk_adjusted_result.json",
    "risk_adjusted_manifest.json",
)

OUTPUT_FILES = (
    "candidate_rule_research.csv",
    "candidate_rule_comparison.csv",
    "candidate_rule_result.json",
    "candidate_rule_manifest.json",
)

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
STOP_LEVELS = (5, 10, 15, 20)

CLASSIFICATIONS = (
    "PATTERN_256_ONLY",
    "BOTH",
)

# Predeclared research conditions.
# These are NOT optimized from the current input data.
#
# min_hit_rate_pct:
#     Historical 30% target-hit rate.
#
# max_stop_touch_pct:
#     Historical stop-level touch frequency.
#
# min_hit_minus_stop_pp:
#     Hit rate minus stop-touch rate, in percentage points.
#
# This difference is NOT an expected trading return.

RULES = (
    {
        "rule_id": "R00_BASELINE",
        "description": "Descriptive baseline; no performance filter",
        "min_hit_rate_pct": 0.0,
        "max_stop_touch_pct": 100.0,
        "min_hit_minus_stop_pp": -100.0,
    },
    {
        "rule_id": "R01_BALANCED",
        "description": "Hit >= 10%, stop touch <= 50%",
        "min_hit_rate_pct": 10.0,
        "max_stop_touch_pct": 50.0,
        "min_hit_minus_stop_pp": -100.0,
    },
    {
        "rule_id": "R02_CONSERVATIVE",
        "description": "Hit >= 20%, stop touch <= 35%",
        "min_hit_rate_pct": 20.0,
        "max_stop_touch_pct": 35.0,
        "min_hit_minus_stop_pp": -100.0,
    },
    {
        "rule_id": "R03_RISK_BALANCE",
        "description": "Hit minus stop-touch >= 0 pp",
        "min_hit_rate_pct": 0.0,
        "max_stop_touch_pct": 100.0,
        "min_hit_minus_stop_pp": 0.0,
    },
    {
        "rule_id": "R04_STRICT",
        "description": "Hit >= 30%, stop touch <= 25%",
        "min_hit_rate_pct": 30.0,
        "max_stop_touch_pct": 25.0,
        "min_hit_minus_stop_pp": 0.0,
    },
)

RESEARCH_COLUMNS = (
    "rule_id",
    "timeframe",
    "horizon",
    "stop_loss_pct",
    "classification",
    "historical_hit_rate_pct",
    "historical_stop_touched_pct",
    "historical_hit_minus_stop_pp",
    "meets_historical_rule",
    "research_status",
    "research_reason",
)

COMPARISON_COLUMNS = (
    "rule_id",
    "timeframe",
    "horizon",
    "stop_loss_pct",
    "only_meets_rule",
    "both_meets_rule",
    "historical_preference",
    "only_hit_rate_pct",
    "both_hit_rate_pct",
    "hit_rate_difference_pp",
    "only_stop_touched_pct",
    "both_stop_touched_pct",
    "stop_touch_difference_pp",
    "interpretation",
)


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def fail(message: str) -> None:
    raise ValueError(message)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as stream:
        value = json.load(stream)

    if not isinstance(value, dict):
        fail(f"Expected JSON object: {path}")

    return value


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            fail(f"CSV header missing: {path}")

        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            fail(f"Duplicate CSV columns: {path}")

        rows = list(reader)

    return rows


def require_columns(
    rows: list[dict[str, str]],
    columns: tuple[str, ...],
    filename: str,
) -> None:
    if not rows:
        fail(f"CSV contains no data rows: {filename}")

    existing = set(rows[0].keys())
    missing = sorted(set(columns) - existing)

    if missing:
        fail(
            f"Missing required columns in {filename}: "
            + ", ".join(missing)
        )


def finite_number(
    value: Any,
    field: str,
    row_number: int,
) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        fail(
            f"Invalid numeric value: "
            f"row={row_number}, field={field}, value={value!r}"
        )

    if not math.isfinite(number):
        fail(
            f"Non-finite number: "
            f"row={row_number}, field={field}"
        )

    return number


def exact_integer(
    value: Any,
    field: str,
    row_number: int,
) -> int:
    number = finite_number(value, field, row_number)

    if not number.is_integer():
        fail(
            f"Expected integer: "
            f"row={row_number}, field={field}"
        )

    return int(number)


def percentage(
    value: Any,
    field: str,
    row_number: int,
) -> float:
    number = finite_number(value, field, row_number)

    if number < 0.0 or number > 100.0:
        fail(
            f"Percentage outside [0,100]: "
            f"row={row_number}, field={field}"
        )

    return number


def verify_source_metadata(
    result: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    if result.get("status") != "PASS":
        fail("Upstream result status is not PASS.")

    if manifest.get("status") != "PASS":
        fail("Upstream manifest status is not PASS.")

    expected_program = (
        "validate_pattern_256_accumulation_risk_adjusted.py"
    )

    if result.get("program") != expected_program:
        fail("Unexpected upstream result program.")

    if manifest.get("program") != expected_program:
        fail("Unexpected upstream manifest program.")

    result_version = result.get("version")
    manifest_version = manifest.get("version")

    if result_version != manifest_version:
        fail("Upstream result/manifest version mismatch.")

    if not isinstance(result_version, str) or not result_version:
        fail("Missing upstream version.")

    if result.get("assessment") != (
        "DESCRIPTIVE_RISK_COMPARISON_ONLY"
    ):
        fail("Unexpected upstream assessment.")

    methodology = result.get("methodology")

    if not isinstance(methodology, dict):
        fail("Missing upstream methodology.")

    expected_false_fields = (
        "margin_is_trading_return",
        "statistical_significance_tested",
        "intrabar_order_resolved",
        "out_of_sample_verified",
    )

    for field in expected_false_fields:
        if methodology.get(field) is not False:
            fail(
                f"Unsupported upstream methodology claim: {field}"
            )


def verify_manifest_hashes(
    source_dir: Path,
    result: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    outputs = manifest.get("outputs")

    if not isinstance(outputs, dict):
        fail("Upstream manifest outputs missing.")

    for filename in (
        "risk_adjusted_groups.csv",
        "risk_adjusted_comparison.csv",
        "risk_adjusted_result.json",
    ):
        expected = outputs.get(filename)

        if not isinstance(expected, str) or len(expected) != 64:
            fail(f"Missing upstream manifest SHA256: {filename}")

        actual = sha256_file(source_dir / filename)

        if actual.lower() != expected.lower():
            fail(f"Upstream SHA256 mismatch: {filename}")

    result_hash_fields = {
        "risk_adjusted_groups.csv": "risk_adjusted_groups_sha256",
        "risk_adjusted_comparison.csv":
            "risk_adjusted_comparison_sha256",
    }

    for filename, field in result_hash_fields.items():
        expected = result.get(field)

        if not isinstance(expected, str) or len(expected) != 64:
            fail(f"Missing upstream result SHA256: {field}")

        actual = sha256_file(source_dir / filename)

        if actual.lower() != expected.lower():
            fail(f"Upstream result SHA256 mismatch: {filename}")


def group_key(
    row: dict[str, str],
    number: int,
) -> tuple[str, int, int, str]:
    timeframe = row["timeframe"].strip()
    horizon = exact_integer(row["horizon"], "horizon", number)
    stop = exact_integer(
        row["stop_loss_pct"],
        "stop_loss_pct",
        number,
    )
    classification = row["classification"].strip()

    if timeframe not in TIMEFRAMES:
        fail(f"Unexpected timeframe in group row {number}.")

    if horizon not in HORIZONS:
        fail(f"Unexpected horizon in group row {number}.")

    if stop not in STOP_LEVELS:
        fail(f"Unexpected stop level in group row {number}.")

    if classification not in CLASSIFICATIONS:
        fail(f"Unexpected classification in group row {number}.")

    return timeframe, horizon, stop, classification


def comparison_key(
    row: dict[str, str],
    number: int,
) -> tuple[str, int, int]:
    timeframe = row["timeframe"].strip()
    horizon = exact_integer(row["horizon"], "horizon", number)
    stop = exact_integer(
        row["stop_loss_pct"],
        "stop_loss_pct",
        number,
    )

    if timeframe not in TIMEFRAMES:
        fail(f"Unexpected comparison timeframe: row {number}")

    if horizon not in HORIZONS:
        fail(f"Unexpected comparison horizon: row {number}")

    if stop not in STOP_LEVELS:
        fail(f"Unexpected comparison stop level: row {number}")

    return timeframe, horizon, stop


def validate_input_tables(
    groups: list[dict[str, str]],
    comparisons: list[dict[str, str]],
    result: dict[str, Any],
) -> tuple[
    dict[tuple[str, int, int, str], dict[str, str]],
    dict[tuple[str, int, int], dict[str, str]],
]:
    require_columns(
        groups,
        (
            "timeframe",
            "horizon",
            "stop_loss_pct",
            "classification",
        ),
        "risk_adjusted_groups.csv",
    )

    require_columns(
        comparisons,
        (
            "timeframe",
            "horizon",
            "stop_loss_pct",
            "only_hit_rate_pct",
            "both_hit_rate_pct",
            "hit_rate_difference_pp",
            "only_stop_touched_pct",
            "both_stop_touched_pct",
            "stop_touch_difference_pp",
            "interpretation",
        ),
        "risk_adjusted_comparison.csv",
    )

    if len(groups) != 72:
        fail(f"Expected 72 group rows, found {len(groups)}.")

    if len(comparisons) != 36:
        fail(
            f"Expected 36 comparison rows, "
            f"found {len(comparisons)}."
        )

    if exact_integer(
        result.get("group_rows"),
        "group_rows",
        0,
    ) != len(groups):
        fail("Upstream group count mismatch.")

    if exact_integer(
        result.get("comparison_rows"),
        "comparison_rows",
        0,
    ) != len(comparisons):
        fail("Upstream comparison count mismatch.")

    group_index = {}

    for number, row in enumerate(groups, start=2):
        key = group_key(row, number)

        if key in group_index:
            fail(f"Duplicate group key: {key}")

        group_index[key] = row

    comparison_index = {}

    for number, row in enumerate(comparisons, start=2):
        key = comparison_key(row, number)

        if key in comparison_index:
            fail(f"Duplicate comparison key: {key}")

        comparison_index[key] = row

        only_hit = percentage(
            row["only_hit_rate_pct"],
            "only_hit_rate_pct",
            number,
        )
        both_hit = percentage(
            row["both_hit_rate_pct"],
            "both_hit_rate_pct",
            number,
        )
        only_stop = percentage(
            row["only_stop_touched_pct"],
            "only_stop_touched_pct",
            number,
        )
        both_stop = percentage(
            row["both_stop_touched_pct"],
            "both_stop_touched_pct",
            number,
        )

        hit_diff = finite_number(
            row["hit_rate_difference_pp"],
            "hit_rate_difference_pp",
            number,
        )
        stop_diff = finite_number(
            row["stop_touch_difference_pp"],
            "stop_touch_difference_pp",
            number,
        )

        tolerance = 0.00001

        if abs((both_hit - only_hit) - hit_diff) > tolerance:
            fail(f"HIT difference mismatch: {key}")

        if abs((both_stop - only_stop) - stop_diff) > tolerance:
            fail(f"Stop difference mismatch: {key}")

        if row["interpretation"].strip() != (
            "NOT_A_TRADE_PERFORMANCE_ESTIMATE"
        ):
            fail(f"Unexpected interpretation: {key}")

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for stop in STOP_LEVELS:
                key = (timeframe, horizon, stop)

                if key not in comparison_index:
                    fail(f"Missing comparison: {key}")

                for classification in CLASSIFICATIONS:
                    group = (*key, classification)

                    if group not in group_index:
                        fail(f"Missing group: {group}")

    return group_index, comparison_index


def evaluate_rule(
    rule: dict[str, Any],
    hit_rate: float,
    stop_rate: float,
) -> bool:
    margin = hit_rate - stop_rate

    return (
        hit_rate >= rule["min_hit_rate_pct"]
        and stop_rate <= rule["max_stop_touch_pct"]
        and margin >= rule["min_hit_minus_stop_pp"]
    )


def build_research(
    comparison_index: dict[
        tuple[str, int, int],
        dict[str, str],
    ],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    research_rows = []
    comparison_rows = []

    for rule in RULES:
        for timeframe in TIMEFRAMES:
            for horizon in HORIZONS:
                for stop in STOP_LEVELS:
                    key = (timeframe, horizon, stop)
                    source = comparison_index[key]

                    only_hit = float(
                        source["only_hit_rate_pct"]
                    )
                    both_hit = float(
                        source["both_hit_rate_pct"]
                    )
                    only_stop = float(
                        source["only_stop_touched_pct"]
                    )
                    both_stop = float(
                        source["both_stop_touched_pct"]
                    )

                    only_pass = evaluate_rule(
                        rule,
                        only_hit,
                        only_stop,
                    )

                    both_pass = evaluate_rule(
                        rule,
                        both_hit,
                        both_stop,
                    )

                    cases = (
                        (
                            "PATTERN_256_ONLY",
                            only_hit,
                            only_stop,
                            only_pass,
                        ),
                        (
                            "BOTH",
                            both_hit,
                            both_stop,
                            both_pass,
                        ),
                    )

                    for classification, hit, stop_rate, passed in cases:
                        research_rows.append(
                            {
                                "rule_id": rule["rule_id"],
                                "timeframe": timeframe,
                                "horizon": horizon,
                                "stop_loss_pct": stop,
                                "classification": classification,
                                "historical_hit_rate_pct": hit,
                                "historical_stop_touched_pct":
                                    stop_rate,
                                "historical_hit_minus_stop_pp":
                                    hit - stop_rate,
                                "meets_historical_rule":
                                    int(passed),
                                "research_status":
                                    "DESCRIPTIVE_ONLY",
                                "research_reason":
                                    "Historical outcome filter; "
                                    "not a live signal condition",
                            }
                        )

                    if both_pass and not only_pass:
                        preference = "BOTH_ONLY_PASSES"
                    elif only_pass and not both_pass:
                        preference = "PATTERN_256_ONLY_PASSES"
                    elif both_pass and only_pass:
                        preference = "BOTH_GROUPS_PASS"
                    else:
                        preference = "NEITHER_GROUP_PASSES"

                    comparison_rows.append(
                        {
                            "rule_id": rule["rule_id"],
                            "timeframe": timeframe,
                            "horizon": horizon,
                            "stop_loss_pct": stop,
                            "only_meets_rule": int(only_pass),
                            "both_meets_rule": int(both_pass),
                            "historical_preference": preference,
                            "only_hit_rate_pct": only_hit,
                            "both_hit_rate_pct": both_hit,
                            "hit_rate_difference_pp":
                                both_hit - only_hit,
                            "only_stop_touched_pct": only_stop,
                            "both_stop_touched_pct": both_stop,
                            "stop_touch_difference_pp":
                                both_stop - only_stop,
                            "interpretation":
                                "HISTORICAL_DESCRIPTIVE_ONLY",
                        }
                    )

    return research_rows, comparison_rows


def write_csv(
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=list(columns),
            extrasaction="raise",
        )

        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    value: dict[str, Any],
) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(
            value,
            stream,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        stream.write("\n")


def resolve_root(argument: str | None) -> Path:
    if argument:
        return Path(argument).expanduser().resolve()

    return Path(__file__).resolve().parent


def snapshot_inputs(
    source_dir: Path,
) -> dict[str, str]:
    hashes = {}

    for filename in INPUT_FILES:
        path = source_dir / filename

        if not path.is_file():
            fail(f"Required upstream file missing: {path}")

        hashes[filename] = sha256_file(path)

    return hashes


def verify_unchanged(
    source_dir: Path,
    expected: dict[str, str],
) -> None:
    for filename, expected_hash in expected.items():
        path = source_dir / filename

        if not path.is_file():
            fail(f"Upstream input disappeared: {filename}")

        if sha256_file(path) != expected_hash:
            fail(f"Upstream input changed: {filename}")


def prepare_output(
    root: Path,
) -> tuple[Path, Path]:
    final_dir = root / OUTPUT_RELATIVE

    # Refuse all existing outputs. This protects prior research.
    if final_dir.exists():
        fail(
            "Output directory already exists. "
            "Refusing to overwrite: "
            f"{final_dir}"
        )

    parent = final_dir.parent
    parent.mkdir(parents=True, exist_ok=True)

    temporary_dir = Path(
        tempfile.mkdtemp(
            prefix=".candidate_rules_staging_",
            dir=str(parent),
        )
    )

    return temporary_dir, final_dir


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Research historical candidate rules."
    )

    parser.add_argument(
        "--root",
        default=None,
        help="Project root; defaults to script directory.",
    )

    args = parser.parse_args()

    root = resolve_root(args.root)
    source_dir = root / SOURCE_RELATIVE

    log("=" * 65)
    log(f"{PROGRAM} / {VERSION}")
    log("=" * 65)

    if not source_dir.is_dir():
        fail(f"Upstream report directory missing: {source_dir}")

    log("[CHECK] Capturing upstream SHA256")
    input_hashes = snapshot_inputs(source_dir)

    result = load_json(
        source_dir / "risk_adjusted_result.json"
    )
    manifest = load_json(
        source_dir / "risk_adjusted_manifest.json"
    )

    verify_source_metadata(result, manifest)
    verify_manifest_hashes(source_dir, result, manifest)

    log("[PASS] Upstream metadata and hashes")

    groups = read_csv(
        source_dir / "risk_adjusted_groups.csv"
    )
    comparisons = read_csv(
        source_dir / "risk_adjusted_comparison.csv"
    )

    _, comparison_index = validate_input_tables(
        groups,
        comparisons,
        result,
    )

    log("[PASS] Upstream CSV accounting")

    research_rows, comparison_rows = build_research(
        comparison_index
    )

    expected_research_rows = (
        len(RULES)
        * len(TIMEFRAMES)
        * len(HORIZONS)
        * len(STOP_LEVELS)
        * len(CLASSIFICATIONS)
    )

    expected_comparison_rows = (
        len(RULES)
        * len(TIMEFRAMES)
        * len(HORIZONS)
        * len(STOP_LEVELS)
    )

    if len(research_rows) != expected_research_rows:
        fail("Research row accounting mismatch.")

    if len(comparison_rows) != expected_comparison_rows:
        fail("Comparison row accounting mismatch.")

    log("[PASS] Historical rule evaluation")

    # Check inputs again before writing output.
    verify_unchanged(source_dir, input_hashes)

    staging_dir, final_dir = prepare_output(root)

    research_path = (
        staging_dir / "candidate_rule_research.csv"
    )
    comparison_path = (
        staging_dir / "candidate_rule_comparison.csv"
    )
    result_path = (
        staging_dir / "candidate_rule_result.json"
    )
    manifest_path = (
        staging_dir / "candidate_rule_manifest.json"
    )

    write_csv(
        research_path,
        RESEARCH_COLUMNS,
        research_rows,
    )

    write_csv(
        comparison_path,
        COMPARISON_COLUMNS,
        comparison_rows,
    )

    counts = Counter(
        row["historical_preference"]
        for row in comparison_rows
    )

    output_hashes = {
        "candidate_rule_research.csv":
            sha256_file(research_path),
        "candidate_rule_comparison.csv":
            sha256_file(comparison_path),
    }

    result_data = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "source_program":
            "validate_pattern_256_accumulation_risk_adjusted.py",
        "source_version": result["version"],
        "source_group_rows": len(groups),
        "source_comparison_rows": len(comparisons),
        "rule_count": len(RULES),
        "research_rows": len(research_rows),
        "comparison_rows": len(comparison_rows),
        "historical_preference_counts": dict(counts),
        "rules": list(RULES),
        "input_sha256": input_hashes,
        "candidate_rule_research_sha256":
            output_hashes["candidate_rule_research.csv"],
        "candidate_rule_comparison_sha256":
            output_hashes["candidate_rule_comparison.csv"],
        "assessment":
            "DESCRIPTIVE_CANDIDATE_RULE_RESEARCH_ONLY",
        "methodology": {
            "historical_outcomes_used_for_evaluation": True,
            "historical_outcomes_allowed_as_live_features": False,
            "rule_thresholds_predeclared": True,
            "thresholds_optimized_on_current_data": False,
            "statistical_significance_tested": False,
            "out_of_sample_verified": False,
            "intrabar_order_resolved": False,
            "trading_return_estimated": False,
            "live_scanner_ready": False,
            "automatic_trading_enabled": False,
        },
        "limitations": [
            "Rule evaluation uses historical outcome aggregates.",
            "Historical hit and stop rates are not live features.",
            "Passing a rule does not establish predictive power.",
            "Sample-size uncertainty is not resolved here.",
            "Overlapping events may create dependent observations.",
            "Intrabar target/stop ordering is not established.",
            "No train/test temporal separation is performed.",
            "No transaction costs or executable trade returns.",
            "A separate event-level out-of-sample study is required.",
        ],
    }

    write_json(result_path, result_data)

    output_hashes[
        "candidate_rule_result.json"
    ] = sha256_file(result_path)

    manifest_data = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "inputs": input_hashes,
        "outputs": output_hashes,
    }

    write_json(manifest_path, manifest_data)

    # Verify all staged outputs before publication.
    if len(read_csv(research_path)) != expected_research_rows:
        fail("Written research CSV row count mismatch.")

    if len(read_csv(comparison_path)) != expected_comparison_rows:
        fail("Written comparison CSV row count mismatch.")

    for filename, expected_hash in output_hashes.items():
        actual_hash = sha256_file(staging_dir / filename)

        if actual_hash != expected_hash:
            fail(f"Staged output SHA256 mismatch: {filename}")

    if load_json(result_path).get("status") != "PASS":
        fail("Written result status invalid.")

    if load_json(manifest_path).get("status") != "PASS":
        fail("Written manifest status invalid.")

    # Check upstream inputs once more before publication.
    verify_unchanged(source_dir, input_hashes)

    if final_dir.exists():
        fail("Production output appeared during execution.")

    # Atomic directory publication on the same filesystem.
    os.rename(staging_dir, final_dir)

    log("[PASS] Candidate rule research completed")
    log(f"Source group rows: {len(groups):,}")
    log(f"Source comparison rows: {len(comparisons):,}")
    log(f"Predeclared rules: {len(RULES):,}")
    log(f"Research rows: {len(research_rows):,}")
    log(f"Comparison rows: {len(comparison_rows):,}")
    log(f"Output: {final_dir}")
    log("=" * 65)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log(f"[FAIL] {exc}")
        sys.exit(1)
