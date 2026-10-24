#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Overlap Sensitivity Audit
Clean V001

Purpose:
    Evaluate how uncertainty changes when effective sample
    sizes are reduced to account for possible event dependence.

This is a sensitivity analysis, NOT a direct overlap audit.

Input:
    data/reports/pattern_256_effect_uncertainty/
        effect_uncertainty_comparison.csv
        effect_uncertainty_result.json
        effect_uncertainty_manifest.json

Output:
    data/reports/pattern_256_overlap_sensitivity/
        overlap_sensitivity_comparison.csv
        overlap_sensitivity_result.json
        overlap_sensitivity_manifest.json

Safety:
    No source modification.
    No automated trading.
    No statistical independence claim.
    No profitability claim.
"""

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


VERSION = "Clean V001"

SOURCE_DIR = (
    "data/reports/pattern_256_effect_uncertainty"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_overlap_sensitivity"
)

SOURCE_FILES = (
    "effect_uncertainty_comparison.csv",
    "effect_uncertainty_result.json",
    "effect_uncertainty_manifest.json",
)

OUTPUT_CSV = "overlap_sensitivity_comparison.csv"
OUTPUT_RESULT = "overlap_sensitivity_result.json"
OUTPUT_MANIFEST = "overlap_sensitivity_manifest.json"

DESIGN_EFFECTS = (1, 2, 5, 10, 20, 50)

Z = 1.959963984540054

REQUIRED_COLUMNS = (
    "timeframe",
    "horizon",
    "regime",
    "stability_status",
    "test_only_events",
    "test_only_hits",
    "test_both_events",
    "test_both_hits",
    "test_uplift_pp",
    "research_priority",
)

OUTPUT_COLUMNS = (
    "timeframe",
    "horizon",
    "regime",
    "original_priority",
    "design_effect",
    "test_only_events",
    "test_only_hits",
    "test_both_events",
    "test_both_hits",
    "effective_only_events",
    "effective_both_events",
    "only_hit_rate_pct",
    "both_hit_rate_pct",
    "uplift_pp",
    "lower_bound_pp",
    "upper_bound_pp",
    "sensitivity_status",
    "research_decision",
)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def valid_sha256(value):
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(
            char in "0123456789abcdef"
            for char in value.lower()
        )
    )


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def atomic_bytes(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            descriptor,
            "wb",
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(path, payload):
    content = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    atomic_bytes(path, content)


def save_csv(path, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=OUTPUT_COLUMNS,
            )

            writer.writeheader()
            writer.writerows(rows)

            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parse_count(value, label):
    text = str(value).strip()

    if text == "":
        return None

    if not text.isdigit():
        raise ValueError(
            f"Invalid count: {label}={value}"
        )

    return int(text)


def parse_float(value, label):
    text = str(value).strip()

    if text == "":
        return None

    number = float(text)

    if not math.isfinite(number):
        raise ValueError(
            f"Invalid float: {label}"
        )

    return number


def validate_source(source):
    paths = {
        name: source / name
        for name in SOURCE_FILES
    }

    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing source: {name}"
            )

    result = load_json(
        paths["effect_uncertainty_result.json"]
    )

    manifest = load_json(
        paths["effect_uncertainty_manifest.json"]
    )

    if result.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected source version"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Source execution is not PASS"
        )

    if result.get("research_decision") != "HOLD":
        raise ValueError(
            "Source research decision is not HOLD"
        )

    for field in (
        "approved_for_live_trading",
        "statistical_independence_proven",
        "independent_oos_proven",
        "profitability_proven",
    ):
        if result.get(field) is not False:
            raise ValueError(
                f"Source safety field invalid: {field}"
            )

    if manifest.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected source manifest version"
        )

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError(
            "Missing source output hashes"
        )

    for name in SOURCE_FILES[:2]:
        expected = hashes.get(name)

        if not valid_sha256(expected):
            raise ValueError(
                f"Invalid source hash: {name}"
            )

        actual = sha256_file(paths[name])

        if actual != expected.lower():
            raise ValueError(
                f"Source hash mismatch: {name}"
            )

    input_hashes = {
        name: sha256_file(path)
        for name, path in paths.items()
    }

    return paths, result, input_hashes


def wilson(hits, effective_n):
    """
    Wilson interval with a hypothetical effective sample size.

    The observed proportion is preserved.

    effective_n = original_n / design_effect

    This is a scenario analysis only.
    """

    if effective_n <= 0:
        return None

    p = hits
    z2 = Z * Z

    denominator = 1.0 + z2 / effective_n

    center = (
        p + z2 / (2.0 * effective_n)
    ) / denominator

    radius = (
        Z
        * math.sqrt(
            (
                p * (1.0 - p)
                + z2 / (4.0 * effective_n)
            )
            / effective_n
        )
        / denominator
    )

    return (
        max(0.0, center - radius),
        min(1.0, center + radius),
    )


def calculate_interval(
    only_events,
    only_hits,
    both_events,
    both_hits,
    design_effect,
):
    if (
        only_events is None
        or both_events is None
        or only_events == 0
        or both_events == 0
    ):
        return None

    only_p = only_hits / only_events
    both_p = both_hits / both_events

    only_effective = (
        only_events / design_effect
    )

    both_effective = (
        both_events / design_effect
    )

    only_interval = wilson(
        only_p,
        only_effective,
    )

    both_interval = wilson(
        both_p,
        both_effective,
    )

    lower = (
        both_interval[0]
        - only_interval[1]
    ) * 100.0

    upper = (
        both_interval[1]
        - only_interval[0]
    ) * 100.0

    return {
        "only_rate": only_p * 100.0,
        "both_rate": both_p * 100.0,
        "uplift": (
            both_p - only_p
        ) * 100.0,
        "lower": lower,
        "upper": upper,
        "effective_only": only_effective,
        "effective_both": both_effective,
    }


def fmt(value):
    if value is None:
        return ""

    return f"{value:.8f}"


def read_source_csv(path, expected_rows):
    rows = []
    seen = set()

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames is None:
            raise ValueError(
                "Missing source CSV header"
            )

        for column in REQUIRED_COLUMNS:
            if column not in reader.fieldnames:
                raise ValueError(
                    f"Missing source column: {column}"
                )

        for line, raw in enumerate(
            reader,
            start=2,
        ):
            timeframe = raw["timeframe"].strip()
            regime = raw["regime"].strip()

            horizon = parse_count(
                raw["horizon"],
                f"line {line} horizon",
            )

            if timeframe not in ("h1", "h4", "d1"):
                raise ValueError(
                    f"Invalid timeframe at line {line}"
                )

            if horizon not in (24, 72, 168):
                raise ValueError(
                    f"Invalid horizon at line {line}"
                )

            if regime not in (
                "UP",
                "DOWN",
                "SIDEWAYS",
            ):
                raise ValueError(
                    f"Invalid regime at line {line}"
                )

            key = (
                timeframe,
                horizon,
                regime,
            )

            if key in seen:
                raise ValueError(
                    f"Duplicate source cell: {key}"
                )

            seen.add(key)

            record = {
                "timeframe": timeframe,
                "horizon": horizon,
                "regime": regime,
                "original_priority": (
                    raw["research_priority"].strip()
                ),
            }

            for field in (
                "test_only_events",
                "test_only_hits",
                "test_both_events",
                "test_both_hits",
            ):
                record[field] = parse_count(
                    raw[field],
                    f"line {line} {field}",
                )

            for prefix in (
                "test_only",
                "test_both",
            ):
                events = record[
                    f"{prefix}_events"
                ]

                hits = record[
                    f"{prefix}_hits"
                ]

                if (events is None) != (hits is None):
                    raise ValueError(
                        f"Incomplete counts at line {line}"
                    )

                if (
                    events is not None
                    and hits > events
                ):
                    raise ValueError(
                        f"Hits exceed events at line {line}"
                    )

            supplied_uplift = parse_float(
                raw["test_uplift_pp"],
                f"line {line} uplift",
            )

            only_events = record[
                "test_only_events"
            ]

            both_events = record[
                "test_both_events"
            ]

            if (
                only_events is None
                or both_events is None
                or only_events == 0
                or both_events == 0
            ):
                calculated_uplift = None

            else:
                calculated_uplift = 100.0 * (
                    record["test_both_hits"]
                    / both_events
                    - record["test_only_hits"]
                    / only_events
                )

            if (
                supplied_uplift is None
            ) != (
                calculated_uplift is None
            ):
                raise ValueError(
                    f"Uplift missingness mismatch at line {line}"
                )

            if (
                supplied_uplift is not None
                and abs(
                    supplied_uplift
                    - calculated_uplift
                ) > 0.00001
            ):
                raise ValueError(
                    f"Uplift mismatch at line {line}"
                )

            rows.append(record)

    if len(rows) != expected_rows:
        raise ValueError(
            "Source row count mismatch"
        )

    if len(rows) != 27:
        raise ValueError(
            "Expected 27 comparison cells"
        )

    expected_keys = {
        (timeframe, horizon, regime)
        for timeframe in ("h1", "h4", "d1")
        for horizon in (24, 72, 168)
        for regime in ("UP", "DOWN", "SIDEWAYS")
    }

    if seen != expected_keys:
        raise ValueError(
            "Source comparison cells incomplete"
        )

    return rows


def evaluate(records):
    output = []

    for record in records:
        for design_effect in DESIGN_EFFECTS:
            only_events = record[
                "test_only_events"
            ]

            only_hits = record[
                "test_only_hits"
            ]

            both_events = record[
                "test_both_events"
            ]

            both_hits = record[
                "test_both_hits"
            ]

            values = calculate_interval(
                only_events,
                only_hits,
                both_events,
                both_hits,
                design_effect,
            )

            if values is None:
                status = "NOT_AVAILABLE"

            elif values["lower"] > 0:
                status = "EXPLORATORY_POSITIVE"

            elif values["upper"] < 0:
                status = "EXPLORATORY_NEGATIVE"

            else:
                status = "CROSSES_ZERO"

            output.append({
                "timeframe": record["timeframe"],
                "horizon": record["horizon"],
                "regime": record["regime"],

                "original_priority": (
                    record["original_priority"]
                ),

                "design_effect": design_effect,

                "test_only_events": (
                    ""
                    if only_events is None
                    else only_events
                ),

                "test_only_hits": (
                    ""
                    if only_hits is None
                    else only_hits
                ),

                "test_both_events": (
                    ""
                    if both_events is None
                    else both_events
                ),

                "test_both_hits": (
                    ""
                    if both_hits is None
                    else both_hits
                ),

                "effective_only_events": fmt(
                    None
                    if values is None
                    else values["effective_only"]
                ),

                "effective_both_events": fmt(
                    None
                    if values is None
                    else values["effective_both"]
                ),

                "only_hit_rate_pct": fmt(
                    None
                    if values is None
                    else values["only_rate"]
                ),

                "both_hit_rate_pct": fmt(
                    None
                    if values is None
                    else values["both_rate"]
                ),

                "uplift_pp": fmt(
                    None
                    if values is None
                    else values["uplift"]
                ),

                "lower_bound_pp": fmt(
                    None
                    if values is None
                    else values["lower"]
                ),

                "upper_bound_pp": fmt(
                    None
                    if values is None
                    else values["upper"]
                ),

                "sensitivity_status": status,
                "research_decision": "HOLD",
            })

    return output


def summarize(rows):
    candidates = {
        (
            row["timeframe"],
            int(row["horizon"]),
            row["regime"],
        )
        for row in rows
        if row["original_priority"]
        == "EVENT_LEVEL_VALIDATION_CANDIDATE"
    }

    if len(candidates) != 7:
        raise ValueError(
            "Expected 7 original research candidates"
        )

    candidate_summary = []

    for key in sorted(candidates):
        relevant = [
            row
            for row in rows
            if (
                row["timeframe"],
                int(row["horizon"]),
                row["regime"],
            ) == key
        ]

        if len(relevant) != len(DESIGN_EFFECTS):
            raise ValueError(
                f"Missing sensitivity scenarios: {key}"
            )

        statuses = {
            int(row["design_effect"]):
            row["sensitivity_status"]
            for row in relevant
        }

        positive_effects = [
            effect
            for effect in DESIGN_EFFECTS
            if statuses[effect]
            == "EXPLORATORY_POSITIVE"
        ]

        candidate_summary.append({
            "timeframe": key[0],
            "horizon": key[1],
            "regime": key[2],

            "positive_design_effects": positive_effects,

            "positive_at_1": (
                statuses[1]
                == "EXPLORATORY_POSITIVE"
            ),

            "positive_at_2": (
                statuses[2]
                == "EXPLORATORY_POSITIVE"
            ),

            "positive_at_5": (
                statuses[5]
                == "EXPLORATORY_POSITIVE"
            ),

            "positive_at_10": (
                statuses[10]
                == "EXPLORATORY_POSITIVE"
            ),

            "positive_at_20": (
                statuses[20]
                == "EXPLORATORY_POSITIVE"
            ),

            "positive_at_50": (
                statuses[50]
                == "EXPLORATORY_POSITIVE"
            ),
        })

    return candidate_summary


def validate_existing(
    output_dir,
    input_hashes,
):
    paths = {
        name: output_dir / name
        for name in (
            OUTPUT_CSV,
            OUTPUT_RESULT,
            OUTPUT_MANIFEST,
        )
    }

    if not all(
        path.is_file()
        for path in paths.values()
    ):
        return False

    try:
        result = load_json(
            paths[OUTPUT_RESULT]
        )

        manifest = load_json(
            paths[OUTPUT_MANIFEST]
        )

        if result.get("version") != VERSION:
            return False

        if manifest.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        if result.get("approved_for_live_trading") is not False:
            return False

        if result.get("statistical_independence_proven") is not False:
            return False

        if result.get("independent_oos_proven") is not False:
            return False

        if result.get("profitability_proven") is not False:
            return False

        if result.get("design_effects") != list(DESIGN_EFFECTS):
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        hashes = manifest.get("output_sha256")

        if not isinstance(hashes, dict):
            return False

        for name in (
            OUTPUT_CSV,
            OUTPUT_RESULT,
        ):
            expected = hashes.get(name)

            if not valid_sha256(expected):
                return False

            if sha256_file(paths[name]) != expected.lower():
                return False

        with paths[OUTPUT_CSV].open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if reader.fieldnames != list(OUTPUT_COLUMNS):
                return False

            rows = list(reader)

        if len(rows) != 162:
            return False

        if result.get("comparison_rows") != len(rows):
            return False

        summary = summarize(rows)

        if result.get("candidate_summary") != summary:
            return False

        actual_counts = dict(
            sorted(
                Counter(
                    row["sensitivity_status"]
                    for row in rows
                ).items()
            )
        )

        if result.get("status_counts") != actual_counts:
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

    root = Path(
        args.root
    ).expanduser().resolve()

    source_dir = root / SOURCE_DIR
    output_dir = root / OUTPUT_DIR

    if not source_dir.is_dir():
        raise FileNotFoundError(
            f"Source directory missing: {source_dir}"
        )

    paths, source_result, input_hashes = (
        validate_source(source_dir)
    )

    if (
        not args.force
        and validate_existing(
            output_dir,
            input_hashes,
        )
    ):
        print(
            "PASS: existing output integrity verified"
        )

        print(
            "SKIP: completed analysis already exists"
        )

        return 0

    records = read_source_csv(
        paths["effect_uncertainty_comparison.csv"],
        source_result["comparison_rows"],
    )

    output_rows = evaluate(records)

    if len(output_rows) != 162:
        raise ValueError(
            "Unexpected sensitivity comparison count"
        )

    candidate_summary = summarize(
        output_rows
    )

    status_counts = dict(
        sorted(
            Counter(
                row["sensitivity_status"]
                for row in output_rows
            ).items()
        )
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",

        "research_decision": "HOLD",

        "approved_for_live_trading": False,
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,

        "source_version": source_result["version"],
        "source_rows": source_result["source_rows"],

        "source_comparison_rows": len(records),
        "comparison_rows": len(output_rows),

        "original_candidates": 7,

        "design_effects": list(DESIGN_EFFECTS),

        "candidate_summary": candidate_summary,

        "status_counts": status_counts,

        "limitations": [
            "Design effects are hypothetical scenarios.",
            "Actual event overlap is not measured.",
            "Effective sample sizes are assumptions.",
            "Wilson intervals remain exploratory.",
            "Original candidate selection used the same data.",
            "No multiplicity-adjusted significance claim.",
            "No independent event-level bootstrap.",
            "No prospective independent OOS proof.",
            "No trading cost simulation.",
            "No profitability proof.",
            "No automated trading.",
        ],

        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_RESULT
    manifest_path = output_dir / OUTPUT_MANIFEST

    save_csv(
        csv_path,
        output_rows,
    )

    save_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,

        "config": {
            "design_effects": list(DESIGN_EFFECTS),
            "z": Z,
            "method": (
                "Wilson sensitivity with "
                "hypothetical effective sample size"
            ),
        },

        "input_sha256": input_hashes,

        "output_sha256": {
            OUTPUT_CSV: sha256_file(csv_path),
            OUTPUT_RESULT: sha256_file(result_path),
        },
    }

    save_json(
        manifest_path,
        manifest,
    )

    if not validate_existing(
        output_dir,
        input_hashes,
    ):
        raise RuntimeError(
            "Final integrity verification failed"
        )

    print("=" * 60)
    print("PATTERN 256 OVERLAP SENSITIVITY AUDIT")
    print("=" * 60)

    print("Execution: PASS")
    print(f"Version: {VERSION}")
    print(f"Source rows: {source_result['source_rows']}")
    print(f"Comparison rows: {len(output_rows)}")
    print(f"Original candidates: {len(candidate_summary)}")

    print()

    for effect in DESIGN_EFFECTS:
        retained = sum(
            1
            for item in candidate_summary
            if effect in item["positive_design_effects"]
        )

        print(
            f"Design effect {effect}: "
            f"{retained}/7 exploratory positive"
        )

    print()
    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")
    print("Research decision: HOLD")
    print("Statistical independence proven: False")
    print("Profitability proven: False")
    print("Live trading approved: False")

    print("=" * 60)

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
