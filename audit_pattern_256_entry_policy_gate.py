
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Entry Policy Gate
Clean V001

Purpose:
- Verify previous policy-review and boundary-replay evidence.
- Reconcile the two known boundary cases.
- Distinguish historical reproduction from execution policy.
- Produce a review report without changing source data.

No automatic policy approval.
No automatic trading.
No training approval.
"""

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_entry_policy_gate"

REVIEW_DIR = Path(
    "data/reports/pattern_256_evaluator_policy_review"
)

REPLAY_DIR = Path(
    "data/reports/pattern_256_boundary_replay"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_entry_policy_gate"
)

EXPECTED_CASES = {
    ("d1", "KRW-AWE", 24): {
        "include_missing": 0,
        "exclude_missing": 1,
        "include_rows": 24,
        "exclude_rows": 23,
        "recorded_status": "MISS",
    },
    ("h1", "KRW-0G", 168): {
        "include_missing": 0,
        "exclude_missing": 1,
        "include_rows": 168,
        "exclude_rows": 168,
        "recorded_status": "HIT",
    },
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    require(
        path.is_file(),
        f"Required file missing: {path}",
    )

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, data):
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            data,
            handle,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
        handle.write("\n")


def verify_manifest(folder, manifest_name):
    manifest_path = folder / manifest_name
    manifest = load_json(manifest_path)

    outputs = manifest.get("outputs")

    require(
        isinstance(outputs, dict) and outputs,
        "Invalid manifest outputs",
    )

    verified = {}

    for name, expected in sorted(outputs.items()):
        require(
            Path(name).name == name,
            f"Unsafe manifest filename: {name}",
        )

        source = folder / name

        require(
            source.is_file(),
            f"Manifest output missing: {source}",
        )

        actual = sha256(source)

        require(
            actual.lower() == str(expected).lower(),
            f"SHA256 mismatch: {source}",
        )

        verified[name] = actual

    return {
        "manifest": str(manifest_path),
        "manifest_sha256": sha256(manifest_path),
        "outputs": verified,
    }


def parse_bool(value):
    if isinstance(value, bool):
        return value

    value = str(value).strip().lower()

    if value == "true":
        return True

    if value == "false":
        return False

    raise ValueError(f"Invalid boolean: {value}")


def load_windows(path):
    require(
        path.is_file(),
        f"Windows CSV missing: {path}",
    )

    result = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        required = {
            "timeframe",
            "market",
            "convention",
            "horizon",
            "observed_rows",
            "missing_slots",
            "complete_by_rows",
            "continuous_by_time",
            "source_sha256",
        }

        require(
            reader.fieldnames is not None
            and required.issubset(
                set(reader.fieldnames)
            ),
            "Missing windows CSV columns",
        )

        for row in reader:
            key = (
                row["timeframe"],
                row["market"],
                int(row["horizon"]),
                row["convention"],
            )

            require(
                key not in result,
                f"Duplicate window: {key}",
            )

            result[key] = {
                "observed_rows": int(
                    row["observed_rows"]
                ),
                "missing_slots": int(
                    row["missing_slots"]
                ),
                "complete_by_rows": parse_bool(
                    row["complete_by_rows"]
                ),
                "continuous_by_time": parse_bool(
                    row["continuous_by_time"]
                ),
                "source_sha256": row[
                    "source_sha256"
                ],
            }

    return result


def validate_review(review):
    require(
        review.get("execution") == "PASS",
        "Policy review did not PASS",
    )

    require(
        review.get("classification")
        == "MANUAL_REVIEW_REQUIRED",
        "Unexpected review classification",
    )

    require(
        review.get("research_decision") == "HOLD",
        "Unexpected research decision",
    )

    require(
        review.get("source_policy_confirmed")
        is False,
        "Unexpected source policy approval",
    )

    require(
        review.get("hash_mismatches") == 0,
        "Source hash mismatches",
    )

    require(
        review.get("attention_cases") == 649,
        "Unexpected attention case count",
    )

    require(
        review.get("boundary_cases") == 2,
        "Unexpected boundary case count",
    )


def validate_replay(replay):
    require(
        replay.get("execution") == "PASS",
        "Boundary replay did not PASS",
    )

    require(
        replay.get("classification")
        == "BOUNDARY_REPLAY_MATCH",
        "Boundary replay mismatch",
    )

    require(
        replay.get("gap_counts_agree_with_previous")
        is True,
        "Gap counts disagree",
    )

    require(
        replay.get("boundary_cases") == 2,
        "Unexpected replay case count",
    )

    require(
        replay.get("window_checks") == 4,
        "Unexpected window count",
    )

    require(
        replay.get("research_decision") == "HOLD",
        "Unexpected replay research decision",
    )


def validate_cases(review, replay, windows):
    review_cases = {
        (
            item["timeframe"],
            item["market"],
            int(item["horizon"]),
        ): item
        for item in review["boundary_details"]
    }

    replay_cases = {
        (
            item["timeframe"],
            item["market"],
            int(item["horizon"]),
        ): item
        for item in replay["cases"]
    }

    require(
        set(review_cases) == set(EXPECTED_CASES),
        "Unexpected policy review cases",
    )

    require(
        set(replay_cases) == set(EXPECTED_CASES),
        "Unexpected boundary replay cases",
    )

    require(
        len(windows) == 4,
        "Unexpected windows CSV row count",
    )

    output = []

    for key, expected in EXPECTED_CASES.items():
        review_case = review_cases[key]
        replay_case = replay_cases[key]

        require(
            review_case["recorded_status"]
            == expected["recorded_status"],
            f"Recorded status changed: {key}",
        )

        require(
            replay_case["recorded_status"]
            == expected["recorded_status"],
            f"Replay status changed: {key}",
        )

        require(
            review_case["signal_timestamp"]
            == replay_case["signal_timestamp"],
            f"Signal timestamp changed: {key}",
        )

        require(
            review_case["entry_timestamp"]
            == replay_case["entry_timestamp"],
            f"Entry timestamp changed: {key}",
        )

        include_key = key + ("INCLUDE_ENTRY",)
        exclude_key = key + ("EXCLUDE_ENTRY",)

        require(
            include_key in windows,
            f"Include window missing: {key}",
        )

        require(
            exclude_key in windows,
            f"Exclude window missing: {key}",
        )

        include = windows[include_key]
        exclude = windows[exclude_key]

        require(
            include["missing_slots"]
            == expected["include_missing"],
            f"Include gap mismatch: {key}",
        )

        require(
            exclude["missing_slots"]
            == expected["exclude_missing"],
            f"Exclude gap mismatch: {key}",
        )

        require(
            include["observed_rows"]
            == expected["include_rows"],
            f"Include row mismatch: {key}",
        )

        require(
            exclude["observed_rows"]
            == expected["exclude_rows"],
            f"Exclude row mismatch: {key}",
        )

        require(
            include["continuous_by_time"] is True,
            f"Include continuity failed: {key}",
        )

        require(
            exclude["continuous_by_time"] is False,
            f"Exclude continuity unexpectedly passed: {key}",
        )

        require(
            include["source_sha256"]
            == replay_case["source_sha256"],
            f"Include source hash mismatch: {key}",
        )

        require(
            exclude["source_sha256"]
            == replay_case["source_sha256"],
            f"Exclude source hash mismatch: {key}",
        )

        require(
            replay_case[
                "include_gap_count_matches_previous"
            ] is True,
            f"Include comparison failed: {key}",
        )

        require(
            replay_case[
                "exclude_gap_count_matches_previous"
            ] is True,
            f"Exclude comparison failed: {key}",
        )

        output.append({
            "timeframe": key[0],
            "market": key[1],
            "horizon": key[2],
            "recorded_status": expected[
                "recorded_status"
            ],
            "include": include,
            "exclude": exclude,
            "historical_window_reproduction": "MATCH",
            "execution_policy_confirmed": False,
        })

    return output


def build_policy_assessment():
    return {
        "historical_reproduction": {
            "entry_convention": "INCLUDE_ENTRY",
            "interpretation": (
                "The observed boundary cases reproduce "
                "the recorded evaluation windows when "
                "the entry candle is included."
            ),
            "status": "REPRODUCED_FOR_TWO_CASES",
        },
        "execution_assumptions": {
            "candidate_entry": (
                "Next available candle open after "
                "the signal candle"
            ),
            "entry_candle_high_low": (
                "May be used only as an OHLCV-based "
                "assumption after an executable "
                "entry at that candle open."
            ),
            "same_candle_target_stop_order": (
                "UNKNOWN_WITH_OHLCV_ONLY"
            ),
            "slippage_fees_liquidity": (
                "NOT_VERIFIED"
            ),
            "causal_signal_availability": (
                "NOT_VERIFIED"
            ),
        },
        "gap_policy": {
            "historical_observation": (
                "The original evaluator uses available "
                "OHLCV rows as the horizon and records "
                "time gaps separately."
            ),
            "unresolved_choice": (
                "Whether missing calendar candle slots "
                "must invalidate a complete-by-rows "
                "outcome for OOS evaluation."
            ),
            "recommendation": (
                "For conservative OOS reporting, "
                "separate continuous windows from "
                "windows with missing candle slots."
            ),
        },
        "manual_decisions_required": [
            "Confirm executable entry timing",
            "Confirm whether entry candle is included",
            "Confirm missing-candle exclusion policy",
            "Confirm incomplete HIT/MISS/CENSORED policy",
            "Confirm target/stop ambiguity handling",
            "Confirm fees, slippage and liquidity assumptions",
        ],
    }


def write_markdown(path, result):
    lines = [
        "# Pattern 256 Entry Policy Gate",
        "",
        f"- Version: {VERSION}",
        "- Execution: PASS",
        "- Historical boundary reproduction: MATCH",
        "- Execution policy: NOT CONFIRMED",
        "- Independent OOS: NOT PROVEN",
        "- Research decision: HOLD",
        "",
        "## Boundary cases",
        "",
        "| Timeframe | Market | Horizon | "
        "Include missing | Exclude missing |",
        "|---|---|---:|---:|---:|",
    ]

    for case in result["cases"]:
        lines.append(
            f"| {case['timeframe']} "
            f"| {case['market']} "
            f"| {case['horizon']} "
            f"| {case['include']['missing_slots']} "
            f"| {case['exclude']['missing_slots']} |"
        )

    lines.extend([
        "",
        "## Interpretation",
        "",
        "The two historical boundary cases are reproduced.",
        "This is not proof that the original entry",
        "convention is executable without look-ahead bias.",
        "",
        "The next-bar open is a possible simulation",
        "assumption, not evidence of actual fills.",
        "",
        "## Manual decisions required",
        "",
    ])

    for item in result[
        "policy_assessment"
    ]["manual_decisions_required"]:
        lines.append(f"- {item}")

    lines.extend([
        "",
        "## Restrictions",
        "",
        "- No original input files were modified.",
        "- No independent OOS performance was established.",
        "- Training remains unapproved.",
        "- Live trading remains unapproved.",
        "- Research decision remains HOLD.",
        "",
    ])

    path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
        help="Persistent local research project root",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()

    review_folder = root / REVIEW_DIR
    replay_folder = root / REPLAY_DIR

    review_integrity = verify_manifest(
        review_folder,
        "policy_review_manifest.json",
    )

    replay_integrity = verify_manifest(
        replay_folder,
        "boundary_replay_manifest.json",
    )

    review = load_json(
        review_folder / "policy_review_result.json"
    )

    replay = load_json(
        replay_folder / "boundary_replay_result.json"
    )

    windows = load_windows(
        replay_folder / "boundary_replay_windows.csv"
    )

    validate_review(review)
    validate_replay(replay)

    cases = validate_cases(
        review,
        replay,
        windows,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": (
            "HISTORICAL_REPRODUCTION_CONFIRMED_"
            "POLICY_PENDING"
        ),
        "cases_checked": len(cases),
        "window_checks": len(windows),
        "historical_reproduction_confirmed": True,
        "source_policy_confirmed": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "next_gate": "MANUAL_ENTRY_GAP_POLICY_DECISION",
        "input_integrity": {
            "policy_review": review_integrity,
            "boundary_replay": replay_integrity,
        },
        "cases": cases,
        "policy_assessment": build_policy_assessment(),
    }

    folder = root / OUTPUT_DIR
    folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = folder / "entry_policy_gate_result.json"
    report_path = folder / "entry_policy_gate_report.md"
    manifest_path = folder / "entry_policy_gate_manifest.json"

    write_json(result_path, result)
    write_markdown(report_path, result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            result_path.name: sha256(result_path),
            report_path.name: sha256(report_path),
        },
    }

    write_json(manifest_path, manifest)

    print("=" * 64)
    print("Pattern 256 Entry Policy Gate")
    print("Execution: PASS")
    print("Historical reproduction: CONFIRMED")
    print("Cases checked:", len(cases))
    print("Window checks:", len(windows))
    print("Execution policy: PENDING")
    print("Independent OOS: NOT PROVEN")
    print("Research decision: HOLD")
    print("Output:", folder)
    print("=" * 64)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"[ERROR] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
