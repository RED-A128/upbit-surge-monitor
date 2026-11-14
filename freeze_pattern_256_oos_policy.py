
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 OOS Policy Freeze
Clean V001

Create a conservative, versioned research policy.

This program:
- Verifies the previous entry policy gate and SHA256 manifest.
- Produces an immutable-by-content OOS policy specification.
- Does not modify source OHLCV, features, signals, or outcomes.
- Does not run an OOS backtest.
- Does not approve training or trading.
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_oos_policy_freeze"

INPUT_RELATIVE = Path(
    "data/reports/pattern_256_entry_policy_gate"
)

OUTPUT_RELATIVE = Path(
    "data/reports/pattern_256_oos_policy_freeze"
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def canonical_json_bytes(data):
    return json.dumps(
        data,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def save_json(path, data):
    path.write_text(
        json.dumps(
            data,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def load_json(path):
    require(
        path.is_file(),
        f"Missing file: {path}",
    )

    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def verify_input(folder):
    manifest_path = (
        folder / "entry_policy_gate_manifest.json"
    )

    result_path = (
        folder / "entry_policy_gate_result.json"
    )

    report_path = (
        folder / "entry_policy_gate_report.md"
    )

    manifest = load_json(manifest_path)
    result = load_json(result_path)

    require(
        manifest.get("audit")
        == "pattern_256_entry_policy_gate",
        "Unexpected input audit",
    )

    require(
        manifest.get("version") == "Clean V001",
        "Unexpected input manifest version",
    )

    expected_files = {
        "entry_policy_gate_result.json",
        "entry_policy_gate_report.md",
    }

    outputs = manifest.get("outputs")

    require(
        isinstance(outputs, dict)
        and set(outputs) == expected_files,
        "Unexpected input manifest outputs",
    )

    for name in sorted(expected_files):
        path = folder / name

        require(
            path.is_file(),
            f"Missing input: {path}",
        )

        actual = file_sha256(path)

        require(
            actual.lower()
            == str(outputs[name]).lower(),
            f"Input SHA256 mismatch: {name}",
        )

    require(
        result.get("audit")
        == "pattern_256_entry_policy_gate",
        "Unexpected gate audit",
    )

    require(
        result.get("version") == "Clean V001",
        "Unexpected gate version",
    )

    require(
        result.get("execution") == "PASS",
        "Previous gate did not pass",
    )

    require(
        result.get(
            "historical_reproduction_confirmed"
        ) is True,
        "Historical reproduction not confirmed",
    )

    require(
        result.get("cases_checked") == 2,
        "Unexpected case count",
    )

    require(
        result.get("window_checks") == 4,
        "Unexpected window count",
    )

    require(
        result.get("source_policy_confirmed")
        is False,
        "Unexpected source policy state",
    )

    require(
        result.get("independent_oos_proven")
        is False,
        "Unexpected OOS state",
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
        result.get("live_trading_approved")
        is False,
        "Unexpected trading approval",
    )

    require(
        len(result.get("cases", [])) == 2,
        "Invalid boundary cases",
    )

    return {
        "manifest_sha256": file_sha256(
            manifest_path
        ),
        "result_sha256": file_sha256(
            result_path
        ),
        "report_sha256": file_sha256(
            report_path
        ),
    }


def make_policy():
    return {
        "policy_id": "pattern_256_oos_conservative_v001",
        "version": VERSION,
        "status": "PROVISIONAL_RESEARCH_POLICY",
        "scope": {
            "exchange": "UPBIT",
            "quote_currency": "KRW",
            "timeframes": ["h1", "h4", "d1"],
            "strategy": "PATTERN_256",
            "target_return_pct": 30.0,
            "automatic_trading": False,
        },
        "signal": {
            "decision_time": "AFTER_SIGNAL_CANDLE_CLOSE",
            "future_candle_access": "FORBIDDEN",
            "prefix_replay_required": True,
            "signal_formula_audit_required": True,
            "historical_point_in_time_universe_required": True,
        },
        "entry": {
            "model": "NEXT_CANDLE_OPEN",
            "include_entry_candle_in_horizon": True,
            "entry_price_source": "NEXT_CANDLE_OPEN",
            "fill_status": "ASSUMED_NOT_VERIFIED",
            "missing_next_candle": "EXCLUDE_NO_ENTRY",
            "entry_price_nonpositive": "EXCLUDE_INVALID",
        },
        "horizon": {
            "unit": "CANDLES",
            "configured_values": [24, 72, 168],
            "count_from": "ENTRY_CANDLE",
            "require_utc_grid_continuity": True,
            "gap_status": "EXCLUDE_WINDOW_GAP",
            "partial_window_status": "CENSORED",
            "censored_as_miss": False,
        },
        "outcome": {
            "target_pct": 30.0,
            "target_reference": "ENTRY_PRICE",
            "target_touch": "HIGH_GREATER_OR_EQUAL",
            "target_touch_label": "TARGET_TOUCHED",
            "target_touch_is_realized_profit": False,
            "no_touch_complete_window": "MISS",
            "no_touch_incomplete_window": "CENSORED",
            "target_touch_in_incomplete_window": (
                "OBSERVED_TOUCH_SEPARATE_FROM_COMPLETE_COHORT"
            ),
            "same_candle_target_stop_sequence": (
                "UNKNOWN_WITH_OHLCV"
            ),
        },
        "costs_and_execution": {
            "fees_mode": "NOT_YET_MODELED",
            "slippage_mode": "NOT_YET_MODELED",
            "liquidity_mode": "NOT_YET_MODELED",
            "market_impact_mode": "NOT_YET_MODELED",
            "net_profit_claim_allowed": False,
        },
        "oos": {
            "split": "CHRONOLOGICAL",
            "random_row_split_allowed": False,
            "split_dates": "TO_BE_DETERMINED_FROM_DATA",
            "training_before_test": True,
            "overlapping_label_purge_required": True,
            "embargo_required": True,
            "purge_embargo_parameters": (
                "TO_BE_DEFINED_BEFORE_EVALUATION"
            ),
            "hyperparameter_tuning_on_test": False,
            "final_test_reuse_for_tuning": False,
            "point_in_time_listing_checks_required": True,
            "delisted_market_bias_review_required": True,
        },
        "reporting": {
            "denominator": (
                "ELIGIBLE_COMPLETE_CONTINUOUS_WINDOWS"
            ),
            "excluded_cases_reported_separately": True,
            "required_counts": [
                "all_signals",
                "eligible",
                "window_gap",
                "censored",
                "invalid_entry",
                "target_touched",
                "miss",
            ],
            "required_metrics": [
                "target_touch_rate",
                "base_rate_comparison",
                "confidence_interval",
                "per_timeframe_results",
                "per_market_results",
                "chronological_test_results",
                "drawdown_distribution",
                "data_coverage",
            ],
            "profitability_approved": False,
        },
        "research_controls": {
            "source_files_read_only": True,
            "checkpoint_resume_required": True,
            "sha256_manifest_required": True,
            "independent_oos_proven": False,
            "training_approved": False,
            "live_trading_approved": False,
            "research_decision": "HOLD",
        },
    }


def make_report(policy, policy_sha, evidence):
    lines = [
        "# Pattern 256 OOS Policy Freeze",
        "",
        f"- Version: {VERSION}",
        "- Execution: PASS",
        "- Policy status: PROVISIONAL_RESEARCH_POLICY",
        "- Research decision: HOLD",
        f"- Policy SHA256: `{policy_sha}`",
        "",
        "## Evidence",
        "",
        "Previous entry policy gate passed.",
        "Two historical boundary cases were reproduced.",
        "This does not establish a valid execution model.",
        "",
        "## Proposed evaluation rules",
        "",
        "- Signals are available only after candle close.",
        "- Entry is modeled at the next candle open.",
        "- The entry candle is included in the horizon.",
        "- A missing entry candle invalidates entry.",
        "- Gapped windows are excluded and counted.",
        "- Incomplete windows are censored, not misses.",
        "- A 30% high-price touch is not realized profit.",
        "- Costs and actual fills are not yet verified.",
        "",
        "## Independent OOS requirements",
        "",
        "- Chronological separation.",
        "- No test-period parameter tuning.",
        "- Purging overlapping label windows.",
        "- Embargo before the final test.",
        "- Point-in-time market availability review.",
        "- Baseline and confidence interval comparison.",
        "- Separate excluded and censored counts.",
        "",
        "## Remaining limitations",
        "",
        "- No independent OOS backtest was run.",
        "- Source evaluator behavior was not changed.",
        "- Execution assumptions remain unverified.",
        "- Training and live trading are not approved.",
        "",
        "## Input hashes",
        "",
    ]

    for name, digest in sorted(evidence.items()):
        lines.append(f"- {name}: `{digest}`")

    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    input_folder = root / INPUT_RELATIVE
    output_folder = root / OUTPUT_RELATIVE

    evidence = verify_input(input_folder)
    policy = make_policy()

    policy_sha = hashlib.sha256(
        canonical_json_bytes(policy)
    ).hexdigest()

    output_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    policy_path = (
        output_folder / "oos_policy.json"
    )

    result_path = (
        output_folder / "oos_policy_freeze_result.json"
    )

    report_path = (
        output_folder / "oos_policy_freeze_report.md"
    )

    manifest_path = (
        output_folder / "oos_policy_freeze_manifest.json"
    )

    # Fail closed if an existing policy differs.
    # Never silently replace a previously frozen policy.
    if policy_path.exists():
        previous = load_json(policy_path)

        require(
            canonical_json_bytes(previous)
            == canonical_json_bytes(policy),
            "Existing frozen policy differs. "
            "Review it manually before creating "
            "a new policy version.",
        )
    else:
        save_json(policy_path, policy)

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": (
            "PROVISIONAL_OOS_POLICY_FROZEN"
        ),
        "policy_id": policy["policy_id"],
        "policy_sha256": policy_sha,
        "previous_gate_verified": True,
        "previous_gate_evidence": evidence,
        "historical_reproduction_confirmed": True,
        "execution_policy_confirmed": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "next_gate": (
            "IMPLEMENT_AND_RUN_CHRONOLOGICAL_OOS"
        ),
    }

    save_json(result_path, result)

    report_path.write_text(
        make_report(policy, policy_sha, evidence),
        encoding="utf-8",
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            path.name: file_sha256(path)
            for path in (
                policy_path,
                result_path,
                report_path,
            )
        },
    }

    save_json(manifest_path, manifest)

    print("=" * 64)
    print("Pattern 256 OOS Policy Freeze")
    print("Execution: PASS")
    print("Policy status: PROVISIONAL")
    print("Policy SHA256:", policy_sha)
    print("Independent OOS: NOT PROVEN")
    print("Research decision: HOLD")
    print("Output:", output_folder)
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
