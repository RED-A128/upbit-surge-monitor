# -*- coding: utf-8 -*-
"""
audit_pattern_256_formula_risk.py
Clean V001

Purpose:
    Independently inspect Pattern 256 calculation source code
    for possible future-data references.

Principles:
    - Read-only access to existing research data.
    - Never modify original OHLCV, features or signals.
    - Verify the previous full-market replay evidence.
    - Verify SHA256 of the calculation scripts.
    - Report suspicious code locations.
    - Never approve TRAIN or live trading automatically.
    - Keep research decision at HOLD.

Run:
    py audit_pattern_256_formula_risk.py --root .
"""

import argparse
import ast
import csv
import hashlib
import json
import re
import sys

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_FILES = (
    "build_features.py",
    "detect_256.py",
)

REPLAY_FILES = (
    "full_market_checks.csv",
    "full_market_exclusions.csv",
    "full_market_issues.csv",
    "full_market_result.json",
)

REPLAY_RELATIVE_DIR = (
    Path("data")
    / "reports"
    / "pattern_256_prefix_replay_full_market"
)

OUTPUT_RELATIVE_DIR = (
    Path("data")
    / "reports"
    / "pattern_256_formula_risk"
)

RISK_COLUMNS = (
    "file",
    "line",
    "severity",
    "rule",
    "expression",
    "description",
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def write_json(path, data):
    with path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

        handle.write("\n")


def read_csv_rows(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return list(csv.DictReader(handle))


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_replay(root):
    report_dir = root / REPLAY_RELATIVE_DIR

    manifest_path = (
        report_dir / "full_market_manifest.json"
    )

    result_path = (
        report_dir / "full_market_result.json"
    )

    require(
        manifest_path.is_file(),
        f"Missing manifest: {manifest_path}",
    )

    require(
        result_path.is_file(),
        f"Missing result: {result_path}",
    )

    manifest = load_json(manifest_path)
    result = load_json(result_path)

    require(
        manifest.get("execution") == "PASS",
        "Previous manifest is not PASS",
    )

    require(
        result.get("execution") == "PASS",
        "Previous replay is not PASS",
    )

    require(
        result.get(
            "full_eligible_market_prefix_replay_verified"
        ) is True,
        "Full eligible-market replay is not verified",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected previous research decision",
    )

    require(
        result.get("train_approved") is False,
        "Unexpected previous TRAIN approval",
    )

    require(
        result.get("live_trading_approved") is False,
        "Unexpected previous trading approval",
    )

    expected = int(result["expected_checks"])
    completed = int(result["completed_checks"])
    passed = int(result["passed_checks"])
    failed = int(result["failed_checks"])
    issues = int(result["issue_count"])

    require(
        expected > 0,
        "Expected checks must be positive",
    )

    require(
        expected == completed == passed,
        "Replay check counts do not match",
    )

    require(
        failed == 0 and issues == 0,
        "Previous replay contains failures",
    )

    require(
        int(result["invalid_exclusion_count"]) == 0,
        "Previous replay has invalid exclusions",
    )

    output_hashes = manifest.get(
        "output_sha256",
        {},
    )

    require(
        isinstance(output_hashes, dict),
        "Invalid output SHA256 manifest",
    )

    for filename in REPLAY_FILES:
        require(
            filename in output_hashes,
            f"Missing output hash: {filename}",
        )

        path = report_dir / filename

        require(
            path.is_file(),
            f"Missing replay file: {path}",
        )

        actual = sha256_file(path)
        expected_hash = str(
            output_hashes[filename]
        ).lower()

        require(
            actual == expected_hash,
            f"SHA256 mismatch: {filename}",
        )

    checks = read_csv_rows(
        report_dir / "full_market_checks.csv"
    )

    exclusions = read_csv_rows(
        report_dir / "full_market_exclusions.csv"
    )

    issue_rows = read_csv_rows(
        report_dir / "full_market_issues.csv"
    )

    require(
        len(checks) == expected,
        "Check CSV row count mismatch",
    )

    require(
        len(issue_rows) == 0,
        "Issue CSV is not empty",
    )

    require(
        len(exclusions)
        == int(result["excluded_market_timeframe_count"]),
        "Exclusion CSV row count mismatch",
    )

    for index, row in enumerate(checks, start=1):
        require(
            row.get("status") == "PASS",
            f"Check row {index} is not PASS",
        )

        require(
            int(row["feature_mismatches"]) == 0,
            f"Feature mismatch at row {index}",
        )

        require(
            int(row["signal_mismatches"]) == 0,
            f"Signal mismatch at row {index}",
        )

    script_hashes = manifest.get(
        "script_sha256",
        {},
    )

    require(
        isinstance(script_hashes, dict),
        "Invalid script hash manifest",
    )

    mapping = {
        "build_features.py": "features",
        "detect_256.py": "detector",
    }

    verified_source_hashes = {}

    for filename, manifest_key in mapping.items():
        path = root / filename

        require(
            path.is_file(),
            f"Missing source script: {path}",
        )

        actual = sha256_file(path)

        expected_hash = str(
            script_hashes.get(manifest_key, "")
        ).lower()

        require(
            len(expected_hash) == 64,
            f"Missing script SHA256: {manifest_key}",
        )

        require(
            actual == expected_hash,
            (
                f"Source script changed since replay: "
                f"{filename}"
            ),
        )

        verified_source_hashes[filename] = actual

    return {
        "expected_checks": expected,
        "passed_checks": passed,
        "excluded_jobs": len(exclusions),
        "source_sha256": verified_source_hashes,
        "manifest_sha256": sha256_file(
            manifest_path
        ),
    }


def expression_text(node):
    try:
        return ast.unparse(node)
    except Exception:
        return type(node).__name__


def is_negative_number(node):
    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.USub):
            if isinstance(
                node.operand,
                ast.Constant,
            ):
                value = node.operand.value

                return (
                    isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and value > 0
                )

    if isinstance(node, ast.Constant):
        value = node.value

        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and value < 0
        )

    return False


def call_name(node):
    if isinstance(node, ast.Attribute):
        return node.attr

    if isinstance(node, ast.Name):
        return node.id

    return ""


def add_risk(
    risks,
    filename,
    node,
    severity,
    rule,
    description,
):
    risks.append({
        "file": filename,
        "line": int(
            getattr(node, "lineno", 0)
        ),
        "severity": severity,
        "rule": rule,
        "expression": expression_text(node),
        "description": description,
    })


def scan_ast(filename, source):
    tree = ast.parse(
        source,
        filename=filename,
    )

    risks = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        name = call_name(node.func)

        if name in ("shift", "diff", "pct_change"):
            if node.args:
                if is_negative_number(node.args[0]):
                    add_risk(
                        risks,
                        filename,
                        node,
                        "HIGH",
                        "NEGATIVE_LOOKBACK",
                        (
                            "Negative periods may reference "
                            "future observations."
                        ),
                    )

            for keyword in node.keywords:
                if keyword.arg == "periods":
                    if is_negative_number(
                        keyword.value
                    ):
                        add_risk(
                            risks,
                            filename,
                            node,
                            "HIGH",
                            "NEGATIVE_LOOKBACK",
                            (
                                "Negative periods may reference "
                                "future observations."
                            ),
                        )

        if name == "rolling":
            for keyword in node.keywords:
                if keyword.arg != "center":
                    continue

                if (
                    isinstance(
                        keyword.value,
                        ast.Constant,
                    )
                    and keyword.value.value is True
                ):
                    add_risk(
                        risks,
                        filename,
                        node,
                        "HIGH",
                        "CENTERED_ROLLING",
                        (
                            "Centered rolling windows may "
                            "include future observations."
                        ),
                    )

        if name in (
            "bfill",
            "backfill",
        ):
            add_risk(
                risks,
                filename,
                node,
                "REVIEW",
                "BACKWARD_FILL",
                (
                    "Backward filling may propagate "
                    "future values into earlier rows."
                ),
            )

        if name == "fillna":
            for keyword in node.keywords:
                if keyword.arg == "method":
                    value = keyword.value

                    if (
                        isinstance(value, ast.Constant)
                        and value.value in (
                            "bfill",
                            "backfill",
                        )
                    ):
                        add_risk(
                            risks,
                            filename,
                            node,
                            "REVIEW",
                            "BACKWARD_FILL",
                            (
                                "Backward filling may "
                                "use future observations."
                            ),
                        )

    return risks


def scan_text(filename, source):
    risks = []

    patterns = (
        (
            "FUTURE_TERMINOLOGY",
            r"\b(?:future|lookahead|look_ahead)\b",
            "REVIEW",
            (
                "Future-related terminology requires "
                "contextual review."
            ),
        ),
        (
            "LEAD_TERMINOLOGY",
            r"\b(?:lead|forward_return)\b",
            "REVIEW",
            (
                "Forward-looking variable names require "
                "contextual review."
            ),
        ),
    )

    for line_number, line in enumerate(
        source.splitlines(),
        start=1,
    ):
        for rule, pattern, severity, description in patterns:
            if not re.search(
                pattern,
                line,
                flags=re.IGNORECASE,
            ):
                continue

            risks.append({
                "file": filename,
                "line": line_number,
                "severity": severity,
                "rule": rule,
                "expression": line.strip()[:500],
                "description": description,
            })

    return risks


def scan_sources(root):
    all_risks = []
    source_details = []

    for filename in SOURCE_FILES:
        path = root / filename

        source = path.read_text(
            encoding="utf-8-sig"
        )

        ast_risks = scan_ast(
            filename,
            source,
        )

        text_risks = scan_text(
            filename,
            source,
        )

        all_risks.extend(ast_risks)
        all_risks.extend(text_risks)

        source_details.append({
            "file": filename,
            "lines": len(
                source.splitlines()
            ),
            "ast_risks": len(ast_risks),
            "text_review_items": len(text_risks),
        })

    all_risks.sort(
        key=lambda item: (
            item["file"],
            item["line"],
            item["rule"],
        )
    )

    return all_risks, source_details


def write_risk_csv(path, risks):
    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=RISK_COLUMNS,
        )

        writer.writeheader()
        writer.writerows(risks)


def run(root):
    root = root.resolve()

    print("=" * 64)
    print("PATTERN 256 FORMULA RISK AUDIT")
    print(f"Version: {VERSION}")
    print(f"Root: {root}")
    print("=" * 64)

    replay_evidence = verify_replay(root)

    print(
        "[PASS] Previous full-market replay "
        "evidence verified"
    )

    print(
        "[PASS] Source script SHA256 "
        "matches replay manifest"
    )

    risks, source_details = scan_sources(root)

    high_count = sum(
        item["severity"] == "HIGH"
        for item in risks
    )

    review_count = sum(
        item["severity"] == "REVIEW"
        for item in risks
    )

    output_dir = root / OUTPUT_RELATIVE_DIR

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    risks_path = (
        output_dir / "formula_risks.csv"
    )

    result_path = (
        output_dir / "formula_risk_result.json"
    )

    manifest_path = (
        output_dir / "formula_risk_manifest.json"
    )

    write_risk_csv(
        risks_path,
        risks,
    )

    result = {
        "audit": "pattern_256_formula_risk",
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": "PASS",
        "replay_evidence_verified": True,
        "source_sha256_verified": True,
        "replay_evidence": replay_evidence,
        "source_details": source_details,
        "total_risk_items": len(risks),
        "high_risk_items": high_count,
        "review_items": review_count,
        "static_review_complete": True,
        "manual_review_required": True,
        "upstream_signal_formula_leakage_free_proven": False,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            (
                "Static analysis detects selected patterns "
                "but cannot prove absence of future leakage."
            ),
            (
                "Detected patterns may be harmless depending "
                "on their execution context."
            ),
            (
                "Indirect function calls, aliases and "
                "dynamic expressions may evade detection."
            ),
            (
                "Manual source review and independent "
                "temporal validation remain required."
            ),
        ],
    }

    write_json(
        result_path,
        result,
    )

    manifest = {
        "audit": "pattern_256_formula_risk",
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": "PASS",
        "output_sha256": {
            "formula_risks.csv": sha256_file(
                risks_path
            ),
            "formula_risk_result.json": sha256_file(
                result_path
            ),
        },
        "source_sha256": replay_evidence[
            "source_sha256"
        ],
    }

    write_json(
        manifest_path,
        manifest,
    )

    print()
    print("===== RESULTS =====")
    print(
        "Replay checks verified:",
        replay_evidence["passed_checks"],
    )
    print(
        "High-risk patterns:",
        high_count,
    )
    print(
        "Review items:",
        review_count,
    )
    print(
        "Total findings:",
        len(risks),
    )
    print(
        "Research decision: HOLD"
    )
    print(
        "TRAIN approved: False"
    )
    print(
        "Live trading approved: False"
    )

    print()
    print("===== OUTPUT =====")
    print(risks_path)
    print(result_path)
    print(manifest_path)

    print()
    print(
        "AUDIT EXECUTION PASS "
        "- MANUAL REVIEW REQUIRED"
    )

    return 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 independent "
            "formula-risk inspection"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
        help="Persistent local research root",
    )

    args = parser.parse_args()

    try:
        return run(
            Path(args.root)
        )

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
