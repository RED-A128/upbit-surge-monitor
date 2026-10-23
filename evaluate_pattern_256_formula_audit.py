
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upbit Surge Monitor - Clean V003-21

Read-only 256 signal formula and lookahead audit.

Inputs:
    Clean V003-20 result and manifest
    build_features.py
    detect_256.py
    validate_256.py

Outputs:
    formula_audit_result.json
    formula_audit_findings.csv
    formula_audit_functions.csv
    formula_audit_manifest.json

This is a static audit, not a mathematical proof.
No source data is changed.
No signals or orders are generated.
"""

import argparse
import ast
import csv
import hashlib
import io
import json
import os
import re
import sys
import tempfile

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V003-21"
PREVIOUS_VERSION = "Clean V003-20"

GATE_DIR = Path(
    "data/reports/pattern_256_signal_integrity_gate"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_formula_audit"
)

SOURCE_FILES = (
    "build_features.py",
    "detect_256.py",
    "validate_256.py",
)

PATTERNS = {
    "POTENTIAL_FUTURE_SHIFT": (
        r"\.shift\s*\(\s*-\s*[1-9]",
        "Potential forward-looking shift",
    ),
    "POTENTIAL_FORWARD_INDEX": (
        r"\.iloc\s*\[\s*(?:[A-Za-z_]\w*\s*\+\s*[1-9])",
        "Potential forward index",
    ),
    "POTENTIAL_CENTERED_WINDOW": (
        r"\bcenter\s*=\s*True\b",
        "Centered rolling window may use future rows",
    ),
    "POTENTIAL_BACKFILL": (
        r"\b(?:bfill|backfill)\s*\(",
        "Backfill may introduce future information",
    ),
    "POTENTIAL_FORWARD_LABEL": (
        r"\b(?:future|forward|target|label|horizon|"
        r"max_return|next_high)\b",
        "Review target or future-data usage",
    ),
    "TIMESTAMP_HANDLING": (
        r"\b(?:timestamp|datetime|timezone|"
        r"utc|candle_date_time)\b",
        "Timestamp handling requires review",
    ),
    "CANDLE_COMPLETENESS": (
        r"\b(?:closed|complete|incomplete|"
        r"partial|finalized|last_candle)\b",
        "Candle completeness logic found",
    ),
    "GAP_HANDLING": (
        r"\b(?:gap|missing|resample|"
        r"asfreq|interpolate|ffill)\b",
        "Candle gap or fill logic requires review",
    ),
    "SIGNAL_LOGIC": (
        r"\b(?:signal|pattern_256|detect_256|"
        r"accumulation|breakout)\b",
        "Signal-related logic found",
    ),
}

RISK_CATEGORIES = {
    "POTENTIAL_FUTURE_SHIFT",
    "POTENTIAL_FORWARD_INDEX",
    "POTENTIAL_CENTERED_WINDOW",
    "POTENTIAL_BACKFILL",
    "POTENTIAL_FORWARD_LABEL",
}


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    return json.loads(
        path.read_text(encoding="utf-8-sig")
    )


def json_content(data):
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"


def csv_content(header, rows):
    buffer = io.StringIO(newline="")
    writer = csv.writer(
        buffer,
        lineterminator="\n",
    )
    writer.writerow(header)
    writer.writerows(rows)
    return buffer.getvalue()


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, name = tempfile.mkstemp(
        prefix=".formula_audit_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def require(path):
    if not path.is_file():
        raise FileNotFoundError(str(path))
    return path


def validate_previous_gate(root):
    directory = root / GATE_DIR

    result_path = require(
        directory / "signal_integrity_result.json"
    )

    manifest_path = require(
        directory / "signal_integrity_manifest.json"
    )

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    if result.get("version") != PREVIOUS_VERSION:
        raise ValueError("Previous result version mismatch")

    if manifest.get("version") != PREVIOUS_VERSION:
        raise ValueError("Previous manifest version mismatch")

    if result.get("execution_status") != "PASS":
        raise ValueError("Previous gate did not pass")

    if result.get("overall_decision") != "HOLD":
        raise ValueError("Unexpected previous decision")

    if result.get("approved_for_live_trading") is not False:
        raise ValueError("Unsafe previous approval state")

    output_hashes = manifest.get("output_sha256", {})

    if not isinstance(output_hashes, dict):
        raise ValueError("Invalid previous output hashes")

    required_outputs = {
        "signal_integrity_result.json",
        "ohlcv_integrity_details.csv",
        "signal_implementation_integrity.csv",
    }

    if not required_outputs.issubset(output_hashes):
        raise ValueError("Incomplete previous output manifest")

    for name, expected in output_hashes.items():
        path = require(directory / name)

        if sha256(path) != expected:
            raise ValueError(
                "Previous output SHA256 mismatch: " + name
            )

    source_hashes = manifest.get(
        "input_sha256", {}
    ).get("implementation_files", {})

    if set(source_hashes) != set(SOURCE_FILES):
        raise ValueError("Implementation file set mismatch")

    for name in SOURCE_FILES:
        path = require(root / name)

        if sha256(path) != source_hashes[name]:
            raise ValueError(
                "Source changed since gate: " + name
            )

    return {
        "result": result,
        "result_sha256": sha256(result_path),
        "manifest_sha256": sha256(manifest_path),
        "source_hashes": source_hashes,
    }


def source_functions(tree, name):
    rows = []

    for node in ast.walk(tree):
        if not isinstance(
            node,
            (ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            continue

        rows.append([
            name,
            node.name,
            node.lineno,
            getattr(node, "end_lineno", node.lineno),
            len(node.args.args),
        ])

    return sorted(
        rows,
        key=lambda row: (row[0], row[2], row[1]),
    )


def inspect_file(path, name):
    content = path.read_text(
        encoding="utf-8-sig"
    )

    tree = ast.parse(
        content,
        filename=name,
    )

    lines = content.splitlines()
    findings = []

    for line_number, line in enumerate(lines, start=1):
        for category, (pattern, explanation) in (
            PATTERNS.items()
        ):
            if re.search(
                pattern,
                line,
                flags=re.IGNORECASE,
            ):
                findings.append([
                    name,
                    line_number,
                    category,
                    (
                        "REVIEW_REQUIRED"
                        if category in RISK_CATEGORIES
                        else "INFORMATION"
                    ),
                    explanation,
                    line.strip()[:240],
                ])

    return (
        source_functions(tree, name),
        findings,
        len(lines),
    )


def verify_resume(output, expected_inputs):
    names = (
        "formula_audit_result.json",
        "formula_audit_findings.csv",
        "formula_audit_functions.csv",
        "formula_audit_manifest.json",
    )

    if not all(
        (output / name).is_file()
        for name in names
    ):
        return False

    try:
        result = read_json(
            output / "formula_audit_result.json"
        )

        manifest = read_json(
            output / "formula_audit_manifest.json"
        )

        if result.get("version") != VERSION:
            return False

        if manifest.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("overall_decision") != "HOLD":
            return False

        if result.get("approved_for_live_trading") is not False:
            return False

        if manifest.get("input_sha256") != expected_inputs:
            return False

        hashes = manifest.get("output_sha256", {})

        if set(hashes) != set(names[:-1]):
            return False

        for name, expected in hashes.items():
            if sha256(output / name) != expected:
                return False

        return True

    except (OSError, ValueError, TypeError, KeyError):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    output = root / OUTPUT_DIR

    previous = validate_previous_gate(root)

    input_hashes = {
        "previous_result":
            previous["result_sha256"],
        "previous_manifest":
            previous["manifest_sha256"],
        "implementation_files":
            previous["source_hashes"],
    }

    if verify_resume(output, input_hashes):
        print("[SKIP] Verified prior audit")
        print("[DECISION] HOLD")
        return 0

    if output.exists() and any(output.iterdir()):
        raise ValueError(
            "Existing output is incomplete or changed. "
            "Manual review required; no automatic deletion."
        )

    function_rows = []
    finding_rows = []
    source_lines = {}

    for name in SOURCE_FILES:
        functions, findings, count = inspect_file(
            root / name,
            name,
        )

        function_rows.extend(functions)
        finding_rows.extend(findings)
        source_lines[name] = count

        print(
            f"[CHECK] {name}: "
            f"{len(functions)} functions, "
            f"{len(findings)} findings",
            flush=True,
        )

    review_count = sum(
        row[3] == "REVIEW_REQUIRED"
        for row in finding_rows
    )

    categories = sorted({
        row[2] for row in finding_rows
    })

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "checked_at_utc": datetime.now(
            timezone.utc
        ).isoformat(timespec="seconds"),
        "primary_market": "KRW-DKA",
        "freeze_cutoff_utc":
            "2026-10-24T00:00:00+00:00",
        "source_file_count": len(SOURCE_FILES),
        "source_line_counts": source_lines,
        "function_count": len(function_rows),
        "finding_count": len(finding_rows),
        "review_required_count": review_count,
        "finding_categories": categories,
        "source_hashes_verified": True,
        "python_syntax_verified": True,
        "static_source_audit_completed": True,
        "lookahead_absence_proven": False,
        "signal_definition_mathematically_verified": False,
        "candle_availability_at_signal_time_verified": False,
        "signal_generation_implemented": False,
        "new_prospective_events_created": 0,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
        "next_required_validation": (
            "Review function-level findings and the "
            "original source of build_features.py and "
            "detect_256.py; establish exact signal formula, "
            "closed-candle semantics and no-lookahead proof."
        ),
    }

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = output / "formula_audit_result.json"
    findings_path = output / "formula_audit_findings.csv"
    functions_path = output / "formula_audit_functions.csv"
    manifest_path = output / "formula_audit_manifest.json"

    atomic_write(
        findings_path,
        csv_content(
            (
                "source_file",
                "line",
                "category",
                "severity",
                "explanation",
                "source_excerpt",
            ),
            finding_rows,
        ),
    )

    atomic_write(
        functions_path,
        csv_content(
            (
                "source_file",
                "function",
                "start_line",
                "end_line",
                "argument_count",
            ),
            function_rows,
        ),
    )

    atomic_write(
        result_path,
        json_content(result),
    )

    manifest = {
        "version": VERSION,
        "input_sha256": input_hashes,
        "output_sha256": {
            name: sha256(output / name)
            for name in (
                "formula_audit_result.json",
                "formula_audit_findings.csv",
                "formula_audit_functions.csv",
            )
        },
    }

    atomic_write(
        manifest_path,
        json_content(manifest),
    )

    if not verify_resume(output, input_hashes):
        raise ValueError(
            "Final audit output integrity failure"
        )

    print("[PASS] " + VERSION)
    print("[INFO] Findings:", len(finding_rows))
    print("[INFO] Review required:", review_count)
    print("[DECISION] HOLD")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
