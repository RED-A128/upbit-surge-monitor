
# -*- coding: utf-8 -*-
"""
Pattern 256 Event Key Schema Audit
Clean V001

Read-only audit:
- Verify 291 reconciliation events against original CSVs.
- Inspect inventory event-key definition using Python AST.
- Compare legacy and classification-extended keys.
- Identify whether classification actually resolves invalid keys.
- Never modify original data or existing checkpoints.
"""

import argparse
import ast
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_event_key_schema"

INPUT_REL = (
    "data/reports/"
    "pattern_256_event_key_reconciliation/"
    "event_key_reconciliation_detail.csv"
)

OUTPUT_REL = (
    "data/reports/"
    "pattern_256_event_key_schema"
)

CODE_FILE = "audit_pattern_256_population_inventory.py"

EXPECTED_COUNT = 291

BASE_FIELDS = (
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "horizon",
)

EXTENDED_FIELDS = BASE_FIELDS + (
    "classification",
)

VALID_CLASSES = {
    "BOTH",
    "PATTERN_256_ONLY",
    "ACCUMULATION_ONLY",
}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)
    return h.hexdigest()


def norm(value):
    return str(value or "").strip()


def load_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def save_csv(path, fields, records):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )
        writer.writeheader()
        writer.writerows(records)


def save_json(path, obj):
    with path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            obj,
            handle,
            ensure_ascii=False,
            indent=2,
        )


def extract_event_key_fields(code_path):
    source = code_path.read_text(
        encoding="utf-8-sig",
    )
    tree = ast.parse(source)

    definitions = []

    for node in ast.walk(tree):
        if not isinstance(
            node,
            (ast.Assign, ast.AnnAssign),
        ):
            continue

        if isinstance(node, ast.Assign):
            targets = node.targets
        else:
            targets = [node.target]

        if not any(
            isinstance(target, ast.Name)
            and target.id == "EVENT_KEY_FIELDS"
            for target in targets
        ):
            continue

        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError):
            raise ValueError(
                "EVENT_KEY_FIELDS is not a static literal"
            )

        if not isinstance(value, (tuple, list)):
            raise ValueError(
                "EVENT_KEY_FIELDS must be tuple/list"
            )

        definitions.append({
            "line": node.lineno,
            "fields": list(value),
        })

    if len(definitions) != 1:
        raise ValueError(
            "Expected exactly one EVENT_KEY_FIELDS "
            f"definition; found {len(definitions)}"
        )

    return definitions[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--code-root", default=None)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    code_root = (
        Path(args.code_root).resolve()
        if args.code_root
        else root
    )

    input_path = root / INPUT_REL
    code_path = code_root / CODE_FILE

    if not input_path.is_file():
        raise FileNotFoundError(input_path)

    if not code_path.is_file():
        raise FileNotFoundError(code_path)

    records = load_csv(input_path)

    if len(records) != EXPECTED_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_COUNT} events; "
            f"found {len(records)}"
        )

    required_input = {
        "source_file",
        "row_number",
        "source_classification",
        "inventory_classification",
        "reconciliation",
    }

    if not required_input.issubset(records[0]):
        raise ValueError(
            "Unexpected reconciliation CSV schema"
        )

    definition = extract_event_key_fields(code_path)
    current_fields = tuple(definition["fields"])

    grouped = defaultdict(list)

    for record in records:
        grouped[record["source_file"]].append(
            record
        )

    issues = []
    details = []
    counts = Counter()
    source_hashes = {}
    legacy_keys = defaultdict(list)
    extended_keys = defaultdict(list)

    for relative, events in sorted(grouped.items()):
        path = (root / relative).resolve()

        try:
            path.relative_to(root)
        except ValueError:
            issues.append(
                f"Unsafe source path: {relative}"
            )
            continue

        if not path.is_file():
            issues.append(
                f"Missing source: {relative}"
            )
            continue

        source_hashes[relative] = digest(path)

        wanted = defaultdict(list)

        for event in events:
            try:
                line = int(event["row_number"])
            except (TypeError, ValueError):
                issues.append(
                    f"Invalid row number: {relative}"
                )
                continue

            wanted[line].append(event)

        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)
            columns = set(reader.fieldnames or [])

            missing = set(EXTENDED_FIELDS) - columns

            if missing:
                issues.append(
                    f"Missing columns in {relative}: "
                    f"{sorted(missing)}"
                )
                continue

            found = set()

            for row_number, row in enumerate(
                reader,
                start=2,
            ):
                if row_number not in wanted:
                    continue

                found.add(row_number)

                for event in wanted[row_number]:
                    actual = norm(
                        row.get("classification")
                    )

                    expected = norm(
                        event["source_classification"]
                    )

                    inventory = norm(
                        event["inventory_classification"]
                    )

                    legacy = tuple(
                        norm(row.get(field))
                        for field in BASE_FIELDS
                    )

                    extended = tuple(
                        norm(row.get(field))
                        for field in EXTENDED_FIELDS
                    )

                    # Include file scope so unrelated files
                    # are not incorrectly treated as duplicates.
                    legacy_group = (
                        relative,
                        legacy,
                    )

                    extended_group = (
                        relative,
                        extended,
                    )

                    event_id = (
                        relative,
                        row_number,
                    )

                    legacy_keys[legacy_group].append(
                        event_id
                    )

                    extended_keys[extended_group].append(
                        event_id
                    )

                    if actual != expected:
                        status = "SOURCE_CHANGED"
                    elif inventory != "":
                        status = "INVENTORY_NOT_EMPTY"
                    elif actual not in VALID_CLASSES:
                        status = "UNKNOWN_CLASSIFICATION"
                    elif any(not value for value in legacy):
                        status = "BASE_KEY_INCOMPLETE"
                    else:
                        status = "BASE_KEY_COMPLETE"

                    counts[status] += 1

                    details.append({
                        "source_file": relative,
                        "row_number": row_number,
                        "timeframe": norm(
                            row.get("timeframe")
                        ),
                        "market": norm(
                            row.get("market")
                        ),
                        "signal_timestamp": norm(
                            row.get("signal_timestamp")
                        ),
                        "entry_timestamp": norm(
                            row.get("entry_timestamp")
                        ),
                        "horizon": norm(
                            row.get("horizon")
                        ),
                        "classification": actual,
                        "status": status,
                        "legacy_key_complete": all(
                            legacy
                        ),
                        "extended_key_complete": all(
                            extended
                        ),
                    })

            for row_number in wanted:
                if row_number not in found:
                    issues.append(
                        f"Source row missing: "
                        f"{relative}:{row_number}"
                    )

    legacy_duplicates = sum(
        len(events) - 1
        for events in legacy_keys.values()
        if len(events) > 1
    )

    extended_duplicates = sum(
        len(events) - 1
        for events in extended_keys.values()
        if len(events) > 1
    )

    # These are schema diagnostics, not a repair.
    current_schema_matches_base = (
        current_fields == BASE_FIELDS
    )

    all_base_complete = (
        counts["BASE_KEY_COMPLETE"]
        == EXPECTED_COUNT
    )

    all_source_consistent = (
        len(details) == EXPECTED_COUNT
        and not issues
        and (
            counts["SOURCE_CHANGED"]
            + counts["INVENTORY_NOT_EMPTY"]
            + counts["UNKNOWN_CLASSIFICATION"]
        ) == 0
    )

    if not current_schema_matches_base:
        issues.append(
            "Inventory EVENT_KEY_FIELDS differs "
            "from expected baseline"
        )

    if all_source_consistent:
        if all_base_complete:
            diagnosis = (
                "CLASSIFICATION_NOT_REQUIRED_"
                "FOR_THESE_BASE_KEYS"
            )
        else:
            diagnosis = (
                "BASE_KEY_MISSING_FIELDS_"
                "NEED_INVESTIGATION"
            )
    else:
        diagnosis = "SOURCE_VERIFICATION_INCOMPLETE"

    execution = (
        "PASS"
        if all_source_consistent
        and current_schema_matches_base
        else "REVIEW"
    )

    output_dir = root / OUTPUT_REL
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_dir / "event_key_schema_detail.csv"
    )
    result_path = (
        output_dir / "event_key_schema_result.json"
    )
    manifest_path = (
        output_dir / "event_key_schema_manifest.json"
    )

    save_csv(
        detail_path,
        [
            "source_file",
            "row_number",
            "timeframe",
            "market",
            "signal_timestamp",
            "entry_timestamp",
            "horizon",
            "classification",
            "status",
            "legacy_key_complete",
            "extended_key_complete",
        ],
        details,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "research_decision": "HOLD",
        "input_events": len(records),
        "checked_events": len(details),
        "source_files_checked": len(source_hashes),
        "inventory_code_sha256": digest(code_path),
        "inventory_event_key_definition": definition,
        "expected_base_fields": list(BASE_FIELDS),
        "extended_fields": list(EXTENDED_FIELDS),
        "current_schema_matches_base": (
            current_schema_matches_base
        ),
        "base_key_complete_events": (
            counts["BASE_KEY_COMPLETE"]
        ),
        "base_key_incomplete_events": (
            counts["BASE_KEY_INCOMPLETE"]
        ),
        "legacy_duplicate_events_within_files": (
            legacy_duplicates
        ),
        "extended_duplicate_events_within_files": (
            extended_duplicates
        ),
        "status_counts": dict(counts),
        "diagnosis": diagnosis,
        "issues": issues,
        "source_hashes": source_hashes,
        "limitations": [
            "Audits only the 291 reconciled events",
            "Does not re-run the full population inventory",
            "Does not prove why prior inventory classified rows as invalid",
            "Does not prove that classification belongs in event identity",
            "Does not modify source files",
            "Does not prove independent OOS",
            "Does not prove profitability",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    save_json(result_path, result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "input_sha256": digest(input_path),
        "output_sha256": {
            detail_path.name: digest(detail_path),
            result_path.name: digest(result_path),
        },
    }

    save_json(manifest_path, manifest)

    print("\n=== EVENT KEY SCHEMA AUDIT ===")
    print("Execution:", execution)
    print("Events:", len(details))
    print("Source files:", len(source_hashes))
    print("Base key complete:",
          counts["BASE_KEY_COMPLETE"])
    print("Base key incomplete:",
          counts["BASE_KEY_INCOMPLETE"])
    print("Legacy duplicates:",
          legacy_duplicates)
    print("Extended duplicates:",
          extended_duplicates)
    print("Diagnosis:", diagnosis)
    print("Issues:", len(issues))
    print("Research decision: HOLD")

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
