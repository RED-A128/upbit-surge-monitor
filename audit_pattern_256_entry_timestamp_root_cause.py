
# -*- coding: utf-8 -*-
"""
Pattern 256 Entry Timestamp Root Cause Audit
Clean V001

Purpose:
  Investigate 291 incomplete event keys caused by empty
  entry_timestamp fields.

Safety:
  Read-only source inspection.
  No original CSV modification.
  No existing checkpoint modification.
  No automatic repair.
  No training/trading approval.

Python 3.9+
Standard library only.
"""

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_entry_timestamp_root_cause"

SCHEMA_RESULT = (
    "data/reports/pattern_256_event_key_schema/"
    "event_key_schema_result.json"
)

SCHEMA_DETAIL = (
    "data/reports/pattern_256_event_key_schema/"
    "event_key_schema_detail.csv"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_entry_timestamp_root_cause"
)

EXPECTED_EVENTS = 291

REQUIRED_COLUMNS = (
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "horizon",
    "classification",
    "status",
    "entry_price",
    "signal_close",
    "observed_bars",
    "required_bars",
    "hit_30pct",
)

CODE_TERMS = (
    "entry_timestamp",
    "entry_price",
    "signal_timestamp",
    "INCOMPLETE",
    "PENDING",
    "NO_ENTRY",
    "classification",
)


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


def norm(value):
    return str(value or "").strip()


def read_json(path):
    with path.open(
        "r", encoding="utf-8-sig"
    ) as f:
        return json.load(f)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_json(path, value):
    with path.open(
        "w", encoding="utf-8"
    ) as f:
        json.dump(
            value,
            f,
            ensure_ascii=False,
            indent=2,
        )


def write_csv(path, fields, rows):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def safe_path(root, relative):
    path = (root / relative).resolve()
    path.relative_to(root)
    return path


def source_rows_for_events(
    path,
    wanted_rows,
):
    result = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.DictReader(f)

        columns = set(reader.fieldnames or [])

        missing = (
            set(REQUIRED_COLUMNS) - columns
        )

        if missing:
            raise ValueError(
                f"Missing columns: {sorted(missing)}"
            )

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            if line_number in wanted_rows:
                result[line_number] = row

            if len(result) == len(wanted_rows):
                break

    return result


def inspect_code(code_root):
    """
    Locate candidate code lines only.
    This does not prove the causal producer.
    """

    findings = []
    inspected = 0

    for path in sorted(
        code_root.rglob("*.py")
    ):
        if any(
            part in {
                ".git",
                ".venv",
                "venv",
                "__pycache__",
                "node_modules",
            }
            for part in path.parts
        ):
            continue

        if path.name == Path(__file__).name:
            continue

        inspected += 1

        try:
            lines = path.read_text(
                encoding="utf-8-sig"
            ).splitlines()
        except (UnicodeError, OSError):
            continue

        for index, line in enumerate(
            lines,
            start=1,
        ):
            matches = [
                term
                for term in CODE_TERMS
                if re.search(
                    re.escape(term),
                    line,
                    re.IGNORECASE,
                )
            ]

            if not matches:
                continue

            # Keep locations compact and reviewable.
            if len(findings) >= 1000:
                continue

            findings.append({
                "file": str(
                    path.relative_to(code_root)
                ).replace("\\", "/"),
                "line": index,
                "terms": ",".join(matches),
                "code": line.strip()[:300],
            })

    return inspected, findings


def classify(row):
    """
    Diagnostic category, not a trading label.
    """

    entry_ts = norm(
        row.get("entry_timestamp")
    )

    entry_price = norm(
        row.get("entry_price")
    )

    status = norm(
        row.get("status")
    ).upper()

    if entry_ts and entry_price:
        return "ENTRY_FIELDS_PRESENT"

    if entry_ts and not entry_price:
        return "ENTRY_PRICE_MISSING"

    if not entry_ts and entry_price:
        return "TIMESTAMP_ONLY_MISSING"

    if status in {
        "INCOMPLETE",
        "PENDING",
        "NO_ENTRY",
        "NOT_ENTERED",
    }:
        return "NO_ENTRY_FIELDS_WITH_PENDING_STATUS"

    return "NO_ENTRY_FIELDS_OTHER_STATUS"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--code-root",
        default=None,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    code_root = (
        Path(args.code_root).resolve()
        if args.code_root
        else root
    )

    schema_result_path = (
        root / SCHEMA_RESULT
    )

    schema_detail_path = (
        root / SCHEMA_DETAIL
    )

    for path in (
        schema_result_path,
        schema_detail_path,
    ):
        if not path.is_file():
            raise FileNotFoundError(path)

    schema = read_json(
        schema_result_path
    )

    events = read_csv(
        schema_detail_path
    )

    if schema.get("execution") != "PASS":
        raise ValueError(
            "Upstream schema audit did not PASS"
        )

    if len(events) != EXPECTED_EVENTS:
        raise ValueError(
            "Unexpected event count: "
            f"{len(events)}"
        )

    if (
        schema.get("base_key_incomplete_events")
        != EXPECTED_EVENTS
    ):
        raise ValueError(
            "Unexpected upstream key counts"
        )

    upstream_hashes = schema.get(
        "source_hashes",
        {},
    )

    if not upstream_hashes:
        raise ValueError(
            "Missing upstream source hashes"
        )

    grouped = defaultdict(list)

    for event in events:
        relative = norm(
            event.get("source_file")
        )

        try:
            line = int(
                event.get("row_number")
            )
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid event row number"
            )

        if line < 2:
            raise ValueError(
                "Invalid CSV line number"
            )

        grouped[relative].append(
            (line, event)
        )

    details = []
    issues = []
    status_counts = Counter()
    category_counts = Counter()
    field_missing_counts = Counter()
    class_counts = Counter()
    timeframe_counts = Counter()

    source_hashes = {}

    for relative, requests in sorted(
        grouped.items()
    ):
        try:
            path = safe_path(
                root,
                relative,
            )

            if not path.is_file():
                raise FileNotFoundError(path)

            actual_hash = sha256(path)

            expected_hash = upstream_hashes.get(
                relative
            )

            if actual_hash != expected_hash:
                raise ValueError(
                    "Source SHA256 differs "
                    "from schema audit"
                )

            source_hashes[relative] = (
                actual_hash
            )

            wanted = {
                line for line, _ in requests
            }

            source_rows = (
                source_rows_for_events(
                    path,
                    wanted,
                )
            )

            for line, event in requests:
                if line not in source_rows:
                    raise ValueError(
                        f"Missing CSV row {line}"
                    )

                row = source_rows[line]

                if (
                    norm(row.get("classification"))
                    != norm(
                        event.get("classification")
                    )
                ):
                    raise ValueError(
                        "Classification changed "
                        f"at row {line}"
                    )

                for field in (
                    "timeframe",
                    "market",
                    "signal_timestamp",
                    "entry_timestamp",
                    "horizon",
                ):
                    if (
                        norm(row.get(field))
                        != norm(event.get(field))
                    ):
                        raise ValueError(
                            f"Field changed: {field} "
                            f"at row {line}"
                        )

                category = classify(row)

                status = norm(
                    row.get("status")
                ).upper()

                classification = norm(
                    row.get("classification")
                )

                timeframe = norm(
                    row.get("timeframe")
                )

                status_counts[status] += 1
                category_counts[category] += 1
                class_counts[classification] += 1
                timeframe_counts[timeframe] += 1

                for field in REQUIRED_COLUMNS:
                    if not norm(row.get(field)):
                        field_missing_counts[
                            field
                        ] += 1

                details.append({
                    "source_file": relative,
                    "row_number": line,
                    "timeframe": timeframe,
                    "market": norm(
                        row.get("market")
                    ),
                    "signal_timestamp": norm(
                        row.get(
                            "signal_timestamp"
                        )
                    ),
                    "entry_timestamp": norm(
                        row.get(
                            "entry_timestamp"
                        )
                    ),
                    "entry_price": norm(
                        row.get("entry_price")
                    ),
                    "signal_close": norm(
                        row.get("signal_close")
                    ),
                    "classification": (
                        classification
                    ),
                    "horizon": norm(
                        row.get("horizon")
                    ),
                    "status": status,
                    "hit_30pct": norm(
                        row.get("hit_30pct")
                    ),
                    "observed_bars": norm(
                        row.get("observed_bars")
                    ),
                    "required_bars": norm(
                        row.get("required_bars")
                    ),
                    "diagnostic_category": (
                        category
                    ),
                })

        except Exception as exc:
            issues.append({
                "source_file": relative,
                "error": str(exc),
            })

    inspected, locations = (
        inspect_code(code_root)
    )

    complete = (
        len(details) == EXPECTED_EVENTS
        and len(source_hashes)
        == len(grouped)
        and not issues
    )

    all_entry_timestamp_missing = (
        field_missing_counts[
            "entry_timestamp"
        ] == EXPECTED_EVENTS
    )

    if not complete:
        diagnosis = (
            "SOURCE_VERIFICATION_FAILED"
        )

    elif all_entry_timestamp_missing:
        diagnosis = (
            "ENTRY_TIMESTAMP_MISSING_"
            "FOR_ALL_RECONCILED_EVENTS"
        )

    else:
        diagnosis = (
            "MIXED_ENTRY_TIMESTAMP_STATES"
        )

    execution = (
        "PASS"
        if complete
        else "REVIEW"
    )

    output_dir = root / OUTPUT_DIR

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_dir
        / "entry_timestamp_root_cause_detail.csv"
    )

    result_path = (
        output_dir
        / "entry_timestamp_root_cause_result.json"
    )

    code_path = (
        output_dir
        / "entry_timestamp_code_locations.csv"
    )

    manifest_path = (
        output_dir
        / "entry_timestamp_root_cause_manifest.json"
    )

    write_csv(
        detail_path,
        [
            "source_file",
            "row_number",
            "timeframe",
            "market",
            "signal_timestamp",
            "entry_timestamp",
            "entry_price",
            "signal_close",
            "classification",
            "horizon",
            "status",
            "hit_30pct",
            "observed_bars",
            "required_bars",
            "diagnostic_category",
        ],
        details,
    )

    write_csv(
        code_path,
        [
            "file",
            "line",
            "terms",
            "code",
        ],
        locations,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "research_decision": "HOLD",
        "input_events": len(events),
        "checked_events": len(details),
        "source_files": len(source_hashes),
        "all_entry_timestamp_missing": (
            all_entry_timestamp_missing
        ),
        "field_missing_counts": dict(
            field_missing_counts
        ),
        "status_counts": dict(
            status_counts
        ),
        "diagnostic_category_counts": dict(
            category_counts
        ),
        "classification_counts": dict(
            class_counts
        ),
        "timeframe_counts": dict(
            timeframe_counts
        ),
        "python_files_inspected": inspected,
        "code_locations_found": len(
            locations
        ),
        "diagnosis": diagnosis,
        "issues": issues,
        "source_hashes": source_hashes,
        "limitations": [
            "Inspects 291 reconciled events only",
            "Code matches are candidates, not proof",
            "Does not establish why entry_timestamp is empty",
            "Does not repair the inventory",
            "Does not change event identity",
            "Does not modify original data",
            "Does not prove leakage freedom",
            "Does not prove independent OOS",
            "Does not prove profitability",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    write_json(
        result_path,
        result,
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "input_sha256": {
            "event_key_schema_result.json": (
                sha256(schema_result_path)
            ),
            "event_key_schema_detail.csv": (
                sha256(schema_detail_path)
            ),
        },
        "output_sha256": {
            detail_path.name: (
                sha256(detail_path)
            ),
            code_path.name: (
                sha256(code_path)
            ),
            result_path.name: (
                sha256(result_path)
            ),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print()
    print("=" * 60)
    print("Pattern 256 Entry Timestamp Audit")
    print("=" * 60)
    print("Version:", VERSION)
    print("Execution:", execution)
    print("Checked events:", len(details))
    print("Source files:", len(source_hashes))
    print(
        "Missing entry_timestamp:",
        field_missing_counts[
            "entry_timestamp"
        ],
    )
    print(
        "Missing entry_price:",
        field_missing_counts[
            "entry_price"
        ],
    )
    print(
        "Status counts:",
        dict(status_counts),
    )
    print(
        "Diagnostic categories:",
        dict(category_counts),
    )
    print(
        "Python files inspected:",
        inspected,
    )
    print(
        "Candidate code locations:",
        len(locations),
    )
    print("Diagnosis:", diagnosis)
    print("Issues:", len(issues))
    print("Research decision: HOLD")
    print("=" * 60)

    return (
        0 if execution == "PASS"
        else 1
    )


if __name__ == "__main__":
    sys.exit(main())
