
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Population Inventory
Clean V002

Purpose:
    Inspect all existing 256+accumulation
    historical backtest checkpoint CSV files.

Principles:
    - Never modify source CSV files.
    - Never delete existing research outputs.
    - Use SHA256-verified per-file checkpoints.
    - Record duplicates and malformed rows.
    - Separate execution errors from data findings.
    - Do not interpret HIT flags as proven accuracy.
    - Research decision remains HOLD.
"""

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V002"
AUDIT = "pattern_256_population_inventory"

SOURCE_RELATIVE = (
    Path("data")
    / "reports"
    / "pattern_256_accumulation_30pct"
    / "checkpoints"
)

OUTPUT_RELATIVE = (
    Path("data")
    / "reports"
    / "pattern_256_population_inventory"
)

TIMEFRAMES = ("h1", "h4", "d1")

REQUIRED_COLUMNS = {
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "horizon",
    "status",
    "hit_30pct",
    "source_ohlcv_sha256",
}

EVENT_KEY_FIELDS = (
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "horizon",
)

DETAIL_COLUMNS = (
    "timeframe",
    "market",
    "source_file",
    "rows",
    "unique_events",
    "duplicate_events",
    "invalid_rows",
    "hit_yes",
    "hit_no",
    "hit_unknown",
    "missing_source_hash",
    "source_sha256",
    "audit_status",
    "error",
)

ISSUE_COLUMNS = (
    "source_file",
    "row_number",
    "issue_type",
    "detail",
)

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def sha256_object(value):
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    return hashlib.sha256(payload).hexdigest()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)

            json.dump(
                value,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

            handle.write("\n")

        os.replace(temp_path, path)

    finally:
        if temp_path is not None:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass


def write_csv(path, columns, records):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="ignore",
        )

        writer.writeheader()

        for record in records:
            writer.writerow(record)


def normalize_hit(value):
    text = str(
        "" if value is None else value
    ).strip().lower()

    if text in ("true", "1", "yes", "hit"):
        return "YES"

    if text in ("false", "0", "no", "no_hit"):
        return "NO"

    return "UNKNOWN"


def valid_sha256(value):
    text = str(
        "" if value is None else value
    ).strip().lower()

    return (
        len(text) == 64
        and all(
            character in "0123456789abcdef"
            for character in text
        )
    )


def new_detail(path, root, source_hash):
    return {
        "timeframe": path.parent.name,
        "market": path.stem,
        "source_file": (
            path.relative_to(root).as_posix()
        ),
        "rows": 0,
        "unique_events": 0,
        "duplicate_events": 0,
        "invalid_rows": 0,
        "hit_yes": 0,
        "hit_no": 0,
        "hit_unknown": 0,
        "missing_source_hash": 0,
        "source_sha256": source_hash,
        "audit_status": "PASS",
        "error": "",
    }


def inspect_file(path, root, source_hash):
    detail = new_detail(
        path, root, source_hash
    )

    issues = []
    statuses = Counter()
    seen = set()

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "CSV header is missing"
            )

        missing = (
            REQUIRED_COLUMNS
            - set(reader.fieldnames)
        )

        if missing:
            raise ValueError(
                "Required columns missing: "
                + ", ".join(sorted(missing))
            )

        for row_number, row in enumerate(
            reader, start=2
        ):
            detail["rows"] += 1

            key = tuple(
                str(
                    row.get(field) or ""
                ).strip()
                for field in EVENT_KEY_FIELDS
            )

            if any(not part for part in key):
                detail["invalid_rows"] += 1

                issues.append({
                    "source_file": detail[
                        "source_file"
                    ],
                    "row_number": row_number,
                    "issue_type": "INVALID_EVENT_KEY",
                    "detail": repr(key),
                })

            elif key in seen:
                detail["duplicate_events"] += 1

                issues.append({
                    "source_file": detail[
                        "source_file"
                    ],
                    "row_number": row_number,
                    "issue_type": "DUPLICATE_EVENT_KEY",
                    "detail": repr(key),
                })

            else:
                seen.add(key)

            status = str(
                row.get("status") or ""
            ).strip().upper()

            statuses[
                status if status else "<EMPTY>"
            ] += 1

            hit = normalize_hit(
                row.get("hit_30pct")
            )

            detail[
                "hit_" + hit.lower()
            ] += 1

            if not valid_sha256(
                row.get("source_ohlcv_sha256")
            ):
                detail[
                    "missing_source_hash"
                ] += 1

    detail["unique_events"] = len(seen)

    if detail["rows"] == 0:
        detail["audit_status"] = "REVIEW"

        issues.append({
            "source_file": detail["source_file"],
            "row_number": "",
            "issue_type": "EMPTY_DATA",
            "detail": "CSV contains no data rows",
        })

    elif (
        detail["invalid_rows"] > 0
        or detail["duplicate_events"] > 0
        or detail["missing_source_hash"] > 0
    ):
        detail["audit_status"] = "REVIEW"

    return {
        "detail": detail,
        "issues": issues,
        "status_counts": dict(statuses),
    }


def load_checkpoint(
    checkpoint_path,
    source_path,
    root,
    source_hash,
    script_hash,
):
    if not checkpoint_path.is_file():
        return None

    try:
        with checkpoint_path.open(
            "r", encoding="utf-8"
        ) as handle:
            checkpoint = json.load(handle)

        payload = checkpoint["payload"]

        checks = (
            checkpoint.get("version") == VERSION,
            checkpoint.get("source_file")
            == source_path.relative_to(
                root
            ).as_posix(),
            checkpoint.get("source_sha256")
            == source_hash,
            checkpoint.get("script_sha256")
            == script_hash,
            checkpoint.get("payload_sha256")
            == sha256_object(payload),
            isinstance(payload.get("detail"), dict),
            isinstance(payload.get("issues"), list),
            isinstance(
                payload.get("status_counts"), dict
            ),
        )

        if all(checks):
            return payload

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
    ):
        pass

    return None


def save_checkpoint(
    checkpoint_path,
    source_path,
    root,
    source_hash,
    script_hash,
    payload,
):
    atomic_json(
        checkpoint_path,
        {
            "version": VERSION,
            "source_file": (
                source_path.relative_to(
                    root
                ).as_posix()
            ),
            "source_sha256": source_hash,
            "script_sha256": script_hash,
            "payload_sha256": (
                sha256_object(payload)
            ),
            "payload": payload,
        },
    )


def run(args):
    root = Path(args.root).resolve()

    source = root / SOURCE_RELATIVE

    output = (
        Path(args.output).resolve()
        if args.output
        else root / OUTPUT_RELATIVE
    )

    output.mkdir(
        parents=True, exist_ok=True
    )

    checkpoint_root = (
        output / "checkpoints"
    )

    script_hash = sha256_file(
        Path(__file__).resolve()
    )

    files = []

    for timeframe in TIMEFRAMES:
        folder = source / timeframe

        if folder.is_dir():
            files.extend(
                sorted(folder.glob("KRW-*.csv"))
            )

    files = [
        path
        for path in files
        if path.is_file()
    ]

    details = []
    issues = []
    errors = []

    status_counts = Counter()
    timeframe_counts = defaultdict(Counter)

    resumed = 0
    processed = 0

    print(
        f"[START] {AUDIT} {VERSION}",
        flush=True,
    )
    print(
        f"[SOURCE] {source}",
        flush=True,
    )
    print(
        f"[FILES] {len(files)}",
        flush=True,
    )

    for index, path in enumerate(
        files, start=1
    ):
        relative = (
            path.relative_to(root).as_posix()
        )

        try:
            source_hash = sha256_file(path)

            checkpoint_path = (
                checkpoint_root
                / path.parent.name
                / (path.stem + ".json")
            )

            payload = None

            if not args.force:
                payload = load_checkpoint(
                    checkpoint_path,
                    path,
                    root,
                    source_hash,
                    script_hash,
                )

            if payload is not None:
                resumed += 1

            else:
                payload = inspect_file(
                    path,
                    root,
                    source_hash,
                )

                save_checkpoint(
                    checkpoint_path,
                    path,
                    root,
                    source_hash,
                    script_hash,
                    payload,
                )

                processed += 1

            detail = payload["detail"]

            details.append(detail)

            issues.extend(
                payload["issues"]
            )

            status_counts.update(
                payload["status_counts"]
            )

            for field in (
                "rows",
                "unique_events",
                "duplicate_events",
                "invalid_rows",
                "hit_yes",
                "hit_no",
                "hit_unknown",
                "missing_source_hash",
            ):
                timeframe_counts[
                    detail["timeframe"]
                ][field] += detail[field]

        except Exception as exc:
            error = {
                "source_file": relative,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }

            errors.append(error)

            issues.append({
                "source_file": relative,
                "row_number": "",
                "issue_type": "FILE_READ_ERROR",
                "detail": str(exc),
            })

            print(
                f"[ERROR] {relative}: {exc}",
                flush=True,
            )

        if (
            index % 25 == 0
            or index == len(files)
        ):
            print(
                f"[PROGRESS] {index}/{len(files)} "
                f"processed={processed} "
                f"resumed={resumed} "
                f"errors={len(errors)}",
                flush=True,
            )

    totals = Counter()

    for detail in details:
        for field in (
            "rows",
            "unique_events",
            "duplicate_events",
            "invalid_rows",
            "hit_yes",
            "hit_no",
            "hit_unknown",
            "missing_source_hash",
        ):
            totals[field] += detail[field]

    review_files = sum(
        detail["audit_status"] != "PASS"
        for detail in details
    )

    if not files or errors:
        execution = "FAIL"

    elif review_files:
        execution = "REVIEW"

    else:
        execution = "PASS"

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": execution,
        "source_directory": str(source),
        "files_discovered": len(files),
        "files_audited": len(details),
        "files_processed": processed,
        "files_resumed": resumed,
        "files_requiring_review": review_files,
        "file_errors": len(errors),
        "total": dict(totals),
        "status_counts": dict(
            sorted(status_counts.items())
        ),
        "timeframe_counts": {
            timeframe: dict(counts)
            for timeframe, counts
            in sorted(
                timeframe_counts.items()
            )
        },
        "errors": errors,
        "issues_total": len(issues),
        "historical_checkpoint_inventory": True,
        "all_historical_signals_proven": False,
        "unbiased_denominator_proven": False,
        "independent_oos_proven": False,
        "trading_profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Existing checkpoint files only",
            "Missing checkpoint files cannot be inferred",
            "Event keys are provisional",
            "Duplicates are reported, not removed",
            "Source OHLCV hashes are format-checked only",
            "HIT flags are not independently replayed",
            "No independent OOS or profitability proof",
        ],
    }

    detail_path = (
        output
        / "population_inventory_detail.csv"
    )

    issue_path = (
        output
        / "population_inventory_issues.csv"
    )

    result_path = (
        output
        / "population_inventory_result.json"
    )

    manifest_path = (
        output
        / "population_inventory_manifest.json"
    )

    write_csv(
        detail_path,
        DETAIL_COLUMNS,
        details,
    )

    write_csv(
        issue_path,
        ISSUE_COLUMNS,
        issues,
    )

    atomic_json(
        result_path,
        result,
    )

    output_files = (
        detail_path,
        issue_path,
        result_path,
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "script_sha256": script_hash,
        "output_sha256": {
            path.name: sha256_file(path)
            for path in output_files
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print("\n=== POPULATION INVENTORY ===")
    print("Execution:", execution)
    print("Files discovered:", len(files))
    print("Files audited:", len(details))
    print("Files needing review:", review_files)
    print("File errors:", len(errors))
    print("Total event rows:", totals["rows"])
    print(
        "Duplicate event keys:",
        totals["duplicate_events"],
    )
    print(
        "Invalid event keys:",
        totals["invalid_rows"],
    )
    print(
        "Missing source hashes:",
        totals["missing_source_hash"],
    )
    print(
        "Status counts:",
        dict(status_counts),
    )
    print("Research decision: HOLD")
    print("Reports:", output)

    # REVIEW is a completed diagnostic audit.
    # FAIL indicates missing or unreadable inputs.
    return 1 if execution == "FAIL" else 0


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--output",
        default=None,
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    return run(args)


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception:
        traceback.print_exc()
        sys.exit(1)
