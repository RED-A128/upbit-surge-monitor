
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Population Inventory - Clean V001

Read-only audit of historical 256+accumulation checkpoints.

- No source modifications
- Per-file SHA256-validated resume checkpoints
- Duplicate event detection
- Status and timeframe distributions
- Original OHLCV hash presence inspection
- HOLD research gate
"""

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_population_inventory"

REQUIRED = {
    "timeframe", "market", "signal_timestamp",
    "entry_timestamp", "horizon", "status",
    "hit_30pct", "source_ohlcv_sha256"
}

KEY_FIELDS = (
    "timeframe", "market", "signal_timestamp",
    "entry_timestamp", "horizon"
)

OUTPUT_COLUMNS = (
    "timeframe", "market", "source_file",
    "rows", "unique_events", "duplicate_events",
    "invalid_rows", "hit_yes", "hit_no",
    "hit_unknown", "missing_source_hash",
    "source_sha256", "audit_status"
)


def hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024), b""
        ):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        suffix=".tmp",
        delete=False
    ) as handle:
        temp = Path(handle.name)
        json.dump(
            value, handle, ensure_ascii=False,
            indent=2, sort_keys=True
        )
        handle.write("\n")

    os.replace(temp, path)


def read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_csv(path, columns, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns
        )
        writer.writeheader()
        writer.writerows(records)


def normalize_hit(value):
    value = str(value).strip().lower()

    if value in ("true", "1", "yes", "hit"):
        return "YES"

    if value in ("false", "0", "no", "no_hit"):
        return "NO"

    return "UNKNOWN"


def inspect_file(path, root, source_hash):
    statuses = Counter()
    hits = Counter()
    unique = set()

    rows = 0
    duplicates = 0
    invalid = 0
    missing_hash = 0

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError("Missing CSV header")

        missing = REQUIRED - set(reader.fieldnames)

        if missing:
            raise ValueError(
                "Missing columns: " +
                ",".join(sorted(missing))
            )

        for row in reader:
            rows += 1

            key = tuple(
                str(row.get(field, "")).strip()
                for field in KEY_FIELDS
            )

            if any(not value for value in key):
                invalid += 1
            elif key in unique:
                duplicates += 1
            else:
                unique.add(key)

            status = str(
                row.get("status", "")
            ).strip().upper()

            statuses[status or "<EMPTY>"] += 1

            hit = normalize_hit(
                row.get("hit_30pct", "")
            )
            hits[hit] += 1

            source_sha = str(
                row.get("source_ohlcv_sha256", "")
            ).strip().lower()

            if not (
                len(source_sha) == 64 and
                all(
                    c in "0123456789abcdef"
                    for c in source_sha
                )
            ):
                missing_hash += 1

    relative = path.relative_to(root).as_posix()

    return {
        "source_file": relative,
        "source_sha256": source_hash,
        "timeframe": path.parent.name,
        "market": path.stem,
        "rows": rows,
        "unique_events": len(unique),
        "duplicate_events": duplicates,
        "invalid_rows": invalid,
        "hit_yes": hits["YES"],
        "hit_no": hits["NO"],
        "hit_unknown": hits["UNKNOWN"],
        "missing_source_hash": missing_hash,
        "status_counts": dict(statuses),
        "audit_status": (
            "PASS"
            if rows > 0 and invalid == 0
            and duplicates == 0
            else "REVIEW"
        )
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root", required=True
    )
    parser.add_argument(
        "--output", default=None
    )
    parser.add_argument(
        "--force", action="store_true"
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source = (
        root / "data" / "reports" /
        "pattern_256_accumulation_30pct" /
        "checkpoints"
    )

    if not source.is_dir():
        raise FileNotFoundError(
            "Checkpoint directory not found: " +
            str(source)
        )

    output = (
        Path(args.output).resolve()
        if args.output
        else root / "data" / "reports" /
        "pattern_256_population_inventory"
    )

    cache = output / "checkpoints"

    files = sorted(
        path
        for timeframe in ("h1", "h4", "d1")
        for path in (source / timeframe).glob("KRW-*.csv")
        if path.is_file()
    )

    if not files:
        raise ValueError(
            "No historical checkpoint CSV files"
        )

    records = []
    errors = []
    resumed = 0
    processed = 0

    for index, path in enumerate(files, start=1):
        relative = path.relative_to(root).as_posix()
        current_hash = hash_file(path)

        cache_path = (
            cache / path.parent.name /
            (path.stem + ".json")
        )

        record = None

        if cache_path.is_file() and not args.force:
            try:
                cached = read_json(cache_path)
                payload = cached["payload"]

                if (
                    cached["version"] == VERSION
                    and cached["source_sha256"]
                    == current_hash
                    and cached["source_file"]
                    == relative
                    and cached["payload_sha256"]
                    == hashlib.sha256(
                        json.dumps(
                            payload, sort_keys=True,
                            ensure_ascii=False
                        ).encode("utf-8")
                    ).hexdigest()
                ):
                    record = payload
                    resumed += 1
            except (
                OSError, ValueError,
                KeyError, TypeError
            ):
                record = None

        if record is None:
            try:
                record = inspect_file(
                    path, root, current_hash
                )

                payload_hash = hashlib.sha256(
                    json.dumps(
                        record, sort_keys=True,
                        ensure_ascii=False
                    ).encode("utf-8")
                ).hexdigest()

                atomic_json(
                    cache_path,
                    {
                        "version": VERSION,
                        "source_file": relative,
                        "source_sha256": current_hash,
                        "payload_sha256": payload_hash,
                        "payload": record
                    }
                )
                processed += 1

            except Exception as exc:
                errors.append({
                    "source_file": relative,
                    "error": str(exc)
                })
                continue

        records.append(record)

        if index % 25 == 0 or index == len(files):
            print(
                f"[{index}/{len(files)}] "
                f"processed={processed} "
                f"resumed={resumed}",
                flush=True
            )

    total = Counter()
    status_counts = Counter()
    timeframe_counts = defaultdict(Counter)

    for record in records:
        for field in (
            "rows", "unique_events",
            "duplicate_events", "invalid_rows",
            "hit_yes", "hit_no", "hit_unknown",
            "missing_source_hash"
        ):
            total[field] += record[field]
            timeframe_counts[
                record["timeframe"]
            ][field] += record[field]

        status_counts.update(
            record["status_counts"]
        )

    reviewed = sum(
        r["audit_status"] != "PASS"
        for r in records
    )

    # This is a source inventory, not a
    # verified unbiased signal denominator.
    execution = (
        "PASS"
        if not errors
        and len(records) == len(files)
        and reviewed == 0
        else "REVIEW"
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "source_directory": (
            source.relative_to(root).as_posix()
        ),
        "files_discovered": len(files),
        "files_processed": processed,
        "files_resumed": resumed,
        "files_audited": len(records),
        "files_requiring_review": reviewed,
        "total": dict(total),
        "status_counts": dict(
            sorted(status_counts.items())
        ),
        "timeframe_counts": {
            key: dict(value)
            for key, value in sorted(
                timeframe_counts.items()
            )
        },
        "errors": errors[:200],
        "historical_checkpoint_inventory": True,
        "all_historical_signals_proven": False,
        "unbiased_denominator_proven": False,
        "independent_oos_proven": False,
        "trading_profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Only existing 256+accumulation checkpoints",
            "Missing checkpoints cannot be inferred",
            "No independent OHLCV outcome replay",
            "Status labels are inventoried, not validated",
            "Source hashes are checked for format only",
            "No unbiased accuracy or profitability claim"
        ]
    }

    output.mkdir(
        parents=True, exist_ok=True
    )

    detail_path = (
        output / "population_inventory_detail.csv"
    )
    result_path = (
        output / "population_inventory_result.json"
    )
    manifest_path = (
        output / "population_inventory_manifest.json"
    )

    write_csv(
        detail_path,
        OUTPUT_COLUMNS,
        [
            {
                column: record.get(column, "")
                for column in OUTPUT_COLUMNS
            }
            for record in records
        ]
    )

    atomic_json(result_path, result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "script_sha256": hash_file(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            detail_path.name: hash_file(
                detail_path
            ),
            result_path.name: hash_file(
                result_path
            )
        }
    }

    atomic_json(manifest_path, manifest)

    print("\n=== POPULATION INVENTORY ===")
    print("Execution:", execution)
    print("Files:", len(records))
    print("Events:", total["rows"])
    print(
        "Duplicates:",
        total["duplicate_events"]
    )
    print(
        "Invalid rows:",
        total["invalid_rows"]
    )
    print("Status counts:", dict(status_counts))
    print("Research decision: HOLD")

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FATAL]", str(exc),
            file=sys.stderr
        )
        sys.exit(1)
