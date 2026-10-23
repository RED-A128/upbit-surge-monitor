
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upbit Surge Monitor
Clean V003-18

Prospective source readiness and integrity gate.

Research only.
Does not generate historical or prospective signals.
Does not modify the frozen protocol or event registry.
Does not execute trades.
"""

import argparse
import csv
import hashlib
import io
import json
import os
import sys
import tempfile

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V003-18"
FREEZE_VERSION = "Clean V003-16"
REGISTRY_VERSION = "Clean V003-17"

CUTOFF = "2026-10-24T00:00:00+00:00"

FREEZE_DIR = Path(
    "data/reports/pattern_256_prospective_freeze"
)

REGISTRY_DIR = Path(
    "data/reports/pattern_256_prospective_registry"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_prospective_source_readiness"
)

FREEZE_FILES = (
    "frozen_candidates.csv",
    "prospective_protocol.json",
    "freeze_result.json",
    "freeze_manifest.json",
)

REGISTRY_FILES = (
    "prospective_events.csv",
    "registry_result.json",
    "registry_manifest.json",
)

INPUT_FIELDS = (
    "market",
    "timeframe",
    "classification",
    "horizon",
    "signal_timestamp",
)

OUTPUT_FILES = (
    "source_readiness_result.json",
    "prospective_event_input_template.csv",
    "source_readiness_manifest.json",
)


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if reader.fieldnames is None:
            raise ValueError(
                "Missing CSV header: " + str(path)
            )

        return list(reader.fieldnames), list(reader)


def json_text(value):
    return json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"


def atomic_write(path, text):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temp_name = tempfile.mkstemp(
        prefix=".readiness_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temp = Path(temp_name)

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temp, path)

    finally:
        if temp.exists():
            temp.unlink()


def check_files(directory, names):
    hashes = {}

    for name in names:
        path = directory / name

        if not path.is_file():
            raise FileNotFoundError(str(path))

        hashes[name] = sha256(path)

    return hashes


def verify_freeze(directory):
    hashes = check_files(
        directory,
        FREEZE_FILES,
    )

    manifest = read_json(
        directory / "freeze_manifest.json"
    )

    result = read_json(
        directory / "freeze_result.json"
    )

    protocol = read_json(
        directory / "prospective_protocol.json"
    )

    for obj in (manifest, result, protocol):
        if obj.get("version") != FREEZE_VERSION:
            raise ValueError(
                "Frozen version mismatch"
            )

        if obj.get("freeze_cutoff_utc") != CUTOFF:
            raise ValueError(
                "Frozen cutoff mismatch"
            )

    if (
        result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
        or result.get("market_count") != 209
        or result.get("primary_market") != "KRW-DKA"
    ):
        raise ValueError(
            "Frozen result invalid"
        )

    expected = manifest.get("output_sha256")

    if (
        not isinstance(expected, dict)
        or set(expected) != {
            "frozen_candidates.csv",
            "prospective_protocol.json",
            "freeze_result.json",
        }
    ):
        raise ValueError(
            "Frozen SHA256 manifest invalid"
        )

    for name, digest in expected.items():
        if hashes[name] != digest:
            raise ValueError(
                "Frozen SHA256 mismatch: " + name
            )

    fields, rows = read_csv(
        directory / "frozen_candidates.csv"
    )

    if (
        "market" not in fields
        or "frozen_strict_candidate" not in fields
        or len(rows) != 209
    ):
        raise ValueError(
            "Frozen candidate schema invalid"
        )

    markets = {
        row["market"] for row in rows
    }

    if len(markets) != 209:
        raise ValueError(
            "Duplicate frozen markets"
        )

    primary = [
        row["market"]
        for row in rows
        if row["frozen_strict_candidate"] == "True"
    ]

    if primary != ["KRW-DKA"]:
        raise ValueError(
            "Primary candidate mismatch"
        )

    for obj in (result, protocol):
        if (
            obj.get("approved_for_live_trading") is not False
            or obj.get("independent_oos_proven") is not False
        ):
            raise ValueError(
                "Unsafe frozen approval flags"
            )

    return hashes, markets


def verify_registry(directory, freeze_hash, markets):
    hashes = check_files(
        directory,
        REGISTRY_FILES,
    )

    manifest = read_json(
        directory / "registry_manifest.json"
    )

    result = read_json(
        directory / "registry_result.json"
    )

    for obj in (manifest, result):
        if obj.get("version") != REGISTRY_VERSION:
            raise ValueError(
                "Registry version mismatch"
            )

        if obj.get("freeze_cutoff_utc") != CUTOFF:
            raise ValueError(
                "Registry cutoff mismatch"
            )

    if (
        manifest.get("freeze_manifest_sha256")
        != freeze_hash["freeze_manifest.json"]
    ):
        raise ValueError(
            "Registry frozen source mismatch"
        )

    if (
        manifest.get("events_sha256")
        != hashes["prospective_events.csv"]
    ):
        raise ValueError(
            "Registry events SHA256 mismatch"
        )

    if (
        manifest.get("result_sha256")
        != hashes["registry_result.json"]
    ):
        raise ValueError(
            "Registry result SHA256 mismatch"
        )

    if (
        result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
        or result.get("approved_for_live_trading") is not False
        or result.get("independent_oos_proven") is not False
    ):
        raise ValueError(
            "Registry status invalid"
        )

    fields, rows = read_csv(
        directory / "prospective_events.csv"
    )

    required = (
        "event_id",
        "market",
        "timeframe",
        "classification",
        "horizon",
        "signal_timestamp",
        "registered_at_utc",
        "status",
        "source_sha256",
    )

    if tuple(fields) != required:
        raise ValueError(
            "Registry event schema mismatch"
        )

    if (
        len(rows) != manifest.get("event_count")
        or len(rows) != result.get("registered_event_count")
    ):
        raise ValueError(
            "Registry event count mismatch"
        )

    seen = set()
    cutoff = datetime.fromisoformat(CUTOFF)

    for row in rows:
        if row["market"] not in markets:
            raise ValueError(
                "Unfrozen market in registry"
            )

        if row["status"] != "PENDING":
            raise ValueError(
                "Unexpected registry outcome"
            )

        if row["event_id"] in seen:
            raise ValueError(
                "Duplicate event"
            )

        seen.add(row["event_id"])

        timestamp = datetime.fromisoformat(
            row["signal_timestamp"].replace(
                "Z",
                "+00:00",
            )
        )

        if (
            timestamp.tzinfo is None
            or timestamp < cutoff
        ):
            raise ValueError(
                "Invalid prospective timestamp"
            )

    return hashes, len(rows)


def verify_existing_output(directory, inputs):
    manifest_path = (
        directory / "source_readiness_manifest.json"
    )

    result_path = (
        directory / "source_readiness_result.json"
    )

    template_path = (
        directory / "prospective_event_input_template.csv"
    )

    if not all(
        path.is_file()
        for path in (
            manifest_path,
            result_path,
            template_path,
        )
    ):
        return False

    try:
        manifest = read_json(manifest_path)
        result = read_json(result_path)

        if (
            manifest.get("version") != VERSION
            or result.get("version") != VERSION
        ):
            return False

        if manifest.get("input_sha256") != inputs:
            return False

        if (
            result.get("execution_status") != "PASS"
            or result.get("overall_decision") != "HOLD"
        ):
            return False

        if (
            result.get("approved_for_live_trading")
            is not False
        ):
            return False

        if (
            manifest.get("result_sha256")
            != sha256(result_path)
        ):
            return False

        if (
            manifest.get("template_sha256")
            != sha256(template_path)
        ):
            return False

        fields, rows = read_csv(template_path)

        return (
            tuple(fields) == INPUT_FIELDS
            and len(rows) == 0
        )

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    freeze_dir = (
        root / FREEZE_DIR
    ).resolve()

    registry_dir = (
        root / REGISTRY_DIR
    ).resolve()

    output_dir = (
        root / OUTPUT_DIR
    ).resolve()

    if len({
        freeze_dir,
        registry_dir,
        output_dir,
    }) != 3:
        raise ValueError(
            "Input and output paths overlap"
        )

    freeze_hashes, markets = verify_freeze(
        freeze_dir
    )

    registry_hashes, count = verify_registry(
        registry_dir,
        freeze_hashes,
        markets,
    )

    inputs = {
        "freeze": freeze_hashes,
        "registry": registry_hashes,
    }

    if verify_existing_output(output_dir, inputs):
        print(
            "[SKIP] Verified readiness output",
            flush=True,
        )
        print(
            "[DECISION] HOLD",
            flush=True,
        )
        return 0

    if (
        output_dir.exists()
        and any(output_dir.iterdir())
    ):
        raise ValueError(
            "Existing output failed integrity check; "
            "manual audit required"
        )

    now = datetime.now(
        timezone.utc
    )

    cutoff = datetime.fromisoformat(
        CUTOFF
    )

    after_cutoff = now >= cutoff

    result = {
        "version": VERSION,
        "input_freeze_version": FREEZE_VERSION,
        "input_registry_version": REGISTRY_VERSION,
        "execution_status": "PASS",
        "overall_decision": "HOLD",
        "checked_at_utc": now.isoformat(
            timespec="seconds"
        ),
        "freeze_cutoff_utc": CUTOFF,
        "freeze_cutoff_reached": after_cutoff,
        "frozen_market_count": len(markets),
        "primary_market": "KRW-DKA",
        "registered_event_count": count,
        "frozen_protocol_verified": True,
        "registry_integrity_verified": True,
        "signal_generation_implemented": False,
        "prospective_source_verified": False,
        "prospective_outcomes_evaluated": 0,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "next_required_validation": (
            "Implement and independently audit "
            "signal-time-only generation from newly "
            "collected post-cutoff OHLCV data"
        ),
    }

    buffer = io.StringIO(newline="")

    writer = csv.writer(
        buffer,
        lineterminator="\n",
    )

    writer.writerow(INPUT_FIELDS)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output_dir / "source_readiness_result.json"
    )

    template_path = (
        output_dir
        / "prospective_event_input_template.csv"
    )

    manifest_path = (
        output_dir
        / "source_readiness_manifest.json"
    )

    atomic_write(
        result_path,
        json_text(result),
    )

    atomic_write(
        template_path,
        buffer.getvalue(),
    )

    manifest = {
        "version": VERSION,
        "input_sha256": inputs,
        "result_sha256": sha256(result_path),
        "template_sha256": sha256(template_path),
    }

    atomic_write(
        manifest_path,
        json_text(manifest),
    )

    if not verify_existing_output(
        output_dir,
        inputs,
    ):
        raise ValueError(
            "Final integrity verification failed"
        )

    print(
        "[PASS] " + VERSION,
        flush=True,
    )

    print(
        "[PASS] Frozen protocol integrity",
        flush=True,
    )

    print(
        "[PASS] Registry integrity",
        flush=True,
    )

    print(
        "[PASS] Registered events: "
        + str(count),
        flush=True,
    )

    print(
        "[INFO] Freeze cutoff reached: "
        + str(after_cutoff),
        flush=True,
    )

    print(
        "[DECISION] HOLD",
        flush=True,
    )

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
