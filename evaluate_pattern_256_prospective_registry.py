
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upbit Surge Monitor
Clean V003-17

Prospective event registry initialization and audit.

Features:
- Verify frozen protocol and SHA256
- Preserve frozen market universe and thresholds
- Initialize an append-only prospective event registry
- Import genuinely new event candidates from a CSV
- Reject pre-cutoff, future, duplicate and invalid events
- Verify the registry's SHA256 integrity
- Atomic file writes
- Never authorize live trading

This script DOES NOT:
- Generate signals from OHLCV
- Backfill historical signals as prospective events
- Evaluate future outcomes
- Execute trades
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


VERSION = "Clean V003-17"
FREEZE_VERSION = "Clean V003-16"

FREEZE_REL = Path(
    "data/reports/pattern_256_prospective_freeze"
)

REGISTRY_REL = Path(
    "data/reports/pattern_256_prospective_registry"
)

CUTOFF = "2026-10-24T00:00:00+00:00"

FREEZE_FILES = (
    "frozen_candidates.csv",
    "prospective_protocol.json",
    "freeze_result.json",
)

EVENT_FIELDS = (
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

ALLOWED_TIMEFRAMES = {
    "h1",
    "h4",
    "d1",
}

ALLOWED_CLASSES = {
    "PATTERN_256_ONLY",
    "BOTH",
}

ALLOWED_HORIZONS = {
    24,
    72,
    168,
}


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def sha256_text(value):
    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8",
    ) as stream:
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
                "CSV header missing: " + str(path)
            )

        return (
            list(reader.fieldnames),
            list(reader),
        )


def csv_text(fields, rows):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        lineterminator="\n",
        extrasaction="raise",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    return buffer.getvalue()


def json_text(value):
    return json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ) + "\n"


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".registry_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def parse_timestamp(value):
    text = str(value).strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    timestamp = datetime.fromisoformat(text)

    if timestamp.tzinfo is None:
        raise ValueError(
            "Timestamp must contain UTC offset"
        )

    if timestamp.utcoffset().total_seconds() != 0:
        raise ValueError(
            "Timestamp must be UTC"
        )

    return timestamp.astimezone(
        timezone.utc
    )


def canonical_timestamp(value):
    return parse_timestamp(value).isoformat(
        timespec="seconds"
    )


def verify_freeze(freeze_dir):
    manifest_path = (
        freeze_dir / "freeze_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(
            str(manifest_path)
        )

    manifest = read_json(manifest_path)

    if manifest.get("version") != FREEZE_VERSION:
        raise ValueError(
            "Freeze manifest version mismatch"
        )

    expected = manifest.get("output_sha256")

    if (
        not isinstance(expected, dict)
        or set(expected) != set(FREEZE_FILES)
    ):
        raise ValueError(
            "Freeze output manifest invalid"
        )

    for name in FREEZE_FILES:
        path = freeze_dir / name

        if not path.is_file():
            raise FileNotFoundError(str(path))

        if sha256_file(path) != expected[name]:
            raise ValueError(
                "Freeze SHA256 mismatch: " + name
            )

    protocol = read_json(
        freeze_dir / "prospective_protocol.json"
    )

    result = read_json(
        freeze_dir / "freeze_result.json"
    )

    if (
        protocol.get("version") != FREEZE_VERSION
        or result.get("version") != FREEZE_VERSION
    ):
        raise ValueError(
            "Freeze version mismatch"
        )

    if (
        protocol.get("freeze_cutoff_utc") != CUTOFF
        or result.get("freeze_cutoff_utc") != CUTOFF
    ):
        raise ValueError(
            "Freeze cutoff mismatch"
        )

    if (
        result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
    ):
        raise ValueError(
            "Freeze decision invalid"
        )

    if (
        result.get("market_count") != 209
        or result.get("primary_market") != "KRW-DKA"
    ):
        raise ValueError(
            "Freeze market configuration invalid"
        )

    for document in (protocol, result):
        if document.get(
            "approved_for_live_trading"
        ) is not False:
            raise ValueError(
                "Unsafe trading approval flag"
            )

        if document.get(
            "independent_oos_proven"
        ) is not False:
            raise ValueError(
                "Unexpected OOS approval flag"
            )

    fields, candidates = read_csv(
        freeze_dir / "frozen_candidates.csv"
    )

    required = {
        "market",
        "frozen_priority_tier",
        "frozen_strict_candidate",
        "approved_for_live_trading",
    }

    if not required.issubset(set(fields)):
        raise ValueError(
            "Frozen candidates schema mismatch"
        )

    markets = {
        row["market"]
        for row in candidates
    }

    if (
        len(candidates) != 209
        or len(markets) != 209
    ):
        raise ValueError(
            "Frozen market count mismatch"
        )

    primary = [
        row["market"]
        for row in candidates
        if row["frozen_strict_candidate"]
        == "True"
    ]

    if primary != ["KRW-DKA"]:
        raise ValueError(
            "Primary candidate mismatch"
        )

    if any(
        row["approved_for_live_trading"]
        != "False"
        for row in candidates
    ):
        raise ValueError(
            "Unsafe frozen market"
        )

    return {
        "markets": markets,
        "manifest_sha256": sha256_file(
            manifest_path
        ),
        "frozen_candidates_sha256": expected[
            "frozen_candidates.csv"
        ],
        "protocol_sha256": expected[
            "prospective_protocol.json"
        ],
    }


def registry_paths(directory):
    return {
        "events": (
            directory / "prospective_events.csv"
        ),
        "result": (
            directory / "registry_result.json"
        ),
        "manifest": (
            directory / "registry_manifest.json"
        ),
    }


def verify_event_row(row, markets):
    if set(row) != set(EVENT_FIELDS):
        raise ValueError(
            "Event row schema mismatch"
        )

    market = row["market"]

    if market not in markets:
        raise ValueError(
            "Market not in frozen universe: "
            + market
        )

    if row["timeframe"] not in ALLOWED_TIMEFRAMES:
        raise ValueError(
            "Invalid timeframe"
        )

    if (
        row["classification"]
        not in ALLOWED_CLASSES
    ):
        raise ValueError(
            "Invalid classification"
        )

    try:
        horizon = int(row["horizon"])
    except (TypeError, ValueError):
        raise ValueError(
            "Invalid horizon"
        ) from None

    if horizon not in ALLOWED_HORIZONS:
        raise ValueError(
            "Unsupported horizon"
        )

    signal_time = parse_timestamp(
        row["signal_timestamp"]
    )

    if signal_time < parse_timestamp(CUTOFF):
        raise ValueError(
            "Pre-freeze event prohibited"
        )

    registered = parse_timestamp(
        row["registered_at_utc"]
    )

    if registered < signal_time:
        raise ValueError(
            "Registration precedes signal"
        )

    if row["status"] != "PENDING":
        raise ValueError(
            "Registry cannot contain outcomes"
        )

    source_hash = row["source_sha256"]

    if (
        len(source_hash) != 64
        or any(
            char not in "0123456789abcdef"
            for char in source_hash
        )
    ):
        raise ValueError(
            "Invalid source SHA256"
        )

    key = "|".join((
        market,
        row["timeframe"],
        row["classification"],
        str(horizon),
        canonical_timestamp(
            row["signal_timestamp"]
        ),
    ))

    expected_id = sha256_text(key)

    if row["event_id"] != expected_id:
        raise ValueError(
            "Event identity mismatch"
        )


def verify_registry(directory, freeze):
    paths = registry_paths(directory)

    existing = [
        path.exists()
        for path in paths.values()
    ]

    if not any(existing):
        return None

    if not all(existing):
        raise ValueError(
            "Incomplete registry files; "
            "manual recovery required"
        )

    manifest = read_json(paths["manifest"])
    result = read_json(paths["result"])

    if (
        manifest.get("version") != VERSION
        or result.get("version") != VERSION
    ):
        raise ValueError(
            "Registry version mismatch"
        )

    if (
        manifest.get("freeze_manifest_sha256")
        != freeze["manifest_sha256"]
    ):
        raise ValueError(
            "Frozen source changed"
        )

    if (
        manifest.get("freeze_cutoff_utc")
        != CUTOFF
    ):
        raise ValueError(
            "Registry cutoff mismatch"
        )

    if (
        result.get("freeze_cutoff_utc")
        != CUTOFF
    ):
        raise ValueError(
            "Registry result cutoff mismatch"
        )

    if (
        result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
        or result.get(
            "approved_for_live_trading"
        ) is not False
    ):
        raise ValueError(
            "Registry status invalid"
        )

    if (
        sha256_file(paths["events"])
        != manifest.get("events_sha256")
    ):
        raise ValueError(
            "Registry event SHA256 mismatch"
        )

    if (
        sha256_file(paths["result"])
        != manifest.get("result_sha256")
    ):
        raise ValueError(
            "Registry result SHA256 mismatch"
        )

    fields, rows = read_csv(paths["events"])

    if tuple(fields) != EVENT_FIELDS:
        raise ValueError(
            "Registry columns changed"
        )

    identities = set()

    for row in rows:
        verify_event_row(
            row,
            freeze["markets"],
        )

        event_id = row["event_id"]

        if event_id in identities:
            raise ValueError(
                "Duplicate registry event"
            )

        identities.add(event_id)

    if result.get("registered_event_count") != len(rows):
        raise ValueError(
            "Registry count mismatch"
        )

    if manifest.get("event_count") != len(rows):
        raise ValueError(
            "Manifest event count mismatch"
        )

    return rows


def build_event(raw, markets, source_hash, now):
    required = {
        "market",
        "timeframe",
        "classification",
        "horizon",
        "signal_timestamp",
    }

    if not required.issubset(raw):
        raise ValueError(
            "Import CSV missing required columns"
        )

    market = str(raw["market"]).strip()
    timeframe = str(raw["timeframe"]).strip()
    classification = str(
        raw["classification"]
    ).strip()

    try:
        horizon = int(
            str(raw["horizon"]).strip()
        )
    except (TypeError, ValueError):
        raise ValueError(
            "Invalid imported horizon"
        ) from None

    timestamp = parse_timestamp(
        raw["signal_timestamp"]
    )

    if timestamp < parse_timestamp(CUTOFF):
        raise ValueError(
            "Imported event predates freeze"
        )

    if timestamp > now:
        raise ValueError(
            "Imported event is in the future"
        )

    if market not in markets:
        raise ValueError(
            "Imported market not frozen"
        )

    if timeframe not in ALLOWED_TIMEFRAMES:
        raise ValueError(
            "Invalid imported timeframe"
        )

    if classification not in ALLOWED_CLASSES:
        raise ValueError(
            "Invalid imported classification"
        )

    if horizon not in ALLOWED_HORIZONS:
        raise ValueError(
            "Invalid imported horizon"
        )

    timestamp_text = timestamp.isoformat(
        timespec="seconds"
    )

    key = "|".join((
        market,
        timeframe,
        classification,
        str(horizon),
        timestamp_text,
    ))

    row = {
        "event_id": sha256_text(key),
        "market": market,
        "timeframe": timeframe,
        "classification": classification,
        "horizon": str(horizon),
        "signal_timestamp": timestamp_text,
        "registered_at_utc": now.isoformat(
            timespec="seconds"
        ),
        "status": "PENDING",
        "source_sha256": source_hash,
    }

    verify_event_row(row, markets)

    return row


def commit_registry(directory, freeze, rows):
    paths = registry_paths(directory)

    events_content = csv_text(
        EVENT_FIELDS,
        rows,
    )

    result = {
        "version": VERSION,
        "input_version": FREEZE_VERSION,
        "execution_status": "PASS",
        "overall_decision": "HOLD",
        "freeze_cutoff_utc": CUTOFF,
        "primary_market": "KRW-DKA",
        "frozen_market_count": 209,
        "registered_event_count": len(rows),
        "prospective_outcomes_evaluated": 0,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "statistical_independence_proven": False,
        "approved_for_live_trading": False,
        "next_required_validation": (
            "Generate genuinely new signal-time events "
            "using frozen signal definitions, then "
            "independently evaluate matured outcomes"
        ),
    }

    result_content = json_text(result)

    manifest = {
        "version": VERSION,
        "freeze_cutoff_utc": CUTOFF,
        "freeze_manifest_sha256": (
            freeze["manifest_sha256"]
        ),
        "frozen_candidates_sha256": (
            freeze["frozen_candidates_sha256"]
        ),
        "protocol_sha256": (
            freeze["protocol_sha256"]
        ),
        "event_count": len(rows),
        "events_sha256": sha256_text(
            events_content
        ),
        "result_sha256": sha256_text(
            result_content
        ),
    }

    # Event file first, manifest last.
    # Interrupted updates are detected on the next run.
    atomic_write(
        paths["events"],
        events_content,
    )

    atomic_write(
        paths["result"],
        result_content,
    )

    atomic_write(
        paths["manifest"],
        json_text(manifest),
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--events",
        default=None,
        help=(
            "CSV of newly observed signal-time events. "
            "Omit to initialize or verify only."
        ),
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    freeze_dir = (
        root / FREEZE_REL
    ).resolve()

    registry_dir = (
        root / REGISTRY_REL
    ).resolve()

    if (
        freeze_dir == registry_dir
        or freeze_dir in registry_dir.parents
        or registry_dir in freeze_dir.parents
    ):
        raise ValueError(
            "Unsafe output directory"
        )

    freeze = verify_freeze(
        freeze_dir
    )

    print(
        "[PASS] Frozen protocol SHA256",
        flush=True,
    )

    existing = verify_registry(
        registry_dir,
        freeze,
    )

    if existing is None:
        if (
            registry_dir.exists()
            and any(registry_dir.iterdir())
        ):
            raise ValueError(
                "Unexpected registry directory contents"
            )

        existing = []

        commit_registry(
            registry_dir,
            freeze,
            existing,
        )

        print(
            "[PASS] Prospective registry initialized",
            flush=True,
        )

    else:
        print(
            "[PASS] Existing registry verified",
            flush=True,
        )

    if args.events is not None:
        event_file = Path(
            args.events
        ).resolve()

        if not event_file.is_file():
            raise FileNotFoundError(
                str(event_file)
            )

        if (
            event_file == registry_paths(
                registry_dir
            )["events"]
        ):
            raise ValueError(
                "Cannot import registry into itself"
            )

        fields, incoming = read_csv(
            event_file
        )

        required = {
            "market",
            "timeframe",
            "classification",
            "horizon",
            "signal_timestamp",
        }

        if not required.issubset(set(fields)):
            raise ValueError(
                "Event import columns missing"
            )

        source_hash = sha256_file(
            event_file
        )

        now = datetime.now(
            timezone.utc
        )

        known_ids = {
            row["event_id"]
            for row in existing
        }

        additions = []

        for raw in incoming:
            event = build_event(
                raw,
                freeze["markets"],
                source_hash,
                now,
            )

            if event["event_id"] in known_ids:
                continue

            known_ids.add(
                event["event_id"]
            )

            additions.append(event)

        if additions:
            commit_registry(
                registry_dir,
                freeze,
                existing + additions,
            )

        print(
            "[PASS] New events registered: "
            + str(len(additions)),
            flush=True,
        )

    verified = verify_registry(
        registry_dir,
        freeze,
    )

    if verified is None:
        raise ValueError(
            "Registry verification failed"
        )

    print(
        "[PASS] " + VERSION,
        flush=True,
    )

    print(
        "[PASS] Registered events: "
        + str(len(verified)),
        flush=True,
    )

    print(
        "[PASS] Registry SHA256 integrity",
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
