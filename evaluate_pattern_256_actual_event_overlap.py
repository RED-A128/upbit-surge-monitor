#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Actual Event Overlap Audit
Clean V001

Read-only source:
  data/reports/pattern_256_accumulation_candidate_independent_backtest/
    backtest_trades.csv

Outputs:
  data/reports/pattern_256_actual_event_overlap/
    actual_event_overlap_comparison.csv
    actual_event_overlap_result.json
    actual_event_overlap_manifest.json

Purpose:
  Measure temporal overlap between observed backtest events.

Scope:
  - Same market
  - Same timeframe
  - Same horizon
  - Across signal classifications

Limitations:
  - No market-regime attribution.
  - No independence proof.
  - No profitability proof.
  - No live-trading approval.
"""

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
import tempfile

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_REL = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_REL = (
    "data/reports/"
    "pattern_256_actual_event_overlap"
)

CSV_NAME = "actual_event_overlap_comparison.csv"
RESULT_NAME = "actual_event_overlap_result.json"
MANIFEST_NAME = "actual_event_overlap_manifest.json"

TIMEFRAME_MS = {
    "h1": 3600000,
    "h4": 14400000,
    "d1": 86400000,
}

HORIZONS = (24, 72, 168)

OUTPUT_COLUMNS = (
    "timeframe",
    "horizon",
    "events",
    "overlapping_events",
    "nonoverlapping_events",
    "overlap_rate_pct",
    "overlap_clusters",
    "largest_cluster",
    "unique_markets",
    "research_decision",
)


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=".tmp_",
    )

    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(path, payload):
    content = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    atomic_write(path, content)


def save_csv(path, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=".tmp_",
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=OUTPUT_COLUMNS,
            )

            writer.writeheader()
            writer.writerows(rows)

            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parse_timestamp(value):
    text = str(value).strip()

    if not text:
        return None

    try:
        number = float(text)

        if not (
            number == number
            and abs(number) != float("inf")
        ):
            raise ValueError("Non-finite timestamp")

        if number > 100000000000:
            return int(number)

        if number > 100000000:
            return int(number * 1000)

    except ValueError:
        pass

    normalized = text.replace("Z", "+00:00")

    dt = datetime.fromisoformat(normalized)

    if dt.tzinfo is None:
        raise ValueError(
            "Timezone missing in timestamp"
        )

    return int(dt.timestamp() * 1000)


def find_column(fieldnames, candidates, required=True):
    lookup = {
        name.lower().strip(): name
        for name in fieldnames
    }

    for candidate in candidates:
        found = lookup.get(candidate.lower())

        if found is not None:
            return found

    if required:
        raise ValueError(
            "Missing required column. "
            f"Expected one of: {candidates}. "
            f"Available: {fieldnames}"
        )

    return None


def resolve_columns(fieldnames):
    return {
        "market": find_column(
            fieldnames,
            ("market", "symbol", "ticker"),
        ),
        "timeframe": find_column(
            fieldnames,
            ("timeframe", "interval"),
        ),
        "horizon": find_column(
            fieldnames,
            ("horizon", "horizon_bars"),
        ),
        "timestamp": find_column(
            fieldnames,
            (
                "entry_timestamp",
                "signal_timestamp",
                "entry_time",
            ),
        ),
        "status": find_column(
            fieldnames,
            ("status", "outcome", "result_status"),
            required=False,
        ),
        "classification": find_column(
            fieldnames,
            (
                "classification",
                "signal_classification",
                "pattern_classification",
            ),
            required=False,
        ),
    }


def normalize_timeframe(value):
    text = str(value).strip().lower()

    aliases = {
        "1h": "h1",
        "4h": "h4",
        "1d": "d1",
        "60m": "h1",
        "240m": "h4",
        "day": "d1",
    }

    return aliases.get(text, text)


def normalize_horizon(value):
    text = str(value).strip()

    if not text:
        return None

    number = float(text)

    if not number.is_integer():
        raise ValueError(
            f"Invalid horizon: {value}"
        )

    return int(number)


def build_database(source_path, db_path):
    connection = sqlite3.connect(str(db_path))

    connection.execute(
        "PRAGMA journal_mode=DELETE"
    )

    connection.execute(
        "PRAGMA synchronous=FULL"
    )

    connection.execute(
        """
        CREATE TABLE events (
            market TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            classification TEXT NOT NULL
        )
        """
    )

    source_rows = 0
    eligible_rows = 0
    skipped_incomplete = 0
    batch = []

    with source_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if reader.fieldnames is None:
            raise ValueError(
                "Source CSV has no header"
            )

        columns = resolve_columns(
            reader.fieldnames
        )

        print(
            "SOURCE COLUMNS:",
            json.dumps(
                columns,
                ensure_ascii=False,
            ),
            flush=True,
        )

        for raw in reader:
            source_rows += 1

            market = str(
                raw[columns["market"]]
            ).strip().upper()

            timeframe = normalize_timeframe(
                raw[columns["timeframe"]]
            )

            horizon = normalize_horizon(
                raw[columns["horizon"]]
            )

            if timeframe not in TIMEFRAME_MS:
                raise ValueError(
                    f"Invalid timeframe at source row "
                    f"{source_rows}: {timeframe}"
                )

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Invalid horizon at source row "
                    f"{source_rows}: {horizon}"
                )

            if not market:
                raise ValueError(
                    f"Missing market at row {source_rows}"
                )

            status = ""

            if columns["status"] is not None:
                status = str(
                    raw[columns["status"]]
                ).strip().upper()

            if status in (
                "CENSORED",
                "INCOMPLETE",
                "PENDING",
            ):
                skipped_incomplete += 1
                continue

            timestamp = parse_timestamp(
                raw[columns["timestamp"]]
            )

            if timestamp is None:
                skipped_incomplete += 1
                continue

            classification = "UNSPECIFIED"

            if columns["classification"] is not None:
                classification = str(
                    raw[columns["classification"]]
                ).strip().upper()

                if not classification:
                    classification = "UNSPECIFIED"

            duration = (
                TIMEFRAME_MS[timeframe]
                * horizon
            )

            batch.append((
                market,
                timeframe,
                horizon,
                timestamp,
                timestamp + duration,
                classification,
            ))

            eligible_rows += 1

            if len(batch) >= 10000:
                connection.executemany(
                    """
                    INSERT INTO events
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )

                connection.commit()
                batch.clear()

            if source_rows % 500000 == 0:
                print(
                    f"PROGRESS: {source_rows:,} rows",
                    flush=True,
                )

    if batch:
        connection.executemany(
            """
            INSERT INTO events
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            batch,
        )

        connection.commit()

    if eligible_rows == 0:
        raise ValueError(
            "No eligible events found"
        )

    print(
        "Building temporary sort index...",
        flush=True,
    )

    connection.execute(
        """
        CREATE INDEX idx_event_order
        ON events (
            timeframe,
            horizon,
            market,
            start_ms,
            end_ms
        )
        """
    )

    connection.commit()

    return (
        connection,
        source_rows,
        eligible_rows,
        skipped_incomplete,
    )


def analyze(connection):
    stats = defaultdict(
        lambda: {
            "events": 0,
            "overlapping": 0,
            "clusters": 0,
            "largest_cluster": 0,
            "markets": set(),
        }
    )

    cursor = connection.execute(
        """
        SELECT
            market,
            timeframe,
            horizon,
            start_ms,
            end_ms
        FROM events
        ORDER BY
            timeframe,
            horizon,
            market,
            start_ms,
            end_ms
        """
    )

    previous_group = None
    cluster_end = None
    cluster_size = 0
    cluster_overlaps = 0

    def finish_cluster(group):
        nonlocal cluster_size
        nonlocal cluster_overlaps

        if group is None or cluster_size == 0:
            return

        key = (group[1], group[2])
        item = stats[key]

        item["clusters"] += 1

        item["largest_cluster"] = max(
            item["largest_cluster"],
            cluster_size,
        )

        # In a cluster of size N, the first event
        # is nonoverlapping with earlier events.
        # Remaining N-1 events overlap an active
        # interval from the same cluster.
        item["overlapping"] += cluster_overlaps

    for (
        market,
        timeframe,
        horizon,
        start_ms,
        end_ms,
    ) in cursor:
        group = (
            market,
            timeframe,
            horizon,
        )

        key = (
            timeframe,
            horizon,
        )

        item = stats[key]

        item["events"] += 1
        item["markets"].add(market)

        if group != previous_group:
            finish_cluster(previous_group)

            previous_group = group
            cluster_end = end_ms
            cluster_size = 1
            cluster_overlaps = 0

            continue

        if start_ms < cluster_end:
            cluster_size += 1
            cluster_overlaps += 1

            cluster_end = max(
                cluster_end,
                end_ms,
            )

        else:
            finish_cluster(previous_group)

            cluster_end = end_ms
            cluster_size = 1
            cluster_overlaps = 0

    finish_cluster(previous_group)

    output = []

    for timeframe in ("h1", "h4", "d1"):
        for horizon in HORIZONS:
            key = (timeframe, horizon)
            item = stats[key]

            events = item["events"]
            overlapping = item["overlapping"]

            rate = (
                100.0 * overlapping / events
                if events
                else 0.0
            )

            output.append({
                "timeframe": timeframe,
                "horizon": horizon,
                "events": events,
                "overlapping_events": overlapping,
                "nonoverlapping_events": (
                    events - overlapping
                ),
                "overlap_rate_pct": f"{rate:.8f}",
                "overlap_clusters": item["clusters"],
                "largest_cluster": (
                    item["largest_cluster"]
                ),
                "unique_markets": len(
                    item["markets"]
                ),
                "research_decision": "HOLD",
            })

    return output


def verify_outputs(
    output_dir,
    input_hash,
):
    csv_path = output_dir / CSV_NAME
    result_path = output_dir / RESULT_NAME
    manifest_path = output_dir / MANIFEST_NAME

    if not all(
        path.is_file()
        for path in (
            csv_path,
            result_path,
            manifest_path,
        )
    ):
        return False

    try:
        with result_path.open(
            "r",
            encoding="utf-8",
        ) as stream:
            result = json.load(stream)

        with manifest_path.open(
            "r",
            encoding="utf-8",
        ) as stream:
            manifest = json.load(stream)

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        for field in (
            "approved_for_live_trading",
            "statistical_independence_proven",
            "independent_oos_proven",
            "profitability_proven",
        ):
            if result.get(field) is not False:
                return False

        if manifest.get("version") != VERSION:
            return False

        if manifest.get("input_sha256", {}).get(
            "backtest_trades.csv"
        ) != input_hash:
            return False

        hashes = manifest.get(
            "output_sha256",
            {},
        )

        if hashes.get(CSV_NAME) != sha256(csv_path):
            return False

        if hashes.get(RESULT_NAME) != sha256(result_path):
            return False

        with csv_path.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)

            if reader.fieldnames != list(OUTPUT_COLUMNS):
                return False

            rows = list(reader)

        if len(rows) != 9:
            return False

        if result.get("comparison_rows") != 9:
            return False

        if sum(
            int(row["events"])
            for row in rows
        ) != result.get("eligible_rows"):
            return False

        if sum(
            int(row["overlapping_events"])
            for row in rows
        ) != result.get("overlapping_events"):
            return False

        return True

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).expanduser().resolve()

    source_path = root / SOURCE_REL
    output_dir = root / OUTPUT_REL

    if not source_path.is_file():
        raise FileNotFoundError(
            f"Missing source: {source_path}"
        )

    input_hash = sha256(source_path)

    if (
        not args.force
        and verify_outputs(
            output_dir,
            input_hash,
        )
    ):
        print(
            "PASS: existing output integrity verified"
        )

        print(
            "SKIP: completed audit already exists"
        )

        return 0

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Temporary SQLite database is kept outside
    # the final report directory.
    # It is removed after successful or failed run.
    with tempfile.TemporaryDirectory(
        prefix="pattern256_overlap_"
    ) as temporary:
        db_path = (
            Path(temporary)
            / "events.sqlite"
        )

        connection = None

        try:
            (
                connection,
                source_rows,
                eligible_rows,
                skipped_incomplete,
            ) = build_database(
                source_path,
                db_path,
            )

            rows = analyze(connection)

        finally:
            if connection is not None:
                connection.close()

    if len(rows) != 9:
        raise ValueError(
            "Unexpected comparison count"
        )

    overlapping = sum(
        int(row["overlapping_events"])
        for row in rows
    )

    clusters = sum(
        int(row["overlap_clusters"])
        for row in rows
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": source_rows,
        "eligible_rows": eligible_rows,
        "skipped_incomplete_rows": skipped_incomplete,
        "comparison_rows": len(rows),
        "overlapping_events": overlapping,
        "nonoverlapping_events": (
            eligible_rows - overlapping
        ),
        "overlap_clusters": clusters,
        "overlap_rate_pct": (
            100.0 * overlapping / eligible_rows
        ),
        "research_decision": "HOLD",
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "method": (
            "Same-market, same-timeframe, same-horizon "
            "interval overlap"
        ),
        "limitations": [
            "Overlap is measured using theoretical event windows.",
            "Missing candles are not reconstructed.",
            "Signal classifications are pooled.",
            "Market regimes are not attributed.",
            "Cluster counts are not effective sample sizes.",
            "No cluster-robust confidence intervals.",
            "No prospective independent OOS proof.",
            "No profitability proof.",
            "No automated trading.",
        ],
        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
    }

    csv_path = output_dir / CSV_NAME
    result_path = output_dir / RESULT_NAME
    manifest_path = output_dir / MANIFEST_NAME

    save_csv(
        csv_path,
        rows,
    )

    save_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "input_sha256": {
            "backtest_trades.csv": input_hash,
        },
        "output_sha256": {
            CSV_NAME: sha256(csv_path),
            RESULT_NAME: sha256(result_path),
        },
    }

    save_json(
        manifest_path,
        manifest,
    )

    if not verify_outputs(
        output_dir,
        input_hash,
    ):
        raise RuntimeError(
            "Final output verification failed"
        )

    print("=" * 60)
    print("PATTERN 256 ACTUAL EVENT OVERLAP")
    print("=" * 60)
    print("Execution: PASS")
    print(f"Version: {VERSION}")
    print(f"Source rows: {source_rows:,}")
    print(f"Eligible rows: {eligible_rows:,}")
    print(f"Overlapping events: {overlapping:,}")
    print(f"Overlap clusters: {clusters:,}")
    print("Comparison rows: 9")
    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")
    print("Research decision: HOLD")
    print("Statistical independence proven: False")
    print("Live trading approved: False")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
