#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Independence Stress Test
Clean V001

Purpose
-------
1. Read original independent backtest CSV.
2. Validate source rows.
3. Deduplicate identical signal/horizon observations.
4. Remove overlapping event windows conservatively.
5. Compare raw, deduplicated, and non-overlapping hit rates.
6. Preserve original source data.
7. Produce SHA256-verifiable research reports.

No automated trading.
No live trading approval.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
import tempfile

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_event_independence"
)

REQUIRED_COLUMNS = {
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
    "entry_timestamp",
    "status",
    "hit_30pct",
    "observed_bars",
    "required_bars",
    "gap_count",
}

TIMEFRAME_HOURS = {
    "h1": 1,
    "h4": 4,
    "d1": 24,
}

VALID_HORIZONS = {24, 72, 168}

VALID_CLASSES = {
    "PATTERN_256_ONLY",
    "BOTH",
    "ACCUMULATION_ONLY",
}

OUTPUT_FILES = (
    "event_independence_comparison.csv",
    "event_independence_result.json",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(8 * 1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def parse_timestamp(value: str) -> datetime:
    value = value.strip()

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError("Timezone missing")

    return parsed.astimezone(timezone.utc)


def parse_integer(value: str) -> int:
    return int(value.strip())


def parse_hit(value: str) -> int:
    value = value.strip()

    if value not in {"0", "1"}:
        raise ValueError("Invalid hit_30pct")

    return int(value)


def atomic_json(path: Path, data: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(
            data,
            stream,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

    os.replace(temporary, path)


def create_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path))

    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA temp_store=FILE")

    connection.execute(
        """
        CREATE TABLE observations (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            classification TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            signal_timestamp TEXT NOT NULL,
            entry_timestamp TEXT NOT NULL,
            hit INTEGER NOT NULL,
            PRIMARY KEY (
                timeframe,
                market,
                classification,
                horizon,
                signal_timestamp
            )
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX idx_observation_order
        ON observations (
            timeframe,
            horizon,
            classification,
            market,
            entry_timestamp,
            signal_timestamp
        )
        """
    )

    return connection


def validate_row(row: dict) -> tuple | None:
    timeframe = row["timeframe"].strip()
    market = row["market"].strip()
    classification = row["classification"].strip()

    if timeframe not in TIMEFRAME_HOURS:
        raise ValueError("Unsupported timeframe")

    if not market.startswith("KRW-"):
        raise ValueError("Invalid market")

    if classification not in VALID_CLASSES:
        raise ValueError("Unsupported classification")

    horizon = parse_integer(row["horizon"])

    if horizon not in VALID_HORIZONS:
        raise ValueError("Unsupported horizon")

    signal_time = parse_timestamp(row["signal_timestamp"])

    status = row["status"].strip().upper()

    if status == "CENSORED":
        return None

    if status not in {"HIT", "MISS"}:
        raise ValueError("Unsupported status")

    hit = parse_hit(row["hit_30pct"])

    if (status == "HIT") != bool(hit):
        raise ValueError("Status/hit mismatch")

    observed = parse_integer(row["observed_bars"])
    required = parse_integer(row["required_bars"])
    gaps = parse_integer(row["gap_count"])

    if observed < horizon:
        return None

    if required < horizon:
        raise ValueError("Invalid required_bars")

    if gaps != 0:
        return None

    entry_value = row["entry_timestamp"].strip()

    if not entry_value:
        raise ValueError("Missing entry timestamp")

    entry_time = parse_timestamp(entry_value)

    if entry_time <= signal_time:
        raise ValueError("Entry is not after signal")

    return (
        timeframe,
        market,
        classification,
        horizon,
        signal_time.isoformat(),
        entry_time.isoformat(),
        hit,
    )


def load_source(
    connection: sqlite3.Connection,
    source: Path,
) -> dict:
    counters = defaultdict(int)

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise RuntimeError("Source CSV has no header")

        missing = REQUIRED_COLUMNS - set(reader.fieldnames)

        if missing:
            raise RuntimeError(
                "Missing columns: " + ", ".join(sorted(missing))
            )

        batch = []

        for line_number, row in enumerate(reader, start=2):
            counters["source_rows"] += 1

            try:
                observation = validate_row(row)
            except (ValueError, TypeError) as error:
                counters["invalid_rows"] += 1

                if counters["invalid_rows"] <= 10:
                    print(
                        f"INVALID line={line_number}: {error}",
                        flush=True,
                    )

                continue

            if observation is None:
                counters["excluded_rows"] += 1
                continue

            counters["eligible_rows"] += 1

            batch.append(observation)

            if len(batch) >= 10000:
                insert_batch(connection, batch, counters)
                batch.clear()

            if counters["source_rows"] % 500000 == 0:
                print(
                    f"Processed {counters['source_rows']:,} rows",
                    flush=True,
                )

        if batch:
            insert_batch(connection, batch, counters)

    connection.commit()

    return dict(counters)


def insert_batch(
    connection: sqlite3.Connection,
    batch: list,
    counters: dict,
) -> None:
    for observation in batch:
        try:
            connection.execute(
                """
                INSERT INTO observations (
                    timeframe,
                    market,
                    classification,
                    horizon,
                    signal_timestamp,
                    entry_timestamp,
                    hit
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                observation,
            )

            counters["unique_observations"] += 1

        except sqlite3.IntegrityError:
            existing = connection.execute(
                """
                SELECT entry_timestamp, hit
                FROM observations
                WHERE timeframe = ?
                  AND market = ?
                  AND classification = ?
                  AND horizon = ?
                  AND signal_timestamp = ?
                """,
                observation[:5],
            ).fetchone()

            if existing != observation[5:7]:
                counters["conflicting_duplicates"] += 1
            else:
                counters["identical_duplicates"] += 1

    connection.commit()


def make_stats() -> dict:
    return {
        "rows": 0,
        "hits": 0,
    }


def add_stats(
    container: dict,
    key: tuple,
    hit: int,
) -> None:
    item = container[key]

    item["rows"] += 1
    item["hits"] += hit


def calculate_rate(item: dict) -> float | None:
    if item["rows"] == 0:
        return None

    return round(
        100.0 * item["hits"] / item["rows"],
        6,
    )


def evaluate_events(
    connection: sqlite3.Connection,
) -> tuple:
    deduplicated = defaultdict(make_stats)
    independent = defaultdict(make_stats)

    cursor = connection.execute(
        """
        SELECT
            timeframe,
            market,
            classification,
            horizon,
            entry_timestamp,
            hit
        FROM observations
        ORDER BY
            timeframe,
            horizon,
            classification,
            market,
            entry_timestamp,
            signal_timestamp
        """
    )

    current_group = None
    blocked_until = None

    for (
        timeframe,
        market,
        classification,
        horizon,
        entry_timestamp,
        hit,
    ) in cursor:
        key = (
            timeframe,
            horizon,
            classification,
        )

        add_stats(
            deduplicated,
            key,
            hit,
        )

        group = (
            timeframe,
            market,
            classification,
            horizon,
        )

        if group != current_group:
            current_group = group
            blocked_until = None

        entry_time = parse_timestamp(entry_timestamp)

        duration = timedelta(
            hours=TIMEFRAME_HOURS[timeframe] * horizon
        )

        if (
            blocked_until is None
            or entry_time >= blocked_until
        ):
            add_stats(
                independent,
                key,
                hit,
            )

            blocked_until = entry_time + duration

    return deduplicated, independent


def write_comparison(
    path: Path,
    deduplicated: dict,
    independent: dict,
) -> list:
    rows = []

    all_keys = sorted(
        set(deduplicated) | set(independent)
    )

    for key in all_keys:
        timeframe, horizon, classification = key

        before = deduplicated.get(
            key,
            make_stats(),
        )

        after = independent.get(
            key,
            make_stats(),
        )

        before_rate = calculate_rate(before)
        after_rate = calculate_rate(after)

        delta = None

        if (
            before_rate is not None
            and after_rate is not None
        ):
            delta = round(
                after_rate - before_rate,
                6,
            )

        rows.append(
            {
                "timeframe": timeframe,
                "horizon": horizon,
                "classification": classification,
                "deduplicated_events": before["rows"],
                "deduplicated_hits": before["hits"],
                "deduplicated_hit_rate_pct": before_rate,
                "nonoverlap_events": after["rows"],
                "nonoverlap_hits": after["hits"],
                "nonoverlap_hit_rate_pct": after_rate,
                "hit_rate_delta_pp": delta,
                "removed_overlap_events": (
                    before["rows"] - after["rows"]
                ),
            }
        )

    columns = [
        "timeframe",
        "horizon",
        "classification",
        "deduplicated_events",
        "deduplicated_hits",
        "deduplicated_hit_rate_pct",
        "nonoverlap_events",
        "nonoverlap_hits",
        "nonoverlap_hit_rate_pct",
        "hit_rate_delta_pp",
        "removed_overlap_events",
    ]

    temporary = path.with_suffix(".csv.tmp")

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
        )

        writer.writeheader()
        writer.writerows(rows)

    os.replace(temporary, path)

    return rows


def run(
    source: Path,
    output: Path,
) -> None:
    source = source.resolve()
    output = output.resolve()

    if not source.is_file():
        raise FileNotFoundError(source)

    if source.stat().st_size == 0:
        raise RuntimeError("Source CSV is empty")

    if output == source.parent:
        raise RuntimeError(
            "Output directory cannot equal source directory"
        )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("Pattern 256 Event Independence")
    print("Version:", VERSION)
    print("Source:", source)
    print("Output:", output)

    source_hash_before = sha256_file(source)

    with tempfile.TemporaryDirectory(
        prefix="pattern256_independence_",
        dir=str(output),
    ) as temporary_dir:
        database_path = (
            Path(temporary_dir) / "observations.sqlite3"
        )

        connection = create_database(database_path)

        try:
            counters = load_source(
                connection,
                source,
            )

            if counters.get("conflicting_duplicates", 0):
                raise RuntimeError(
                    "Conflicting duplicate observations found"
                )

            if counters.get("invalid_rows", 0):
                raise RuntimeError(
                    "Invalid source rows found"
                )

            deduplicated, independent = evaluate_events(
                connection
            )

        finally:
            connection.close()

    source_hash_after = sha256_file(source)

    if source_hash_before != source_hash_after:
        raise RuntimeError(
            "Source CSV changed during processing"
        )

    comparison_path = (
        output / "event_independence_comparison.csv"
    )

    result_path = (
        output / "event_independence_result.json"
    )

    manifest_path = (
        output / "event_independence_manifest.json"
    )

    comparison_rows = write_comparison(
        comparison_path,
        deduplicated,
        independent,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "research_decision": "HOLD",
        "source_sha256": source_hash_before,
        "source_rows": counters.get(
            "source_rows",
            0,
        ),
        "eligible_rows": counters.get(
            "eligible_rows",
            0,
        ),
        "excluded_rows": counters.get(
            "excluded_rows",
            0,
        ),
        "invalid_rows": counters.get(
            "invalid_rows",
            0,
        ),
        "identical_duplicates": counters.get(
            "identical_duplicates",
            0,
        ),
        "conflicting_duplicates": counters.get(
            "conflicting_duplicates",
            0,
        ),
        "unique_observations": counters.get(
            "unique_observations",
            0,
        ),
        "comparison_rows": len(comparison_rows),
        "independence_method": (
            "Greedy non-overlapping entry windows, "
            "grouped by timeframe, market, "
            "classification and horizon"
        ),
        "limitations": [
            "Non-overlapping windows do not prove statistical independence.",
            "The test does not establish independent OOS profitability.",
            "Entry-window spacing assumes regular timeframe duration.",
            "Selection bias and market regime effects remain unproven.",
        ],
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
    }

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "source_sha256": source_hash_before,
        "output_sha256": {
            name: sha256_file(output / name)
            for name in OUTPUT_FILES
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print("=" * 60)
    print("EVENT INDEPENDENCE AUDIT COMPLETED")
    print("=" * 60)

    for key in sorted(counters):
        print(f"{key}: {counters[key]}")

    print("Comparison groups:", len(comparison_rows))
    print("Research decision: HOLD")
    print("Live trading approved: False")
    print("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--source",
        default=None,
    )

    parser.add_argument(
        "--output",
        default=None,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source = (
        Path(args.source)
        if args.source
        else root / SOURCE_RELATIVE
    )

    output = (
        Path(args.output)
        if args.output
        else root / OUTPUT_RELATIVE
    )

    try:
        run(
            source,
            output,
        )

        return 0

    except Exception as error:
        print(
            f"FATAL: {type(error).__name__}: {error}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
