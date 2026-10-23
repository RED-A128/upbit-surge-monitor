#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Matched Comparison Audit
Clean V001

Research only. No automated trading.

Purpose:
    Compare PATTERN_256_ONLY and BOTH under matched
    market/timeframe/horizon/calendar-period conditions.

Important:
    Calendar matching does not establish causal effects.
    This script does not prove statistical independence,
    out-of-sample profitability, or live-trading readiness.
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
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_matched_comparison"
)

CLASSES = (
    "PATTERN_256_ONLY",
    "BOTH",
)

TIMEFRAMES = {
    "h1": 1,
    "h4": 4,
    "d1": 24,
}

HORIZONS = {
    24,
    72,
    168,
}

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

COMPARISON_COLUMNS = [
    "timeframe",
    "horizon",
    "period",
    "matched_market_periods",
    "pattern_256_events",
    "pattern_256_hits",
    "pattern_256_hit_rate_pct",
    "both_events",
    "both_hits",
    "both_hit_rate_pct",
    "difference_pp",
    "market_periods_both_better",
    "market_periods_pattern_better",
    "market_periods_tied",
]

OUTPUT_FILES = [
    "matched_comparison.csv",
    "matched_comparison_result.json",
]


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
        raise ValueError("Timestamp timezone missing")

    return parsed.astimezone(timezone.utc)


def parse_integer(value: str) -> int:
    return int(value.strip())


def parse_hit(value: str) -> int:
    value = value.strip()

    if value not in {"0", "1"}:
        raise ValueError("Invalid hit_30pct")

    return int(value)


def atomic_json(path: Path, payload: dict) -> None:
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
    ) as stream:
        json.dump(
            payload,
            stream,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

    os.replace(
        temporary,
        path,
    )


def period_from_timestamp(
    timestamp: datetime,
    mode: str,
) -> str:
    if mode == "month":
        return timestamp.strftime("%Y-%m")

    if mode == "quarter":
        quarter = (
            (timestamp.month - 1) // 3
        ) + 1

        return f"{timestamp.year}-Q{quarter}"

    raise ValueError(
        "Unsupported period mode"
    )


def validate_row(
    row: dict,
    period_mode: str,
):
    timeframe = row["timeframe"].strip()

    if timeframe not in TIMEFRAMES:
        raise ValueError(
            "Unsupported timeframe"
        )

    market = row["market"].strip()

    if not market.startswith("KRW-"):
        raise ValueError(
            "Invalid market"
        )

    classification = (
        row["classification"].strip()
    )

    if classification not in CLASSES:
        return None

    horizon = parse_integer(
        row["horizon"]
    )

    if horizon not in HORIZONS:
        raise ValueError(
            "Unsupported horizon"
        )

    status = (
        row["status"].strip().upper()
    )

    if status == "CENSORED":
        return None

    if status not in {
        "HIT",
        "MISS",
    }:
        raise ValueError(
            "Unsupported status"
        )

    hit = parse_hit(
        row["hit_30pct"]
    )

    if bool(hit) != (
        status == "HIT"
    ):
        raise ValueError(
            "Status/hit mismatch"
        )

    observed = parse_integer(
        row["observed_bars"]
    )

    required = parse_integer(
        row["required_bars"]
    )

    gaps = parse_integer(
        row["gap_count"]
    )

    if required < horizon:
        raise ValueError(
            "Required bars below horizon"
        )

    if observed < horizon:
        return None

    if gaps != 0:
        return None

    signal_time = parse_timestamp(
        row["signal_timestamp"]
    )

    entry_time = parse_timestamp(
        row["entry_timestamp"]
    )

    if entry_time <= signal_time:
        raise ValueError(
            "Entry timestamp invalid"
        )

    period = period_from_timestamp(
        signal_time,
        period_mode,
    )

    return (
        timeframe,
        market,
        horizon,
        period,
        classification,
        signal_time.isoformat(),
        entry_time.isoformat(),
        hit,
    )


def create_database(
    path: Path,
) -> sqlite3.Connection:
    connection = sqlite3.connect(
        str(path)
    )

    connection.execute(
        "PRAGMA journal_mode=DELETE"
    )

    connection.execute(
        "PRAGMA synchronous=FULL"
    )

    connection.execute(
        "PRAGMA temp_store=FILE"
    )

    connection.execute(
        """
        CREATE TABLE events (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            period TEXT NOT NULL,
            classification TEXT NOT NULL,
            signal_timestamp TEXT NOT NULL,
            entry_timestamp TEXT NOT NULL,
            hit INTEGER NOT NULL,

            PRIMARY KEY (
                timeframe,
                market,
                horizon,
                classification,
                signal_timestamp
            )
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX idx_matched_group
        ON events (
            timeframe,
            horizon,
            period,
            market,
            classification
        )
        """
    )

    return connection


def load_source(
    connection: sqlite3.Connection,
    source: Path,
    period_mode: str,
) -> dict:
    counters = defaultdict(int)

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(
            stream
        )

        if not reader.fieldnames:
            raise RuntimeError(
                "CSV header missing"
            )

        missing = (
            REQUIRED_COLUMNS
            - set(reader.fieldnames)
        )

        if missing:
            raise RuntimeError(
                "Missing columns: "
                + ", ".join(
                    sorted(missing)
                )
            )

        batch = []

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            counters["source_rows"] += 1

            try:
                event = validate_row(
                    row,
                    period_mode,
                )

            except (
                ValueError,
                TypeError,
                KeyError,
            ) as error:
                counters["invalid_rows"] += 1

                if counters["invalid_rows"] <= 10:
                    print(
                        f"INVALID line "
                        f"{line_number}: "
                        f"{error}",
                        flush=True,
                    )

                continue

            if event is None:
                counters["excluded_rows"] += 1
                continue

            counters["eligible_rows"] += 1

            batch.append(
                event
            )

            if len(batch) >= 10000:
                insert_batch(
                    connection,
                    batch,
                    counters,
                )

                batch.clear()

            if (
                counters["source_rows"]
                % 500000
                == 0
            ):
                print(
                    "Processed "
                    f"{counters['source_rows']:,}"
                    " source rows",
                    flush=True,
                )

        if batch:
            insert_batch(
                connection,
                batch,
                counters,
            )

    connection.commit()

    return dict(counters)


def insert_batch(
    connection: sqlite3.Connection,
    batch: list,
    counters: dict,
) -> None:
    for event in batch:
        try:
            connection.execute(
                """
                INSERT INTO events (
                    timeframe,
                    market,
                    horizon,
                    period,
                    classification,
                    signal_timestamp,
                    entry_timestamp,
                    hit
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                event,
            )

            counters[
                "unique_events"
            ] += 1

        except sqlite3.IntegrityError:
            existing = connection.execute(
                """
                SELECT
                    period,
                    entry_timestamp,
                    hit
                FROM events
                WHERE timeframe = ?
                  AND market = ?
                  AND horizon = ?
                  AND classification = ?
                  AND signal_timestamp = ?
                """,
                (
                    event[0],
                    event[1],
                    event[2],
                    event[4],
                    event[5],
                ),
            ).fetchone()

            expected = (
                event[3],
                event[6],
                event[7],
            )

            if existing == expected:
                counters[
                    "identical_duplicates"
                ] += 1
            else:
                counters[
                    "conflicting_duplicates"
                ] += 1

    connection.commit()


def create_aggregates(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE market_period_stats AS
        SELECT
            timeframe,
            market,
            horizon,
            period,
            classification,
            COUNT(*) AS events,
            SUM(hit) AS hits
        FROM events
        GROUP BY
            timeframe,
            market,
            horizon,
            period,
            classification
        """
    )

    connection.execute(
        """
        CREATE INDEX idx_market_period_stats
        ON market_period_stats (
            timeframe,
            market,
            horizon,
            period,
            classification
        )
        """
    )


def calculate_rate(
    hits: int,
    events: int,
) -> float | None:
    if events <= 0:
        return None

    return (
        100.0 * hits / events
    )


def evaluate_matched(
    connection: sqlite3.Connection,
) -> tuple:
    cursor = connection.execute(
        """
        SELECT
            a.timeframe,
            a.horizon,
            a.period,
            a.market,
            a.events,
            a.hits,
            b.events,
            b.hits
        FROM market_period_stats a
        JOIN market_period_stats b
          ON a.timeframe = b.timeframe
         AND a.horizon = b.horizon
         AND a.period = b.period
         AND a.market = b.market
        WHERE a.classification = 'PATTERN_256_ONLY'
          AND b.classification = 'BOTH'
        ORDER BY
            a.timeframe,
            a.horizon,
            a.period,
            a.market
        """
    )

    groups = defaultdict(
        lambda: {
            "matched_market_periods": 0,
            "pattern_events": 0,
            "pattern_hits": 0,
            "both_events": 0,
            "both_hits": 0,
            "both_better": 0,
            "pattern_better": 0,
            "tied": 0,
        }
    )

    matched_market_periods = 0

    for (
        timeframe,
        horizon,
        period,
        market,
        pattern_events,
        pattern_hits,
        both_events,
        both_hits,
    ) in cursor:
        key = (
            timeframe,
            horizon,
            period,
        )

        item = groups[key]

        item[
            "matched_market_periods"
        ] += 1

        item[
            "pattern_events"
        ] += pattern_events

        item[
            "pattern_hits"
        ] += pattern_hits

        item[
            "both_events"
        ] += both_events

        item[
            "both_hits"
        ] += both_hits

        matched_market_periods += 1

        pattern_rate = (
            pattern_hits
            / pattern_events
        )

        both_rate = (
            both_hits
            / both_events
        )

        if both_rate > pattern_rate:
            item[
                "both_better"
            ] += 1

        elif both_rate < pattern_rate:
            item[
                "pattern_better"
            ] += 1

        else:
            item[
                "tied"
            ] += 1

    return (
        groups,
        matched_market_periods,
    )


def write_comparison(
    path: Path,
    groups: dict,
) -> list:
    rows = []

    for key in sorted(groups):
        timeframe, horizon, period = key

        item = groups[key]

        pattern_rate = calculate_rate(
            item["pattern_hits"],
            item["pattern_events"],
        )

        both_rate = calculate_rate(
            item["both_hits"],
            item["both_events"],
        )

        difference = (
            both_rate - pattern_rate
        )

        rows.append(
            {
                "timeframe": timeframe,
                "horizon": horizon,
                "period": period,
                "matched_market_periods": item[
                    "matched_market_periods"
                ],
                "pattern_256_events": item[
                    "pattern_events"
                ],
                "pattern_256_hits": item[
                    "pattern_hits"
                ],
                "pattern_256_hit_rate_pct": round(
                    pattern_rate,
                    6,
                ),
                "both_events": item[
                    "both_events"
                ],
                "both_hits": item[
                    "both_hits"
                ],
                "both_hit_rate_pct": round(
                    both_rate,
                    6,
                ),
                "difference_pp": round(
                    difference,
                    6,
                ),
                "market_periods_both_better": item[
                    "both_better"
                ],
                "market_periods_pattern_better": item[
                    "pattern_better"
                ],
                "market_periods_tied": item[
                    "tied"
                ],
            }
        )

    temporary = path.with_suffix(
        ".csv.tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=COMPARISON_COLUMNS,
        )

        writer.writeheader()

        writer.writerows(
            rows
        )

    os.replace(
        temporary,
        path,
    )

    return rows


def run(
    source: Path,
    output: Path,
    period_mode: str,
) -> None:
    source = source.resolve()
    output = output.resolve()

    if not source.is_file():
        raise FileNotFoundError(
            source
        )

    if source.stat().st_size == 0:
        raise RuntimeError(
            "Source CSV empty"
        )

    if (
        output == source.parent
        or output == source
        or output in source.parents
    ):
        raise RuntimeError(
            "Unsafe output directory"
        )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 60)
    print("PATTERN 256 MATCHED COMPARISON")
    print("Version:", VERSION)
    print("Period mode:", period_mode)
    print("Source:", source)
    print("Output:", output)
    print("=" * 60)

    source_hash_before = sha256_file(
        source
    )

    with tempfile.TemporaryDirectory(
        prefix="pattern256_matched_",
        dir=str(output),
    ) as temporary_directory:
        database_path = (
            Path(temporary_directory)
            / "matched.sqlite3"
        )

        connection = create_database(
            database_path
        )

        try:
            counters = load_source(
                connection,
                source,
                period_mode,
            )

            if counters.get(
                "invalid_rows",
                0,
            ) != 0:
                raise RuntimeError(
                    "Invalid source rows detected"
                )

            if counters.get(
                "conflicting_duplicates",
                0,
            ) != 0:
                raise RuntimeError(
                    "Conflicting duplicate events"
                )

            create_aggregates(
                connection
            )

            (
                groups,
                matched_market_periods,
            ) = evaluate_matched(
                connection
            )

        finally:
            connection.close()

    source_hash_after = sha256_file(
        source
    )

    if (
        source_hash_before
        != source_hash_after
    ):
        raise RuntimeError(
            "Source changed during processing"
        )

    comparison_path = (
        output
        / "matched_comparison.csv"
    )

    result_path = (
        output
        / "matched_comparison_result.json"
    )

    manifest_path = (
        output
        / "matched_comparison_manifest.json"
    )

    comparison_rows = write_comparison(
        comparison_path,
        groups,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "research_decision": "HOLD",
        "period_mode": period_mode,
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
        "unique_events": counters.get(
            "unique_events",
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
        "matched_market_periods": (
            matched_market_periods
        ),
        "comparison_rows": len(
            comparison_rows
        ),
        "matching_method": (
            "Same timeframe, market, "
            "horizon and calendar period"
        ),
        "limitations": [
            "Calendar matching is not exact signal-time matching.",
            "Different signals may occur at different times within the same period.",
            "Market-period matching does not remove all event overlap.",
            "This is an observational comparison, not a causal estimate.",
            "No independent profitability or statistical independence proof.",
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
            name: sha256_file(
                output / name
            )
            for name in OUTPUT_FILES
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print("=" * 60)
    print("MATCHED COMPARISON COMPLETED")
    print("=" * 60)

    for key in sorted(counters):
        print(
            f"{key}: {counters[key]}"
        )

    print(
        "Matched market-periods:",
        matched_market_periods,
    )

    print(
        "Comparison rows:",
        len(comparison_rows),
    )

    print(
        "Research decision: HOLD"
    )

    print(
        "Live trading approved: False"
    )

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

    parser.add_argument(
        "--period",
        choices=[
            "month",
            "quarter",
        ],
        default="month",
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).resolve()

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
            args.period,
        )

        return 0

    except Exception as error:
        print(
            "FATAL: "
            f"{type(error).__name__}: "
            f"{error}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
