
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_walk_forward.py

Clean V003-2

Non-overlapping event cohort Walk-Forward evaluation.

Source:
  pattern_256_accumulation_candidate_independent_backtest/
  backtest_trades.csv

Evaluation:
  Fold 1: 2024 TRAIN -> 2025 TEST
  Fold 2: 2025 TRAIN -> 2026 TEST

Rules:
  - Only completed HIT/MISS observations
  - Exclude candle gaps
  - Exclude events crossing evaluation boundaries
  - No overlap within market/timeframe/horizon cohorts
  - Baseline and combined compete for the same event slots
  - No future outcomes used for signal selection
  - No automated trading

The comparison is observational, not a causal experiment.

Checkpoint:
  SQLite ingestion with committed batches.
  Interrupted ingestion can resume.

Outputs:
  walk_forward_comparison.csv
  walk_forward_by_market.csv
  walk_forward_result.json
  walk_forward_manifest.json
"""

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V003-2"

SOURCE_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_RELATIVE = (
    "data/reports/pattern_256_walk_forward"
)

WORK_RELATIVE = (
    "data/work/pattern_256_walk_forward_v003_2"
)

EXPECTED_SOURCE_HASH = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

EXPECTED_SOURCE_ROWS = 7_949_826

TIMEFRAME_HOURS = {
    "h1": 1,
    "h4": 4,
    "d1": 24,
}

HORIZONS = (24, 72, 168)

FOLDS = (
    {
        "name": "2024_to_2025",
        "train_year": 2024,
        "test_year": 2025,
    },
    {
        "name": "2025_to_2026",
        "train_year": 2025,
        "test_year": 2026,
    },
)

TEST_END = datetime(
    2026, 9, 1,
    tzinfo=timezone.utc,
)

REQUIRED_COLUMNS = {
    "market",
    "timeframe",
    "signal_timestamp",
    "classification",
    "horizon",
    "entry_timestamp",
    "status",
    "hit_30pct",
    "observed_bars",
    "required_bars",
    "gap_count",
    "entry_time_gap",
}

BATCH_SIZE = 10000

COMPARISON_COLUMNS = [
    "fold",
    "role",
    "timeframe",
    "horizon",
    "baseline_completed",
    "baseline_hits",
    "baseline_rate_pct",
    "combined_completed",
    "combined_hits",
    "combined_rate_pct",
    "difference_pp",
    "status",
]

MARKET_COLUMNS = [
    "fold",
    "role",
    "market",
    "timeframe",
    "horizon",
    "classification",
    "completed",
    "hits",
    "hit_rate_pct",
]


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(4 * 1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_json(path, payload):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def atomic_csv(path, columns, records):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
        )

        writer.writeheader()
        writer.writerows(records)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def parse_time(value):
    if not value:
        raise ValueError("Missing timestamp")

    parsed = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        raise ValueError(
            "Timestamp timezone missing"
        )

    return parsed.astimezone(timezone.utc)


def year_start(year):
    return datetime(
        year, 1, 1,
        tzinfo=timezone.utc,
    )


def classification_name(value):
    value = value.strip().upper()

    if value == "PATTERN_256_ONLY":
        return "BASELINE"

    if value == "BOTH":
        return "COMBINED"

    raise ValueError(
        "Unsupported classification: " + value
    )


def percent(hits, completed):
    if completed == 0:
        return 0.0

    return round(
        hits * 100.0 / completed,
        6,
    )


def setup_database(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS events (
            source_row INTEGER PRIMARY KEY,
            market TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            classification TEXT NOT NULL,
            entry_timestamp TEXT NOT NULL,
            maturity_timestamp TEXT NOT NULL,
            event_year INTEGER NOT NULL,
            hit INTEGER NOT NULL
        )
    """)

    connection.commit()


def get_meta(connection, key, default=None):
    row = connection.execute(
        "SELECT value FROM metadata WHERE key = ?",
        (key,),
    ).fetchone()

    return row[0] if row else default


def set_meta(connection, key, value):
    connection.execute(
        """
        INSERT INTO metadata(key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )


def validate_database(connection, source_hash):
    existing = get_meta(
        connection,
        "source_sha256",
    )

    if existing is None:
        existing_events = connection.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0]

        if existing_events:
            raise RuntimeError(
                "Unidentified existing checkpoint data"
            )

        with connection:
            set_meta(
                connection,
                "source_sha256",
                source_hash,
            )

            set_meta(
                connection,
                "version",
                VERSION,
            )

            set_meta(
                connection,
                "processed_rows",
                0,
            )

            set_meta(
                connection,
                "accepted_rows",
                0,
            )

            set_meta(
                connection,
                "excluded_json",
                "{}",
            )

        return

    if existing != source_hash:
        raise RuntimeError(
            "Checkpoint source SHA256 mismatch"
        )

    if get_meta(connection, "version") != VERSION:
        raise RuntimeError(
            "Checkpoint version mismatch"
        )


def convert_row(row, excluded):
    timeframe = row["timeframe"].strip().lower()

    if timeframe not in TIMEFRAME_HOURS:
        excluded["unsupported_timeframe"] += 1
        return None

    try:
        horizon = int(row["horizon"])
    except (ValueError, TypeError):
        excluded["invalid_horizon"] += 1
        return None

    if horizon not in HORIZONS:
        excluded["unsupported_horizon"] += 1
        return None

    classification = classification_name(
        row["classification"]
    )

    status = row["status"].strip().upper()

    if status not in ("HIT", "MISS"):
        excluded["incomplete"] += 1
        return None

    hit_text = row["hit_30pct"].strip()

    if hit_text not in ("0", "1"):
        raise ValueError("Invalid hit_30pct")

    hit = int(hit_text)

    if (status == "HIT") != (hit == 1):
        raise ValueError(
            "Status and hit_30pct mismatch"
        )

    try:
        observed = int(row["observed_bars"])
        required = int(row["required_bars"])
        gaps = int(row["gap_count"] or "0")
        entry_gap = int(
            row["entry_time_gap"] or "0"
        )
    except (ValueError, TypeError):
        excluded["invalid_observation"] += 1
        return None

    if observed != horizon or required != horizon:
        excluded["incomplete_observation"] += 1
        return None

    if gaps != 0 or entry_gap != 0:
        excluded["candle_gap"] += 1
        return None

    try:
        signal = parse_time(
            row["signal_timestamp"]
        )

        entry = parse_time(
            row["entry_timestamp"]
        )
    except (ValueError, TypeError):
        excluded["invalid_timestamp"] += 1
        return None

    if entry <= signal:
        excluded["invalid_entry_order"] += 1
        return None

    if entry.year not in (2024, 2025, 2026):
        excluded["outside_evaluation_years"] += 1
        return None

    maturity = entry + timedelta(
        hours=TIMEFRAME_HOURS[timeframe] * horizon
    )

    end = min(
        year_start(entry.year + 1),
        TEST_END,
    )

    if entry >= end:
        excluded["after_evaluation_end"] += 1
        return None

    if maturity > end:
        excluded["crosses_year_boundary"] += 1
        return None

    market = row["market"].strip().upper()

    if not market.startswith("KRW-"):
        excluded["unsupported_market"] += 1
        return None

    return (
        market,
        timeframe,
        horizon,
        classification,
        entry.isoformat(),
        maturity.isoformat(),
        entry.year,
        hit,
    )


def ingest(
    source,
    connection,
    source_hash,
):
    validate_database(
        connection,
        source_hash,
    )

    processed = int(
        get_meta(
            connection,
            "processed_rows",
            "0",
        )
    )

    accepted = int(
        get_meta(
            connection,
            "accepted_rows",
            "0",
        )
    )

    excluded = defaultdict(
        int,
        json.loads(
            get_meta(
                connection,
                "excluded_json",
                "{}",
            )
        ),
    )

    print(
        "[RESUME] rows="
        + format(processed, ","),
        flush=True,
    )

    batch = []

    def commit_batch(last_row):
        nonlocal accepted

        with connection:
            if batch:
                connection.executemany(
                    """
                    INSERT INTO events (
                        source_row,
                        market,
                        timeframe,
                        horizon,
                        classification,
                        entry_timestamp,
                        maturity_timestamp,
                        event_year,
                        hit
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )

                accepted += len(batch)
                batch.clear()

            set_meta(
                connection,
                "processed_rows",
                last_row,
            )

            set_meta(
                connection,
                "accepted_rows",
                accepted,
            )

            set_meta(
                connection,
                "excluded_json",
                json.dumps(
                    dict(excluded),
                    sort_keys=True,
                ),
            )

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        missing = REQUIRED_COLUMNS - set(
            reader.fieldnames or []
        )

        if missing:
            raise RuntimeError(
                "Missing columns: "
                + ", ".join(sorted(missing))
            )

        last_row = processed

        for index, row in enumerate(
            reader,
            start=1,
        ):
            if index <= processed:
                continue

            last_row = index

            try:
                event = convert_row(
                    row,
                    excluded,
                )
            except Exception as exc:
                raise RuntimeError(
                    "Source row "
                    + str(index)
                    + ": "
                    + str(exc)
                ) from exc

            if event is not None:
                batch.append((index, *event))

            if index % BATCH_SIZE == 0:
                commit_batch(index)

                if index % 100000 == 0:
                    print(
                        "[PROGRESS] rows="
                        + format(index, ","),
                        flush=True,
                    )

        if last_row > processed:
            commit_batch(last_row)

    final_rows = int(
        get_meta(
            connection,
            "processed_rows",
            "0",
        )
    )

    if final_rows != EXPECTED_SOURCE_ROWS:
        raise RuntimeError(
            "Unexpected source row count: "
            + str(final_rows)
        )

    actual_accepted = connection.execute(
        "SELECT COUNT(*) FROM events"
    ).fetchone()[0]

    if actual_accepted != accepted:
        raise RuntimeError(
            "Checkpoint accepted row mismatch"
        )

    print(
        "[PASS] Ingestion completed: "
        + format(final_rows, ","),
        flush=True,
    )

    return {
        "source_rows": final_rows,
        "eligible_rows": accepted,
        "excluded": dict(excluded),
    }


def ensure_index(connection):
    print(
        "[INDEX] Building chronological index",
        flush=True,
    )

    connection.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_events_chronological
        ON events (
            event_year,
            market,
            timeframe,
            horizon,
            entry_timestamp,
            source_row
        )
    """)

    connection.commit()

    print(
        "[PASS] Index ready",
        flush=True,
    )


def evaluate_cohorts(connection):
    """
    Select one non-overlapping event at a time
    within year/market/timeframe/horizon.

    Both classifications compete in the same
    chronological event stream.

    This reduces overlap within each horizon.
    It does not eliminate cross-horizon or
    cross-market dependence.
    """

    totals = defaultdict(
        lambda: [0, 0]
    )

    markets = defaultdict(
        lambda: [0, 0]
    )

    selected_by_year = defaultdict(int)
    overlaps_by_year = defaultdict(int)

    cursor = connection.execute("""
        SELECT
            event_year,
            market,
            timeframe,
            horizon,
            classification,
            entry_timestamp,
            maturity_timestamp,
            hit
        FROM events
        ORDER BY
            event_year,
            market,
            timeframe,
            horizon,
            entry_timestamp,
            source_row
    """)

    previous_stream = None
    last_end = None

    for (
        year,
        market,
        timeframe,
        horizon,
        classification,
        entry_timestamp,
        maturity_timestamp,
        hit,
    ) in cursor:

        stream = (
            year,
            market,
            timeframe,
            horizon,
        )

        if stream != previous_stream:
            previous_stream = stream
            last_end = None

        if (
            last_end is not None
            and entry_timestamp < last_end
        ):
            overlaps_by_year[year] += 1
            continue

        last_end = maturity_timestamp

        selected_by_year[year] += 1

        key = (
            year,
            timeframe,
            horizon,
            classification,
        )

        totals[key][0] += 1
        totals[key][1] += hit

        market_key = (
            year,
            market,
            timeframe,
            horizon,
            classification,
        )

        markets[market_key][0] += 1
        markets[market_key][1] += hit

    return (
        totals,
        markets,
        dict(selected_by_year),
        dict(overlaps_by_year),
    )


def build_comparisons(totals):
    records = []

    for fold in FOLDS:
        for role, year in (
            ("TRAIN", fold["train_year"]),
            ("TEST", fold["test_year"]),
        ):
            for timeframe in (
                "h1",
                "h4",
                "d1",
            ):
                for horizon in HORIZONS:
                    baseline = totals.get(
                        (
                            year,
                            timeframe,
                            horizon,
                            "BASELINE",
                        ),
                        [0, 0],
                    )

                    combined = totals.get(
                        (
                            year,
                            timeframe,
                            horizon,
                            "COMBINED",
                        ),
                        [0, 0],
                    )

                    baseline_n, baseline_hits = baseline
                    combined_n, combined_hits = combined

                    baseline_rate = percent(
                        baseline_hits,
                        baseline_n,
                    )

                    combined_rate = percent(
                        combined_hits,
                        combined_n,
                    )

                    if baseline_n == 0 or combined_n == 0:
                        status = "INSUFFICIENT_DATA"
                    elif min(baseline_n, combined_n) < 100:
                        status = "SMALL_SAMPLE"
                    elif combined_rate > baseline_rate:
                        status = "OBSERVED_IMPROVEMENT"
                    elif combined_rate < baseline_rate:
                        status = "OBSERVED_DETERIORATION"
                    else:
                        status = "OBSERVED_EQUAL"

                    records.append({
                        "fold": fold["name"],
                        "role": role,
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "baseline_completed": baseline_n,
                        "baseline_hits": baseline_hits,
                        "baseline_rate_pct": baseline_rate,
                        "combined_completed": combined_n,
                        "combined_hits": combined_hits,
                        "combined_rate_pct": combined_rate,
                        "difference_pp": round(
                            combined_rate - baseline_rate,
                            6,
                        ),
                        "status": status,
                    })

    return records


def build_market_rows(markets):
    records = []

    for fold in FOLDS:
        for role, year in (
            ("TRAIN", fold["train_year"]),
            ("TEST", fold["test_year"]),
        ):
            for key, values in sorted(
                markets.items()
            ):
                (
                    event_year,
                    market,
                    timeframe,
                    horizon,
                    classification,
                ) = key

                if event_year != year:
                    continue

                completed, hits = values

                records.append({
                    "fold": fold["name"],
                    "role": role,
                    "market": market,
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "classification": classification,
                    "completed": completed,
                    "hits": hits,
                    "hit_rate_pct": percent(
                        hits,
                        completed,
                    ),
                })

    return records


def build_decisions(comparisons):
    decisions = []

    for fold in FOLDS:
        train = {}
        test = {}

        for row in comparisons:
            if row["fold"] != fold["name"]:
                continue

            key = (
                row["timeframe"],
                row["horizon"],
            )

            if row["role"] == "TRAIN":
                train[key] = row
            else:
                test[key] = row

        for key in sorted(train):
            train_row = train[key]
            test_row = test[key]

            train_diff = train_row["difference_pp"]
            test_diff = test_row["difference_pp"]

            sample_ok = (
                min(
                    train_row["baseline_completed"],
                    train_row["combined_completed"],
                    test_row["baseline_completed"],
                    test_row["combined_completed"],
                ) >= 100
            )

            observed_consistency = (
                sample_ok
                and train_diff > 0
                and test_diff > 0
            )

            decisions.append({
                "fold": fold["name"],
                "timeframe": key[0],
                "horizon": key[1],
                "train_difference_pp": train_diff,
                "test_difference_pp": test_diff,
                "minimum_sample_100": sample_ok,
                "observed_consistency": observed_consistency,
                "decision": "HOLD",
            })

    return decisions


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source = root / SOURCE_RELATIVE
    output = root / OUTPUT_RELATIVE
    work = root / WORK_RELATIVE

    if not source.is_file():
        raise FileNotFoundError(source)

    if source.resolve() == output.resolve():
        raise RuntimeError(
            "Unsafe output path"
        )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    work.mkdir(
        parents=True,
        exist_ok=True,
    )

    print(
        "[START] " + VERSION,
        flush=True,
    )

    source_hash = sha256_file(source)

    if source_hash != EXPECTED_SOURCE_HASH:
        raise RuntimeError(
            "Source SHA256 mismatch"
        )

    print(
        "[PASS] Source SHA256",
        flush=True,
    )

    database = work / "events.sqlite"

    connection = sqlite3.connect(
        str(database),
        timeout=120,
    )

    try:
        connection.execute(
            "PRAGMA journal_mode=DELETE"
        )

        connection.execute(
            "PRAGMA synchronous=FULL"
        )

        setup_database(connection)

        ingestion = ingest(
            source,
            connection,
            source_hash,
        )

        ensure_index(connection)

        print(
            "[ANALYZE] Non-overlapping cohorts",
            flush=True,
        )

        (
            totals,
            markets,
            selected,
            overlaps,
        ) = evaluate_cohorts(connection)

    finally:
        connection.close()

    comparisons = build_comparisons(
        totals
    )

    market_rows = build_market_rows(
        markets
    )

    decisions = build_decisions(
        comparisons
    )

    comparison_path = (
        output / "walk_forward_comparison.csv"
    )

    market_path = (
        output / "walk_forward_by_market.csv"
    )

    result_path = (
        output / "walk_forward_result.json"
    )

    manifest_path = (
        output / "walk_forward_manifest.json"
    )

    atomic_csv(
        comparison_path,
        COMPARISON_COLUMNS,
        comparisons,
    )

    atomic_csv(
        market_path,
        MARKET_COLUMNS,
        market_rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": ingestion["source_rows"],
        "source_sha256": source_hash,
        "eligible_rows": ingestion["eligible_rows"],
        "excluded": ingestion["excluded"],
        "comparison_rows": len(comparisons),
        "market_rows": len(market_rows),
        "selected_events_by_year": selected,
        "overlapping_events_excluded_by_year": overlaps,
        "folds": list(FOLDS),
        "test_end": TEST_END.isoformat(),
        "decisions": decisions,
        "matched_timestamp_pairs": 0,
        "comparison_type": (
            "Non-overlapping observational cohorts"
        ),
        "limitations": [
            "Different classifications are not paired.",
            "Differences do not establish causal effect.",
            "Cross-horizon dependence remains.",
            "Cross-market dependence remains.",
            "Chronological event selection may affect cohort composition.",
            "Theoretical maturity is used for boundary exclusion.",
            "No transaction costs or executable profitability model.",
            "No strategy optimization is performed in this test.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
    }

    atomic_json(
        result_path,
        result,
    )

    output_files = (
        comparison_path,
        market_path,
        result_path,
    )

    output_hashes = {
        path.name: sha256_file(path)
        for path in output_files
    }

    manifest = {
        "version": VERSION,
        "source_sha256": source_hash,
        "output_sha256": output_hashes,
        "source_rows": ingestion["source_rows"],
        "comparison_rows": len(comparisons),
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    if sha256_file(source) != source_hash:
        raise RuntimeError(
            "Source changed during evaluation"
        )

    for path in output_files:
        if sha256_file(path) != output_hashes[path.name]:
            raise RuntimeError(
                "Output SHA256 mismatch: "
                + path.name
            )

    if len(comparisons) != 36:
        raise RuntimeError(
            "Expected 36 comparison rows"
        )

    if len(decisions) != 18:
        raise RuntimeError(
            "Expected 18 decision rows"
        )

    print(
        "[PASS] Source rows: "
        + format(ingestion["source_rows"], ","),
        flush=True,
    )

    print(
        "[PASS] Comparison rows: "
        + str(len(comparisons)),
        flush=True,
    )

    print(
        "[PASS] Decisions: "
        + str(len(decisions)),
        flush=True,
    )

    print(
        "[PASS] Output SHA256 verified",
        flush=True,
    )

    print(
        "[OUTPUT] " + str(output),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED] Resume from committed checkpoint",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
