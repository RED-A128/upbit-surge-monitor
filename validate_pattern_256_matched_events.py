
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_matched_events.py
Clean V003-1

Checks whether PATTERN_256_ONLY and BOTH can be
compared at identical market/timeframe/signal/horizon.

Does not claim statistical independence or profitability.
Does not modify source data.
"""

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path

VERSION = "Clean V003-1"

SOURCE_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_matched_events_diagnostic"
)

EXPECTED_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

EXPECTED_ROWS = 7949826

REQUIRED = {
    "market",
    "timeframe",
    "signal_timestamp",
    "classification",
    "horizon",
    "status",
    "hit_30pct",
}

BATCH_SIZE = 10000


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def write_json(path, data):
    temporary = path.with_suffix(".tmp")

    with temporary.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def classify(value):
    value = value.strip().upper()

    if value == "PATTERN_256_ONLY":
        return "BASELINE"

    if value == "BOTH":
        return "COMBINED"

    raise ValueError(
        "Unexpected classification: " + value
    )


def setup_database(connection):
    connection.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            market TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            signal_timestamp TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            classification TEXT NOT NULL,
            status TEXT NOT NULL,
            hit INTEGER,
            PRIMARY KEY (
                market,
                timeframe,
                signal_timestamp,
                horizon,
                classification
            )
        )
    """)

    connection.commit()


def get_metadata(connection, key):
    row = connection.execute(
        "SELECT value FROM metadata WHERE key = ?",
        (key,),
    ).fetchone()

    return row[0] if row else None


def set_metadata(connection, key, value):
    connection.execute(
        """
        INSERT OR REPLACE INTO metadata(key, value)
        VALUES (?, ?)
        """,
        (key, str(value)),
    )


def process(source, connection, source_hash):
    saved_hash = get_metadata(
        connection, "source_sha256"
    )

    if saved_hash is not None:
        if saved_hash != source_hash:
            raise RuntimeError(
                "Checkpoint source SHA256 mismatch"
            )

        if get_metadata(connection, "version") != VERSION:
            raise RuntimeError(
                "Checkpoint version mismatch"
            )

    else:
        with connection:
            set_metadata(
                connection,
                "source_sha256",
                source_hash,
            )
            set_metadata(
                connection,
                "version",
                VERSION,
            )
            set_metadata(
                connection,
                "processed_rows",
                0,
            )

    processed = int(
        get_metadata(
            connection, "processed_rows"
        ) or 0
    )

    print(
        "[RESUME] processed=",
        processed,
        flush=True,
    )

    batch = []

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not REQUIRED.issubset(
            set(reader.fieldnames or [])
        ):
            raise RuntimeError(
                "Required source columns missing"
            )

        for index, row in enumerate(
            reader, start=1
        ):
            if index <= processed:
                continue

            classification = classify(
                row["classification"]
            )

            status = row["status"].strip().upper()

            if status not in {
                "HIT",
                "MISS",
                "CENSORED",
            }:
                raise RuntimeError(
                    "Unexpected status at row "
                    + str(index)
                    + ": "
                    + status
                )

            raw_hit = row["hit_30pct"].strip()

            if status == "CENSORED":
                hit = None
            elif raw_hit in {"0", "1"}:
                hit = int(raw_hit)
            else:
                raise RuntimeError(
                    "Invalid hit flag at row "
                    + str(index)
                )

            if status == "HIT" and hit != 1:
                raise RuntimeError(
                    "HIT mismatch at row "
                    + str(index)
                )

            if status == "MISS" and hit != 0:
                raise RuntimeError(
                    "MISS mismatch at row "
                    + str(index)
                )

            batch.append(
                (
                    row["market"].strip().upper(),
                    row["timeframe"].strip().lower(),
                    row["signal_timestamp"].strip(),
                    int(row["horizon"]),
                    classification,
                    status,
                    hit,
                )
            )

            if len(batch) >= BATCH_SIZE:
                with connection:
                    connection.executemany(
                        """
                        INSERT INTO signals (
                            market,
                            timeframe,
                            signal_timestamp,
                            horizon,
                            classification,
                            status,
                            hit
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )

                    set_metadata(
                        connection,
                        "processed_rows",
                        index,
                    )

                batch.clear()

                if index % 100000 == 0:
                    print(
                        "[PROGRESS] rows="
                        + format(index, ","),
                        flush=True,
                    )

        if batch:
            with connection:
                connection.executemany(
                    """
                    INSERT INTO signals (
                        market,
                        timeframe,
                        signal_timestamp,
                        horizon,
                        classification,
                        status,
                        hit
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )

                set_metadata(
                    connection,
                    "processed_rows",
                    index,
                )

    final_count = int(
        get_metadata(
            connection,
            "processed_rows",
        )
    )

    if final_count != EXPECTED_ROWS:
        raise RuntimeError(
            "Unexpected source row count: "
            + str(final_count)
        )

    return final_count


def analyze(connection):
    classification_rows = connection.execute(
        """
        SELECT
            classification,
            COUNT(*),
            SUM(CASE WHEN hit IS NOT NULL
                     THEN 1 ELSE 0 END)
        FROM signals
        GROUP BY classification
        ORDER BY classification
        """
    ).fetchall()

    paired_rows = connection.execute(
        """
        SELECT
            a.timeframe,
            a.horizon,
            COUNT(*) AS matched_count,
            SUM(
                CASE
                    WHEN a.hit IS NOT NULL
                     AND b.hit IS NOT NULL
                    THEN 1 ELSE 0
                END
            ) AS completed_pairs,
            SUM(
                CASE
                    WHEN a.hit IS NOT NULL
                     AND b.hit IS NOT NULL
                     AND a.hit != b.hit
                    THEN 1 ELSE 0
                END
            ) AS differing_outcomes
        FROM signals a
        INNER JOIN signals b
          ON a.market = b.market
         AND a.timeframe = b.timeframe
         AND a.signal_timestamp = b.signal_timestamp
         AND a.horizon = b.horizon
        WHERE a.classification = 'BASELINE'
          AND b.classification = 'COMBINED'
        GROUP BY a.timeframe, a.horizon
        ORDER BY a.timeframe, a.horizon
        """
    ).fetchall()

    pairs = [
        {
            "timeframe": row[0],
            "horizon": row[1],
            "matched_count": row[2],
            "completed_pairs": row[3],
            "differing_outcomes": row[4],
        }
        for row in paired_rows
    ]

    total_pairs = sum(
        row["matched_count"]
        for row in pairs
    )

    return {
        "classification_counts": {
            row[0]: {
                "rows": row[1],
                "completed": row[2],
            }
            for row in classification_rows
        },
        "paired_groups": pairs,
        "total_matched_pairs": total_pairs,
        "same_timestamp_comparison_possible": (
            total_pairs > 0
        ),
    }


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

    if not source.is_file():
        raise FileNotFoundError(source)

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("[START]", VERSION, flush=True)

    source_hash = sha256_file(source)

    if source_hash != EXPECTED_SHA256:
        raise RuntimeError(
            "Source SHA256 mismatch"
        )

    database_path = (
        output / "matched_events_checkpoint.sqlite"
    )

    connection = sqlite3.connect(
        str(database_path),
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

        count = process(
            source,
            connection,
            source_hash,
        )

        analysis = analyze(connection)

    finally:
        connection.close()

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": count,
        "source_sha256": source_hash,
        **analysis,
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "next_step": (
            "If no matched pairs exist, use "
            "non-overlapping event cohorts and "
            "walk-forward comparison instead "
            "of falsely claiming paired results."
        ),
    }

    result_path = (
        output / "matched_events_result.json"
    )

    write_json(result_path, result)

    manifest = {
        "version": VERSION,
        "source_sha256": source_hash,
        "output_sha256": {
            result_path.name: sha256_file(
                result_path
            )
        },
    }

    write_json(
        output / "matched_events_manifest.json",
        manifest,
    )

    if sha256_file(source) != source_hash:
        raise RuntimeError(
            "Source changed during evaluation"
        )

    print(
        "[PASS] source rows="
        + format(count, ","),
        flush=True,
    )

    print(
        "[RESULT] matched pairs="
        + format(
            analysis["total_matched_pairs"],
            ",",
        ),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED] Resume supported",
            file=sys.stderr,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
