#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Non-Overlap Retest
Clean V001

Purpose:
    Re-evaluate 256-only and BOTH signals after
    deterministic within-class temporal de-overlap.

Important:
    This is a diagnostic retest.
    It does NOT prove statistical independence,
    independent OOS performance, or profitability.

Source:
    data/reports/
    pattern_256_accumulation_candidate_independent_backtest/
    backtest_trades.csv

Output:
    data/reports/pattern_256_nonoverlap_retest/
"""

import argparse
import csv
import hashlib
import json
import math
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
    "pattern_256_nonoverlap_retest"
)

CSV_NAME = "nonoverlap_retest_comparison.csv"
RESULT_NAME = "nonoverlap_retest_result.json"
MANIFEST_NAME = "nonoverlap_retest_manifest.json"

CUTOFF = "2025-01-01T00:00:00+00:00"
TEST_END = "2026-09-01T00:00:00+00:00"

TIMEFRAME_MS = {
    "h1": 3600000,
    "h4": 14400000,
    "d1": 86400000,
}

HORIZONS = (24, 72, 168)

CLASSES = (
    "PATTERN_256_ONLY",
    "BOTH",
)

CSV_COLUMNS = [
    "split",
    "timeframe",
    "horizon",
    "classification",
    "source_events",
    "selected_events",
    "excluded_overlap",
    "hits",
    "hit_rate_pct",
    "unique_markets",
    "research_decision",
]


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def write_atomic(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, payload):
    content = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")

    write_atomic(path, content)


def write_csv(path, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=CSV_COLUMNS,
            )

            writer.writeheader()
            writer.writerows(rows)

            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parse_time(value):
    text = str(value).strip()

    if not text:
        return None

    dt = datetime.fromisoformat(
        text.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise ValueError(
            f"Timezone missing: {text}"
        )

    return int(dt.timestamp() * 1000)


def normalize_timeframe(value):
    aliases = {
        "1h": "h1",
        "4h": "h4",
        "1d": "d1",
    }

    text = str(value).strip().lower()

    return aliases.get(text, text)


def validate_columns(fieldnames):
    required = {
        "market",
        "timeframe",
        "horizon",
        "entry_timestamp",
        "classification",
        "status",
        "hit_30pct",
    }

    missing = required - set(fieldnames or [])

    if missing:
        raise ValueError(
            f"Missing source columns: {sorted(missing)}"
        )


def classify_split(start_ms, end_ms):
    cutoff_ms = parse_time(CUTOFF)
    test_end_ms = parse_time(TEST_END)

    if end_ms <= cutoff_ms:
        return "TRAIN"

    if (
        start_ms >= cutoff_ms
        and end_ms <= test_end_ms
    ):
        return "TEST"

    return None


def create_database(source, db_path):
    conn = sqlite3.connect(str(db_path))

    conn.execute("PRAGMA synchronous=FULL")

    conn.execute(
        """
        CREATE TABLE events (
            split TEXT NOT NULL,
            market TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            classification TEXT NOT NULL,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            hit INTEGER NOT NULL
        )
        """
    )

    counters = defaultdict(int)
    batch = []

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        validate_columns(reader.fieldnames)

        for row in reader:
            counters["source_rows"] += 1

            status = row["status"].strip().upper()

            if status in {
                "CENSORED",
                "INCOMPLETE",
                "PENDING",
            }:
                counters["excluded_incomplete"] += 1
                continue

            if status not in {"HIT", "MISS"}:
                raise ValueError(
                    "Unexpected completed status at row "
                    f"{counters['source_rows']}: {status}"
                )

            hit_text = row["hit_30pct"].strip()

            if hit_text not in {"0", "1"}:
                raise ValueError(
                    "Invalid hit_30pct at row "
                    f"{counters['source_rows']}"
                )

            hit = int(hit_text)

            if (status == "HIT") != bool(hit):
                raise ValueError(
                    "Status/hit mismatch at row "
                    f"{counters['source_rows']}"
                )

            classification = (
                row["classification"].strip().upper()
            )

            if classification not in CLASSES:
                counters["excluded_other_class"] += 1
                continue

            timeframe = normalize_timeframe(
                row["timeframe"]
            )

            if timeframe not in TIMEFRAME_MS:
                raise ValueError(
                    f"Invalid timeframe: {timeframe}"
                )

            horizon = int(row["horizon"])

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Invalid horizon: {horizon}"
                )

            market = row["market"].strip().upper()

            if not market:
                raise ValueError("Missing market")

            start_ms = parse_time(
                row["entry_timestamp"]
            )

            if start_ms is None:
                raise ValueError(
                    "Missing completed entry timestamp "
                    f"at row {counters['source_rows']}"
                )

            end_ms = (
                start_ms
                + TIMEFRAME_MS[timeframe] * horizon
            )

            split = classify_split(
                start_ms,
                end_ms,
            )

            if split is None:
                counters["excluded_split_boundary"] += 1
                continue

            batch.append((
                split,
                market,
                timeframe,
                horizon,
                classification,
                start_ms,
                end_ms,
                hit,
            ))

            counters["eligible_rows"] += 1

            if len(batch) >= 10000:
                conn.executemany(
                    """
                    INSERT INTO events
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )

                conn.commit()
                batch.clear()

            if counters["source_rows"] % 500000 == 0:
                print(
                    "READ:",
                    f"{counters['source_rows']:,}",
                    flush=True,
                )

    if batch:
        conn.executemany(
            """
            INSERT INTO events
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )

        conn.commit()

    if counters["eligible_rows"] == 0:
        raise ValueError(
            "No eligible observations"
        )

    conn.execute(
        """
        CREATE INDEX idx_event_order
        ON events (
            split,
            timeframe,
            horizon,
            classification,
            market,
            start_ms,
            end_ms
        )
        """
    )

    conn.commit()

    return conn, dict(counters)


def analyze(conn):
    stats = defaultdict(
        lambda: {
            "source": 0,
            "selected": 0,
            "excluded": 0,
            "hits": 0,
            "markets": set(),
        }
    )

    cursor = conn.execute(
        """
        SELECT
            split,
            market,
            timeframe,
            horizon,
            classification,
            start_ms,
            end_ms,
            hit
        FROM events
        ORDER BY
            split,
            timeframe,
            horizon,
            classification,
            market,
            start_ms,
            end_ms,
            hit
        """
    )

    previous_group = None
    selected_end = None

    for (
        split,
        market,
        timeframe,
        horizon,
        classification,
        start_ms,
        end_ms,
        hit,
    ) in cursor:

        group = (
            split,
            timeframe,
            horizon,
            classification,
            market,
        )

        key = (
            split,
            timeframe,
            horizon,
            classification,
        )

        item = stats[key]
        item["source"] += 1

        if group != previous_group:
            previous_group = group
            selected_end = None

        # Deterministic earliest-start greedy selection.
        # An event is selected only when it does not
        # overlap the previously selected interval.
        #
        # The selection uses timestamps, not hit outcomes.
        if (
            selected_end is None
            or start_ms >= selected_end
        ):
            item["selected"] += 1
            item["hits"] += hit
            item["markets"].add(market)
            selected_end = end_ms

        else:
            item["excluded"] += 1

    rows = []

    for split in ("TRAIN", "TEST"):
        for timeframe in ("h1", "h4", "d1"):
            for horizon in HORIZONS:
                for classification in CLASSES:
                    key = (
                        split,
                        timeframe,
                        horizon,
                        classification,
                    )

                    item = stats[key]
                    selected = item["selected"]

                    rate = (
                        100.0 * item["hits"] / selected
                        if selected
                        else 0.0
                    )

                    rows.append({
                        "split": split,
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "classification": classification,
                        "source_events": item["source"],
                        "selected_events": selected,
                        "excluded_overlap": item["excluded"],
                        "hits": item["hits"],
                        "hit_rate_pct": f"{rate:.8f}",
                        "unique_markets": len(
                            item["markets"]
                        ),
                        "research_decision": "HOLD",
                    })

    return rows


def summarize_pairs(rows):
    lookup = {
        (
            row["split"],
            row["timeframe"],
            int(row["horizon"]),
            row["classification"],
        ): row
        for row in rows
    }

    comparisons = []

    for split in ("TRAIN", "TEST"):
        for timeframe in ("h1", "h4", "d1"):
            for horizon in HORIZONS:
                base = lookup[
                    (
                        split,
                        timeframe,
                        horizon,
                        "PATTERN_256_ONLY",
                    )
                ]

                both = lookup[
                    (
                        split,
                        timeframe,
                        horizon,
                        "BOTH",
                    )
                ]

                base_n = int(base["selected_events"])
                both_n = int(both["selected_events"])

                if base_n and both_n:
                    difference = (
                        float(both["hit_rate_pct"])
                        - float(base["hit_rate_pct"])
                    )

                    if difference > 0:
                        direction = "BOTH_HIGHER"
                    elif difference < 0:
                        direction = "BOTH_LOWER"
                    else:
                        direction = "EQUAL"

                else:
                    difference = None
                    direction = "INSUFFICIENT_GROUP"

                comparisons.append({
                    "split": split,
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "base_events": base_n,
                    "both_events": both_n,
                    "difference_pp": difference,
                    "direction": direction,
                })

    return comparisons


def verify_existing(output_dir, source_hash):
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
        result = json.loads(
            result_path.read_text(
                encoding="utf-8"
            )
        )

        manifest = json.loads(
            manifest_path.read_text(
                encoding="utf-8"
            )
        )

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        for field in (
            "statistical_independence_proven",
            "independent_oos_proven",
            "profitability_proven",
            "approved_for_live_trading",
        ):
            if result.get(field) is not False:
                return False

        if manifest.get("version") != VERSION:
            return False

        if manifest.get(
            "input_sha256", {}
        ).get("backtest_trades.csv") != source_hash:
            return False

        outputs = manifest.get(
            "output_sha256", {}
        )

        if outputs.get(CSV_NAME) != sha256(csv_path):
            return False

        if outputs.get(RESULT_NAME) != sha256(result_path):
            return False

        with csv_path.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)

            if reader.fieldnames != CSV_COLUMNS:
                return False

            rows = list(reader)

        if len(rows) != 36:
            return False

        if result.get("comparison_rows") != 36:
            return False

        if sum(
            int(row["source_events"])
            for row in rows
        ) != result.get("eligible_rows"):
            return False

        if sum(
            int(row["selected_events"])
            for row in rows
        ) != result.get("selected_events"):
            return False

        if sum(
            int(row["excluded_overlap"])
            for row in rows
        ) != result.get("excluded_overlap"):
            return False

        if sum(
            int(row["hits"])
            for row in rows
        ) != result.get("selected_hits"):
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

    source = root / SOURCE_REL
    output_dir = root / OUTPUT_REL

    if not source.is_file():
        raise FileNotFoundError(
            f"Missing source: {source}"
        )

    source_hash = sha256(source)

    if (
        not args.force
        and verify_existing(
            output_dir,
            source_hash,
        )
    ):
        print(
            "PASS: existing output verified"
        )

        print(
            "SKIP: completed analysis"
        )

        return 0

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    with tempfile.TemporaryDirectory(
        prefix="pattern256_nonoverlap_"
    ) as temporary:

        db_path = (
            Path(temporary)
            / "events.sqlite"
        )

        conn = None

        try:
            conn, counters = create_database(
                source,
                db_path,
            )

            rows = analyze(conn)

        finally:
            if conn is not None:
                conn.close()

    if len(rows) != 36:
        raise ValueError(
            "Unexpected comparison row count"
        )

    selected = sum(
        int(row["selected_events"])
        for row in rows
    )

    excluded = sum(
        int(row["excluded_overlap"])
        for row in rows
    )

    hits = sum(
        int(row["hits"])
        for row in rows
    )

    if (
        selected + excluded
        != counters["eligible_rows"]
    ):
        raise ValueError(
            "Event accounting mismatch"
        )

    pair_comparisons = summarize_pairs(rows)

    direction_counts = defaultdict(int)

    for item in pair_comparisons:
        direction_counts[
            item["direction"]
        ] += 1

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": counters["source_rows"],
        "eligible_rows": counters["eligible_rows"],
        "selected_events": selected,
        "selected_hits": hits,
        "excluded_overlap": excluded,
        "excluded_incomplete": counters.get(
            "excluded_incomplete",
            0,
        ),
        "excluded_other_class": counters.get(
            "excluded_other_class",
            0,
        ),
        "excluded_split_boundary": counters.get(
            "excluded_split_boundary",
            0,
        ),
        "comparison_rows": len(rows),
        "pair_comparisons": pair_comparisons,
        "direction_counts": dict(direction_counts),
        "research_decision": "HOLD",
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "settings": {
            "train_cutoff": CUTOFF,
            "test_end": TEST_END,
            "selection": (
                "Earliest-start nonoverlapping "
                "interval per market/timeframe/"
                "horizon/classification/split"
            ),
        },
        "limitations": [
            "De-overlap is performed within each classification.",
            "BOTH and PATTERN_256_ONLY may share market exposure.",
            "Same-market events across different horizons remain correlated.",
            "Different markets may share market-wide shocks.",
            "Theoretical event windows are used.",
            "Selection may change the population being compared.",
            "No matched causal comparison is established.",
            "No independent prospective OOS proof.",
            "No trading cost simulation.",
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

    write_csv(
        csv_path,
        rows,
    )

    write_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "input_sha256": {
            "backtest_trades.csv": source_hash,
        },
        "output_sha256": {
            CSV_NAME: sha256(csv_path),
            RESULT_NAME: sha256(result_path),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    if not verify_existing(
        output_dir,
        source_hash,
    ):
        raise RuntimeError(
            "Final integrity verification failed"
        )

    print("=" * 60)
    print("PATTERN 256 NON-OVERLAP RETEST")
    print("=" * 60)
    print(f"Execution: PASS")
    print(f"Version: {VERSION}")
    print(
        "Source rows:",
        f"{counters['source_rows']:,}",
    )
    print(
        "Eligible rows:",
        f"{counters['eligible_rows']:,}",
    )
    print(
        "Selected events:",
        f"{selected:,}",
    )
    print(
        "Excluded overlap:",
        f"{excluded:,}",
    )
    print(
        "Selected hits:",
        f"{hits:,}",
    )
    print(
        "Comparison rows:",
        len(rows),
    )
    print(
        "Pair comparisons:",
        len(pair_comparisons),
    )
    print(
        "Direction counts:",
        dict(direction_counts),
    )
    print("Research decision: HOLD")
    print("Statistical independence proven: False")
    print("Independent OOS proven: False")
    print("Profitability proven: False")
    print("Live trading approved: False")
    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")
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
