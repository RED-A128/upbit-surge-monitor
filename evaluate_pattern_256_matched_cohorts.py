
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-6
Exact matched-cohort diagnostic for Pattern 256.

Match key:
  timeframe, market, signal_timestamp, horizon

Only genuinely identical signal keys are matched.
Unmatched events are never artificially paired.

Reads the original independent-backtest trade CSV.
Uses SQLite to avoid holding millions of rows in RAM.

Research only. No live trading.
"""

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = "Clean V003-6"

SOURCE_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

SOURCE_REL = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_REL = "data/reports/pattern_256_matched_cohorts"

CANDIDATES = {"h1": 1, "h4": 4}
HORIZON = 24

BASELINE_NAMES = {"PATTERN_256_ONLY"}

COMBINED_NAMES = {
    "BOTH",
    "PATTERN_256_AND_ACCUMULATION",
    "PATTERN_256_PLUS_ACCUMULATION",
    "PATTERN_256_WITH_ACCUMULATION",
    "PATTERN_256_ACCUMULATION",
}

PERIODS = {
    2025: (
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    ),
    2026: (
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 9, 1, tzinfo=timezone.utc),
    ),
}

FIELDS = [
    "timeframe",
    "test_year",
    "baseline_events",
    "combined_events",
    "matched_pairs",
    "baseline_only_events",
    "combined_only_events",
    "matched_baseline_hits",
    "matched_combined_hits",
    "matched_baseline_rate_pct",
    "matched_combined_rate_pct",
    "matched_difference_pp",
    "both_hit",
    "baseline_only_hit",
    "combined_only_hit",
    "neither_hit",
    "outcome_discordance",
    "decision",
]


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            h.update(chunk)
    return h.hexdigest()


def atomic_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def atomic_csv(path, rows):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def parse_timestamp(value):
    if not value:
        raise ValueError("Missing timestamp")
    parsed = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        raise ValueError("Timezone missing: " + value)
    return parsed.astimezone(timezone.utc)


def classify(value):
    if value in BASELINE_NAMES:
        return "BASELINE"
    if value in COMBINED_NAMES:
        return "COMBINED"
    return None


def rate(hits, total):
    if not total:
        return None
    return round(100 * hits / total, 6)


def create_database(path):
    connection = sqlite3.connect(str(path))
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")

    connection.execute(
        """
        CREATE TABLE events (
            timeframe TEXT NOT NULL,
            test_year INTEGER NOT NULL,
            market TEXT NOT NULL,
            signal_timestamp TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            cohort TEXT NOT NULL,
            hit INTEGER NOT NULL,
            PRIMARY KEY (
                timeframe,
                test_year,
                market,
                signal_timestamp,
                horizon,
                cohort
            )
        )
        """
    )
    return connection


def ingest(source, connection):
    total = 0
    selected = 0
    skipped = 0
    classes_seen = set()
    selected_classes = set()

    batch = []

    required = {
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
    }

    with source.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.DictReader(handle)

        if not required.issubset(reader.fieldnames or []):
            raise ValueError(
                "Missing source columns: "
                + str(sorted(required - set(reader.fieldnames or [])))
            )

        for row in reader:
            total += 1

            tf = row["timeframe"].strip()
            if tf not in CANDIDATES:
                continue

            try:
                horizon = int(row["horizon"])
            except ValueError as exc:
                raise ValueError(
                    "Invalid horizon at row " + str(total)
                ) from exc

            if horizon != HORIZON:
                continue

            classification = row["classification"].strip()
            classes_seen.add(classification)

            cohort = classify(classification)
            if cohort is None:
                continue

            selected_classes.add(classification)

            status = row["status"].strip()
            if status not in ("HIT", "MISS"):
                skipped += 1
                continue

            if (
                int(row["observed_bars"]) < HORIZON
                or int(row["required_bars"]) != HORIZON
            ):
                skipped += 1
                continue

            hit = int(row["hit_30pct"])
            if hit not in (0, 1):
                raise ValueError("Invalid hit flag")

            if (status == "HIT") != (hit == 1):
                raise ValueError(
                    "Status/hit mismatch at row " + str(total)
                )

            signal = parse_timestamp(row["signal_timestamp"])
            entry = parse_timestamp(row["entry_timestamp"])

            if entry < signal:
                raise ValueError(
                    "Entry precedes signal at row " + str(total)
                )

            # Conservative maturity rule:
            # the entire evaluation horizon must fit the period.
            maturity = entry + timedelta(
                hours=HORIZON * CANDIDATES[tf]
            )

            year = None
            for test_year, (start, end) in PERIODS.items():
                if start <= signal < end and maturity <= end:
                    year = test_year
                    break

            if year is None:
                skipped += 1
                continue

            market = row["market"].strip()
            if not market.startswith("KRW-"):
                raise ValueError("Unexpected market: " + market)

            batch.append(
                (
                    tf,
                    year,
                    market,
                    signal.isoformat(),
                    horizon,
                    cohort,
                    hit,
                )
            )

            if len(batch) >= 10000:
                try:
                    with connection:
                        connection.executemany(
                            "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                            batch,
                        )
                except sqlite3.IntegrityError as exc:
                    raise ValueError(
                        "Duplicate exact signal/cohort key; "
                        "cannot safely match"
                    ) from exc

                selected += len(batch)
                batch.clear()

            if total % 500000 == 0:
                print(
                    f"[PROGRESS] source_rows={total:,}",
                    flush=True,
                )

    if batch:
        try:
            with connection:
                connection.executemany(
                    "INSERT INTO events VALUES (?,?,?,?,?,?,?)",
                    batch,
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError(
                "Duplicate exact signal/cohort key"
            ) from exc
        selected += len(batch)

    if not selected:
        raise ValueError(
            "No eligible events. "
            "Observed classifications: "
            + str(sorted(classes_seen))
        )

    print(
        "[INFO] Selected classifications:",
        sorted(selected_classes),
        flush=True,
    )

    return {
        "source_rows": total,
        "eligible_rows": selected,
        "excluded_incomplete_or_outside_period": skipped,
        "observed_classifications": sorted(classes_seen),
    }


def evaluate(connection):
    results = []

    for tf in CANDIDATES:
        for year in PERIODS:
            counts = dict(
                connection.execute(
                    """
                    SELECT cohort, COUNT(*)
                    FROM events
                    WHERE timeframe=? AND test_year=?
                    GROUP BY cohort
                    """,
                    (tf, year),
                ).fetchall()
            )

            baseline_count = counts.get("BASELINE", 0)
            combined_count = counts.get("COMBINED", 0)

            pair_stats = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(b.hit),0),
                    COALESCE(SUM(c.hit),0),
                    COALESCE(SUM(
                        CASE WHEN b.hit=1 AND c.hit=1
                        THEN 1 ELSE 0 END
                    ),0),
                    COALESCE(SUM(
                        CASE WHEN b.hit=1 AND c.hit=0
                        THEN 1 ELSE 0 END
                    ),0),
                    COALESCE(SUM(
                        CASE WHEN b.hit=0 AND c.hit=1
                        THEN 1 ELSE 0 END
                    ),0),
                    COALESCE(SUM(
                        CASE WHEN b.hit=0 AND c.hit=0
                        THEN 1 ELSE 0 END
                    ),0)
                FROM events b
                INNER JOIN events c
                  ON b.timeframe=c.timeframe
                 AND b.test_year=c.test_year
                 AND b.market=c.market
                 AND b.signal_timestamp=c.signal_timestamp
                 AND b.horizon=c.horizon
                WHERE b.cohort='BASELINE'
                  AND c.cohort='COMBINED'
                  AND b.timeframe=?
                  AND b.test_year=?
                """,
                (tf, year),
            ).fetchone()

            (
                matched,
                baseline_hits,
                combined_hits,
                both_hit,
                baseline_only_hit,
                combined_only_hit,
                neither_hit,
            ) = pair_stats

            if (
                matched > baseline_count
                or matched > combined_count
                or sum(
                    (
                        both_hit,
                        baseline_only_hit,
                        combined_only_hit,
                        neither_hit,
                    )
                ) != matched
            ):
                raise ValueError("Matched-count inconsistency")

            br = rate(baseline_hits, matched)
            cr = rate(combined_hits, matched)

            difference = (
                round(cr - br, 6)
                if br is not None and cr is not None
                else None
            )

            results.append(
                {
                    "timeframe": tf,
                    "test_year": year,
                    "baseline_events": baseline_count,
                    "combined_events": combined_count,
                    "matched_pairs": matched,
                    "baseline_only_events": baseline_count - matched,
                    "combined_only_events": combined_count - matched,
                    "matched_baseline_hits": baseline_hits,
                    "matched_combined_hits": combined_hits,
                    "matched_baseline_rate_pct": br,
                    "matched_combined_rate_pct": cr,
                    "matched_difference_pp": difference,
                    "both_hit": both_hit,
                    "baseline_only_hit": baseline_only_hit,
                    "combined_only_hit": combined_only_hit,
                    "neither_hit": neither_hit,
                    "outcome_discordance": (
                        baseline_only_hit + combined_only_hit
                    ),
                    "decision": "HOLD",
                }
            )

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = (root / SOURCE_REL).resolve()
    output = (root / OUTPUT_REL).resolve()

    if not source.is_file():
        raise FileNotFoundError(source)

    if (
        output == source.parent
        or output in source.parents
        or source.parent in output.parents
    ):
        raise ValueError("Unsafe output location")

    print("[START]", VERSION, flush=True)

    source_hash = sha256(source)
    if source_hash != SOURCE_SHA256:
        raise ValueError(
            "Source SHA256 mismatch; "
            "do not silently analyze changed data"
        )

    output.mkdir(parents=True, exist_ok=True)
    database = output / "matched_cohorts_work.sqlite"

    # Never reuse a possibly incomplete SQLite database.
    if database.exists():
        database.unlink()

    connection = None

    try:
        connection = create_database(database)
        ingestion = ingest(source, connection)

        connection.execute(
            """
            CREATE INDEX idx_matching
            ON events (
                timeframe,
                test_year,
                cohort,
                market,
                signal_timestamp,
                horizon
            )
            """
        )

        summaries = evaluate(connection)

        # Verify original input has not changed during processing.
        if sha256(source) != SOURCE_SHA256:
            raise ValueError("Source changed during analysis")

        summary_path = output / "matched_cohorts_summary.csv"
        result_path = output / "matched_cohorts_result.json"
        manifest_path = output / "matched_cohorts_manifest.json"

        atomic_csv(summary_path, summaries)

        result = {
            "version": VERSION,
            "execution_status": "PASS",
            "source_sha256": SOURCE_SHA256,
            **ingestion,
            "summary_rows": len(summaries),
            "summary": summaries,
            "match_key": [
                "timeframe",
                "market",
                "signal_timestamp",
                "horizon",
            ],
            "limitations": [
                "Different signal cohorts may have no exact matches.",
                "No artificial or nearest-neighbor matching is used.",
                "Signal outcomes may be identical by construction.",
                "Overlapping signal windows remain dependent.",
                "Prior candidate selection used test data.",
                "No causal improvement or profitability proof.",
            ],
            "independent_oos_proven": False,
            "causal_improvement_proven": False,
            "profitability_proven": False,
            "approved_for_live_trading": False,
            "overall_decision": "HOLD",
        }

        atomic_json(result_path, result)

        hashes = {
            path.name: sha256(path)
            for path in (summary_path, result_path)
        }

        atomic_json(
            manifest_path,
            {
                "version": VERSION,
                "source_sha256": SOURCE_SHA256,
                "output_sha256": hashes,
                "summary_rows": len(summaries),
            },
        )

        for filename, expected in hashes.items():
            if sha256(output / filename) != expected:
                raise ValueError("Output integrity mismatch")

        print(
            "[PASS] Source rows:",
            ingestion["source_rows"],
            flush=True,
        )

        for item in summaries:
            print(
                "[RESULT]",
                item["timeframe"],
                item["test_year"],
                "matched=",
                item["matched_pairs"],
                "difference_pp=",
                item["matched_difference_pp"],
                flush=True,
            )

        print("[PASS] SHA256 integrity", flush=True)
        print("[DECISION] HOLD", flush=True)
        return 0

    finally:
        if connection is not None:
            connection.close()
        if database.exists():
            database.unlink()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("[INTERRUPTED]", file=sys.stderr, flush=True)
        sys.exit(130)
    except Exception as exc:
        print("[FAIL]", str(exc), file=sys.stderr, flush=True)
        sys.exit(1)
