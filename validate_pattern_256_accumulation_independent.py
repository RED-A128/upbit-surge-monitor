
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_independent.py
Clean V001

Independent-signal and chronological-split validation.

Input:
    data/reports/pattern_256_accumulation_30pct/
        validation_detail.csv
        validation_result.json

Output:
    data/reports/pattern_256_accumulation_independent/
        independent_detail.csv
        independent_summary.csv
        independent_comparison.csv
        independent_result.json
        independent_manifest.json
        checkpoints/
        working.sqlite3

Safety:
    - Source data is read-only.
    - No trading or exchange API calls.
    - No Git operations.
    - SQLite disk-backed processing.
    - Source SHA256 verification.
    - Checkpoint/resume.
    - Atomic output replacement.
    - Future outcomes used only for validation.

Important:
    This is a historical robustness check, not a genuinely
    unseen out-of-sample test.

Run:
    py validate_pattern_256_accumulation_independent.py

Restart:
    Run the same command again.

Force rebuild:
    py validate_pattern_256_accumulation_independent.py --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import sys
import tempfile
import time
import traceback

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROGRAM = "validate_pattern_256_accumulation_independent.py"
VERSION = "Clean V001"

ROOT = Path(__file__).resolve().parent

SOURCE_DIR = (
    ROOT
    / "data"
    / "reports"
    / "pattern_256_accumulation_30pct"
)

SOURCE_DETAIL = SOURCE_DIR / "validation_detail.csv"
SOURCE_RESULT = SOURCE_DIR / "validation_result.json"

OUTPUT_DIR = (
    ROOT
    / "data"
    / "reports"
    / "pattern_256_accumulation_independent"
)

OUTPUT_DETAIL = OUTPUT_DIR / "independent_detail.csv"
OUTPUT_SUMMARY = OUTPUT_DIR / "independent_summary.csv"
OUTPUT_COMPARISON = OUTPUT_DIR / "independent_comparison.csv"
OUTPUT_RESULT = OUTPUT_DIR / "independent_result.json"
OUTPUT_MANIFEST = OUTPUT_DIR / "independent_manifest.json"

CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"
DATABASE_FILE = OUTPUT_DIR / "working.sqlite3"
DATABASE_META = OUTPUT_DIR / "database_checkpoint.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

CLASSIFICATIONS = (
    "PATTERN_256_ONLY",
    "ACCUMULATION_ONLY",
    "BOTH",
    "NEITHER",
)

SECONDS_PER_CANDLE = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TRAIN_FRACTION = 0.80

REQUIRED_COLUMNS = {
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "observed_bars",
    "required_bars",
    "source_ohlcv_sha256",
}

DETAIL_COLUMNS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "classification",
    "horizon",
    "period",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "observed_bars",
    "required_bars",
    "entry_price",
    "signal_epoch",
    "entry_epoch",
    "evaluation_end_epoch",
]

SUMMARY_COLUMNS = [
    "timeframe",
    "classification",
    "horizon",
    "period",
    "selected_signals",
    "hit",
    "miss",
    "censored",
    "invalid",
    "complete",
    "hit_rate_pct",
    "wilson_low_pct",
    "wilson_high_pct",
]

COMPARISON_COLUMNS = [
    "timeframe",
    "horizon",
    "period",
    "pattern_only_complete",
    "pattern_only_hit",
    "pattern_only_hit_rate_pct",
    "both_complete",
    "both_hit",
    "both_hit_rate_pct",
    "difference_percentage_points",
    "assessment",
]

BATCH_SIZE = 10000


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def parse_timestamp(value: str) -> int:
    text = str(value).strip()

    if not text:
        raise ValueError("Empty timestamp")

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    parsed = datetime.fromisoformat(text)

    if parsed.tzinfo is None:
        raise ValueError(
            f"Timezone missing from timestamp: {value}"
        )

    return int(
        parsed.astimezone(timezone.utc).timestamp()
    )


def parse_float(value: str) -> float:
    number = float(value)

    if not math.isfinite(number):
        raise ValueError(
            f"Non-finite numeric value: {value}"
        )

    return number


def parse_int(value: str) -> int:
    return int(value)


def optional_int(value: str) -> int | None:
    text = str(value or "").strip()

    if not text:
        return None

    return int(text)


def iso_from_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(
        epoch,
        tz=timezone.utc,
    ).isoformat()


def wilson_interval(
    hit: int,
    total: int,
    z: float = 1.96,
) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None

    p = hit / total
    z2 = z * z

    denominator = 1.0 + z2 / total

    center = (
        p + z2 / (2.0 * total)
    ) / denominator

    margin = (
        z
        * math.sqrt(
            p * (1.0 - p) / total
            + z2 / (4.0 * total * total)
        )
        / denominator
    )

    return (
        max(0.0, center - margin) * 100.0,
        min(1.0, center + margin) * 100.0,
    )


def percentage(
    numerator: int,
    denominator: int,
) -> float | None:
    if denominator <= 0:
        return None

    return (
        100.0 * numerator / denominator
    )


def rounded(value: float | None) -> str | float:
    if value is None:
        return ""

    return round(value, 6)


def verify_sources() -> tuple[str, str, dict]:
    if not SOURCE_DETAIL.is_file():
        raise FileNotFoundError(SOURCE_DETAIL)

    if not SOURCE_RESULT.is_file():
        raise FileNotFoundError(SOURCE_RESULT)

    result = read_json(SOURCE_RESULT)

    if result.get("status") != "PASS":
        raise ValueError(
            "Source validation status is not PASS"
        )

    if result.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected source validation version"
        )

    if result.get("limited_test") is not False:
        raise ValueError(
            "Source is not a full-market production result"
        )

    if float(result.get("target_return_pct", 0)) != 30.0:
        raise ValueError(
            "Unexpected target return"
        )

    if result.get("entry_rule") != "NEXT_CANDLE_OPEN":
        raise ValueError(
            "Unexpected source entry rule"
        )

    detail_hash = sha256_file(SOURCE_DETAIL)
    result_hash = sha256_file(SOURCE_RESULT)

    expected = result.get("detail_sha256")

    if not isinstance(expected, str):
        raise ValueError(
            "Source detail SHA256 missing"
        )

    if detail_hash.lower() != expected.lower():
        raise ValueError(
            "Source detail SHA256 mismatch"
        )

    with SOURCE_DETAIL.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.reader(handle)
        header = next(reader, [])

    missing = REQUIRED_COLUMNS - set(header)

    if missing:
        raise ValueError(
            f"Source columns missing: {sorted(missing)}"
        )

    log("[PASS] Source SHA256 verified")

    return detail_hash, result_hash, result


def connect_database() -> sqlite3.Connection:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = sqlite3.connect(
        str(DATABASE_FILE),
        timeout=120,
    )

    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=120000")

    return connection


def create_database_schema(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            signal_epoch INTEGER NOT NULL,
            signal_timestamp TEXT NOT NULL,
            entry_epoch INTEGER,
            entry_timestamp TEXT,
            classification TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            status TEXT NOT NULL,
            hit INTEGER,
            first_hit_bars INTEGER,
            observed_bars INTEGER NOT NULL,
            required_bars INTEGER NOT NULL,
            entry_price REAL,
            source_hash TEXT NOT NULL,

            PRIMARY KEY (
                timeframe,
                market,
                signal_epoch,
                horizon
            )
        )
        """
    )

    connection.commit()


def reset_database(
    connection: sqlite3.Connection,
) -> None:
    connection.execute(
        "DROP TABLE IF EXISTS signals"
    )

    connection.commit()

    create_database_schema(connection)


def import_checkpoint_valid(
    connection: sqlite3.Connection,
    source_hash: str,
    result_hash: str,
) -> bool:
    if not DATABASE_META.is_file():
        return False

    try:
        metadata = read_json(DATABASE_META)

        if metadata.get("version") != VERSION:
            return False

        if metadata.get("source_sha256") != source_hash:
            return False

        if metadata.get("result_sha256") != result_hash:
            return False

        if metadata.get("status") != "COMPLETE":
            return False

        actual = connection.execute(
            "SELECT COUNT(*) FROM signals"
        ).fetchone()[0]

        return actual == metadata.get("row_count")

    except (
        OSError,
        ValueError,
        KeyError,
        sqlite3.Error,
        json.JSONDecodeError,
    ):
        return False


INSERT_SQL = """
INSERT INTO signals (
    timeframe,
    market,
    signal_epoch,
    signal_timestamp,
    entry_epoch,
    entry_timestamp,
    classification,
    horizon,
    status,
    hit,
    first_hit_bars,
    observed_bars,
    required_bars,
    entry_price,
    source_hash
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def convert_source_row(
    row: dict,
) -> tuple:
    timeframe = str(
        row["timeframe"]
    ).strip().lower()

    market = str(
        row["market"]
    ).strip().upper()

    classification = str(
        row["classification"]
    ).strip().upper()

    horizon = parse_int(
        row["horizon"]
    )

    status = str(
        row["status"]
    ).strip().upper()

    if timeframe not in TIMEFRAMES:
        raise ValueError(
            f"Unsupported timeframe: {timeframe}"
        )

    if not market.startswith("KRW-"):
        raise ValueError(
            f"Invalid market: {market}"
        )

    if classification not in CLASSIFICATIONS:
        raise ValueError(
            f"Invalid classification: {classification}"
        )

    if horizon not in HORIZONS:
        raise ValueError(
            f"Invalid horizon: {horizon}"
        )

    if status not in {
        "HIT",
        "MISS",
        "CENSORED",
        "INVALID",
    }:
        raise ValueError(
            f"Invalid status: {status}"
        )

    signal_epoch = parse_timestamp(
        row["signal_timestamp"]
    )

    entry_text = str(
        row.get("entry_timestamp", "")
    ).strip()

    entry_epoch = (
        parse_timestamp(entry_text)
        if entry_text
        else None
    )

    entry_price_text = str(
        row.get("entry_price", "")
    ).strip()

    entry_price = (
        parse_float(entry_price_text)
        if entry_price_text
        else None
    )

    observed = parse_int(
        row["observed_bars"]
    )

    required = parse_int(
        row["required_bars"]
    )

    if observed < 0 or required != horizon:
        raise ValueError(
            "Invalid observed/required candle count"
        )

    if observed > required:
        raise ValueError(
            "Observed candles exceed horizon"
        )

    first_hit = optional_int(
        row.get("first_hit_bars", "")
    )

    if status == "HIT":
        hit = 1

        if (
            first_hit is None
            or first_hit < 1
            or first_hit > observed
        ):
            raise ValueError(
                "Invalid first-hit candle position"
            )

    elif status == "MISS":
        hit = 0

        if observed != horizon:
            raise ValueError(
                "MISS requires complete observation window"
            )

    else:
        hit = None

    if status in {"HIT", "MISS"}:
        if (
            entry_epoch is None
            or entry_price is None
            or entry_price <= 0
        ):
            raise ValueError(
                "Completed signal has invalid entry"
            )

    source_hash = str(
        row["source_ohlcv_sha256"]
    ).strip().lower()

    if len(source_hash) != 64:
        raise ValueError(
            "Invalid OHLCV source hash"
        )

    return (
        timeframe,
        market,
        signal_epoch,
        str(row["signal_timestamp"]),
        entry_epoch,
        entry_text,
        classification,
        horizon,
        status,
        hit,
        first_hit,
        observed,
        required,
        entry_price,
        source_hash,
    )


def import_source(
    connection: sqlite3.Connection,
    detail_hash: str,
    result_hash: str,
    force: bool,
) -> int:
    create_database_schema(connection)

    if (
        not force
        and import_checkpoint_valid(
            connection,
            detail_hash,
            result_hash,
        )
    ):
        count = connection.execute(
            "SELECT COUNT(*) FROM signals"
        ).fetchone()[0]

        log(
            f"[SKIP] Verified database checkpoint: "
            f"{count:,} rows"
        )

        return count

    log("[IMPORT] Rebuilding SQLite source index")

    if DATABASE_META.exists():
        DATABASE_META.unlink()

    reset_database(connection)

    batch = []
    total = 0

    with SOURCE_DETAIL.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            batch.append(
                convert_source_row(row)
            )

            if len(batch) >= BATCH_SIZE:
                with connection:
                    connection.executemany(
                        INSERT_SQL,
                        batch,
                    )

                total += len(batch)
                batch.clear()

                if total % 100000 == 0:
                    log(
                        f"[IMPORT] {total:,} rows"
                    )

        if batch:
            with connection:
                connection.executemany(
                    INSERT_SQL,
                    batch,
                )

            total += len(batch)

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_signals_group_order
        ON signals (
            timeframe,
            market,
            horizon,
            signal_epoch
        )
        """
    )

    connection.commit()

    if total <= 0:
        raise ValueError(
            "Source contains no validation rows"
        )

    current_hash = sha256_file(
        SOURCE_DETAIL
    )

    if current_hash != detail_hash:
        raise RuntimeError(
            "Source detail changed during import"
        )

    actual = connection.execute(
        "SELECT COUNT(*) FROM signals"
    ).fetchone()[0]

    if actual != total:
        raise ValueError(
            "SQLite import row count mismatch"
        )

    atomic_json(
        DATABASE_META,
        {
            "program": PROGRAM,
            "version": VERSION,
            "status": "COMPLETE",
            "created_at_utc": utc_now(),
            "source_sha256": detail_hash,
            "result_sha256": result_hash,
            "row_count": total,
        },
    )

    log(
        f"[PASS] SQLite import: {total:,} rows"
    )

    return total


def get_split_boundaries(
    connection: sqlite3.Connection,
) -> dict:
    """
    Use chronological time-span split per timeframe.

    Split is based on signal timestamps, never
    on future outcomes or target-hit labels.
    """
    boundaries = {}

    for timeframe in TIMEFRAMES:
        minimum, maximum = connection.execute(
            """
            SELECT
                MIN(signal_epoch),
                MAX(signal_epoch)
            FROM signals
            WHERE timeframe = ?
            """,
            (timeframe,),
        ).fetchone()

        if minimum is None or maximum is None:
            raise ValueError(
                f"No signals for timeframe {timeframe}"
            )

        if maximum <= minimum:
            raise ValueError(
                f"Insufficient chronological range: {timeframe}"
            )

        cutoff = minimum + int(
            (maximum - minimum) * TRAIN_FRACTION
        )

        boundaries[timeframe] = {
            "minimum_epoch": minimum,
            "maximum_epoch": maximum,
            "cutoff_epoch": cutoff,
            "minimum_utc": iso_from_epoch(minimum),
            "maximum_utc": iso_from_epoch(maximum),
            "cutoff_utc": iso_from_epoch(cutoff),
        }

        log(
            f"[SPLIT] {timeframe}: "
            f"{boundaries[timeframe]['cutoff_utc']}"
        )

    return boundaries


def get_groups(
    connection: sqlite3.Connection,
) -> list[tuple[str, str, int]]:
    cursor = connection.execute(
        """
        SELECT DISTINCT
            timeframe,
            market,
            horizon
        FROM signals
        ORDER BY
            timeframe,
            market,
            horizon
        """
    )

    return [
        (str(tf), str(market), int(horizon))
        for tf, market, horizon in cursor
    ]


def group_file(
    timeframe: str,
    market: str,
    horizon: int,
) -> Path:
    safe_market = market.replace("/", "_")

    return (
        CHECKPOINT_DIR
        / timeframe
        / f"{safe_market}_{horizon}.csv"
    )


def group_metadata_file(
    timeframe: str,
    market: str,
    horizon: int,
) -> Path:
    safe_market = market.replace("/", "_")

    return (
        CHECKPOINT_DIR
        / timeframe
        / f"{safe_market}_{horizon}.json"
    )


def checkpoint_valid(
    csv_path: Path,
    metadata_path: Path,
    detail_hash: str,
    result_hash: str,
    cutoff: int,
) -> bool:
    if not csv_path.is_file():
        return False

    if not metadata_path.is_file():
        return False

    try:
        metadata = read_json(metadata_path)

        if metadata.get("version") != VERSION:
            return False

        if metadata.get("detail_sha256") != detail_hash:
            return False

        if metadata.get("result_sha256") != result_hash:
            return False

        if metadata.get("cutoff_epoch") != cutoff:
            return False

        if metadata.get("csv_sha256") != sha256_file(csv_path):
            return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if reader.fieldnames != DETAIL_COLUMNS:
                return False

            count = sum(1 for _ in reader)

        return count == metadata.get("selected_rows")

    except (
        OSError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
    ):
        return False


def process_group(
    connection: sqlite3.Connection,
    timeframe: str,
    market: str,
    horizon: int,
    cutoff: int,
    detail_hash: str,
    result_hash: str,
    force: bool,
) -> tuple[int, int, int]:
    """
    Greedy non-overlap selection.

    Each accepted signal blocks subsequent signals
    in the same market/timeframe/horizon until its
    assumed evaluation interval ends.

    The interval uses nominal candle duration.
    Missing OHLCV candles can make actual calendar
    duration longer; this limitation is reported.
    """
    csv_path = group_file(
        timeframe,
        market,
        horizon,
    )

    metadata_path = group_metadata_file(
        timeframe,
        market,
        horizon,
    )

    if not force and checkpoint_valid(
        csv_path,
        metadata_path,
        detail_hash,
        result_hash,
        cutoff,
    ):
        metadata = read_json(metadata_path)

        return (
            metadata["source_rows"],
            metadata["selected_rows"],
            metadata["excluded_rows"],
        )

    csv_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=csv_path.name + ".",
        suffix=".tmp",
        dir=str(csv_path.parent),
    )

    temporary = Path(temporary_name)

    duration = (
        horizon * SECONDS_PER_CANDLE[timeframe]
    )

    source_rows = 0
    selected_rows = 0
    excluded_rows = 0

    # The last accepted signal's evaluation end.
    blocked_until = None

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as output:
            writer = csv.DictWriter(
                output,
                fieldnames=DETAIL_COLUMNS,
            )

            writer.writeheader()

            cursor = connection.execute(
                """
                SELECT
                    signal_epoch,
                    signal_timestamp,
                    entry_epoch,
                    entry_timestamp,
                    classification,
                    status,
                    hit,
                    first_hit_bars,
                    observed_bars,
                    required_bars,
                    entry_price
                FROM signals
                WHERE
                    timeframe = ?
                    AND market = ?
                    AND horizon = ?
                ORDER BY
                    signal_epoch,
                    entry_epoch
                """,
                (timeframe, market, horizon),
            )

            for record in cursor:
                source_rows += 1

                (
                    signal_epoch,
                    signal_timestamp,
                    entry_epoch,
                    entry_timestamp,
                    classification,
                    status,
                    hit,
                    first_hit_bars,
                    observed_bars,
                    required_bars,
                    entry_price,
                ) = record

                # No entry candle means the signal cannot
                # be evaluated as an executable entry.
                if entry_epoch is None:
                    excluded_rows += 1
                    continue

                evaluation_end = (
                    entry_epoch + duration
                )

                # Select non-overlapping signals before
                # assessing their historical outcomes.
                if (
                    blocked_until is not None
                    and signal_epoch < blocked_until
                ):
                    excluded_rows += 1
                    continue

                blocked_until = evaluation_end

                # Train labels cannot use an evaluation
                # window crossing the split boundary.
                if signal_epoch < cutoff:
                    if evaluation_end > cutoff:
                        period = "BOUNDARY_EXCLUDED"
                    else:
                        period = "TRAIN"
                else:
                    period = "TEST"

                writer.writerow({
                    "timeframe": timeframe,
                    "market": market,
                    "signal_timestamp": signal_timestamp,
                    "entry_timestamp": entry_timestamp,
                    "classification": classification,
                    "horizon": horizon,
                    "period": period,
                    "status": status,
                    "hit_30pct": (
                        hit if hit is not None else ""
                    ),
                    "first_hit_bars": (
                        first_hit_bars
                        if first_hit_bars is not None
                        else ""
                    ),
                    "observed_bars": observed_bars,
                    "required_bars": required_bars,
                    "entry_price": (
                        entry_price
                        if entry_price is not None
                        else ""
                    ),
                    "signal_epoch": signal_epoch,
                    "entry_epoch": entry_epoch,
                    "evaluation_end_epoch": evaluation_end,
                })

                selected_rows += 1

        os.replace(temporary, csv_path)

    finally:
        if temporary.exists():
            temporary.unlink()

    atomic_json(
        metadata_path,
        {
            "program": PROGRAM,
            "version": VERSION,
            "created_at_utc": utc_now(),
            "timeframe": timeframe,
            "market": market,
            "horizon": horizon,
            "detail_sha256": detail_hash,
            "result_sha256": result_hash,
            "cutoff_epoch": cutoff,
            "source_rows": source_rows,
            "selected_rows": selected_rows,
            "excluded_rows": excluded_rows,
            "csv_sha256": sha256_file(csv_path),
        },
    )

    return (
        source_rows,
        selected_rows,
        excluded_rows,
    )


def combine_detail(
    groups: list[tuple[str, str, int]],
) -> int:
    fd, temporary_name = tempfile.mkstemp(
        prefix=OUTPUT_DETAIL.name + ".",
        suffix=".tmp",
        dir=str(OUTPUT_DIR),
    )

    temporary = Path(temporary_name)
    total = 0

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as output:
            writer = csv.DictWriter(
                output,
                fieldnames=DETAIL_COLUMNS,
            )

            writer.writeheader()

            for timeframe, market, horizon in groups:
                path = group_file(
                    timeframe,
                    market,
                    horizon,
                )

                with path.open(
                    "r",
                    encoding="utf-8-sig",
                    newline="",
                ) as handle:
                    reader = csv.DictReader(handle)

                    if reader.fieldnames != DETAIL_COLUMNS:
                        raise ValueError(
                            f"Checkpoint schema mismatch: {path}"
                        )

                    for row in reader:
                        writer.writerow(row)
                        total += 1

        os.replace(
            temporary,
            OUTPUT_DETAIL,
        )

    finally:
        if temporary.exists():
            temporary.unlink()

    return total


def calculate_summary() -> list[dict]:
    counters = defaultdict(
        lambda: {
            "selected": 0,
            "hit": 0,
            "miss": 0,
            "censored": 0,
            "invalid": 0,
        }
    )

    with OUTPUT_DETAIL.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            key = (
                row["timeframe"],
                row["classification"],
                int(row["horizon"]),
                row["period"],
            )

            counter = counters[key]
            counter["selected"] += 1

            status = row["status"]

            if status == "HIT":
                counter["hit"] += 1

            elif status == "MISS":
                counter["miss"] += 1

            elif status == "CENSORED":
                counter["censored"] += 1

            elif status == "INVALID":
                counter["invalid"] += 1

            else:
                raise ValueError(
                    f"Unexpected status: {status}"
                )

    rows = []

    for timeframe in TIMEFRAMES:
        for classification in CLASSIFICATIONS:
            for horizon in HORIZONS:
                for period in (
                    "TRAIN",
                    "TEST",
                    "BOUNDARY_EXCLUDED",
                ):
                    key = (
                        timeframe,
                        classification,
                        horizon,
                        period,
                    )

                    counter = counters[key]

                    hit = counter["hit"]
                    miss = counter["miss"]
                    complete = hit + miss

                    low, high = wilson_interval(
                        hit,
                        complete,
                    )

                    rows.append({
                        "timeframe": timeframe,
                        "classification": classification,
                        "horizon": horizon,
                        "period": period,
                        "selected_signals": counter["selected"],
                        "hit": hit,
                        "miss": miss,
                        "censored": counter["censored"],
                        "invalid": counter["invalid"],
                        "complete": complete,
                        "hit_rate_pct": rounded(
                            percentage(hit, complete)
                        ),
                        "wilson_low_pct": rounded(low),
                        "wilson_high_pct": rounded(high),
                    })

    return rows


def calculate_comparison(
    summary: list[dict],
) -> list[dict]:
    lookup = {
        (
            row["timeframe"],
            row["classification"],
            row["horizon"],
            row["period"],
        ): row
        for row in summary
    }

    comparisons = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for period in ("TRAIN", "TEST"):
                only = lookup[
                    (
                        timeframe,
                        "PATTERN_256_ONLY",
                        horizon,
                        period,
                    )
                ]

                both = lookup[
                    (
                        timeframe,
                        "BOTH",
                        horizon,
                        period,
                    )
                ]

                only_complete = only["complete"]
                both_complete = both["complete"]

                only_rate = percentage(
                    only["hit"],
                    only_complete,
                )

                both_rate = percentage(
                    both["hit"],
                    both_complete,
                )

                if (
                    only_rate is None
                    or both_rate is None
                ):
                    difference = None
                    assessment = "NO_DATA"

                else:
                    difference = (
                        both_rate - only_rate
                    )

                    if (
                        only_complete < 100
                        or both_complete < 100
                    ):
                        assessment = "SMALL_SAMPLE"

                    elif difference > 0:
                        assessment = "BOTH_HIGHER_DESCRIPTIVE"

                    elif difference < 0:
                        assessment = "PATTERN_ONLY_HIGHER_DESCRIPTIVE"

                    else:
                        assessment = "EQUAL_DESCRIPTIVE"

                comparisons.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "period": period,
                    "pattern_only_complete": only_complete,
                    "pattern_only_hit": only["hit"],
                    "pattern_only_hit_rate_pct": rounded(
                        only_rate
                    ),
                    "both_complete": both_complete,
                    "both_hit": both["hit"],
                    "both_hit_rate_pct": rounded(
                        both_rate
                    ),
                    "difference_percentage_points": rounded(
                        difference
                    ),
                    "assessment": assessment,
                })

    return comparisons


def atomic_csv(
    path: Path,
    columns: list[str],
    rows: list[dict],
) -> None:
    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def verify_outputs(
    expected_detail_rows: int,
) -> None:
    with OUTPUT_DETAIL.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != DETAIL_COLUMNS:
            raise ValueError(
                "Independent detail schema mismatch"
            )

        count = sum(1 for _ in reader)

    if count != expected_detail_rows:
        raise ValueError(
            "Independent detail row count mismatch"
        )

    expected = (
        len(TIMEFRAMES)
        * len(CLASSIFICATIONS)
        * len(HORIZONS)
        * 3
    )

    with OUTPUT_SUMMARY.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != SUMMARY_COLUMNS:
            raise ValueError(
                "Independent summary schema mismatch"
            )

        count = sum(1 for _ in reader)

    if count != expected:
        raise ValueError(
            f"Expected {expected} summary rows, got {count}"
        )

    with OUTPUT_COMPARISON.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != COMPARISON_COLUMNS:
            raise ValueError(
                "Independent comparison schema mismatch"
            )

        count = sum(1 for _ in reader)

    if count != 18:
        raise ValueError(
            f"Expected 18 comparison rows, got {count}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Independent-signal chronological validation "
            "for Pattern 256 and accumulation candles."
        )
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild database and all checkpoints.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = time.monotonic()

    log("=" * 72)
    log(f"{PROGRAM} - {VERSION}")
    log("=" * 72)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    CHECKPOINT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_hash, result_hash, source_result = (
        verify_sources()
    )

    connection = connect_database()

    try:
        imported_rows = import_source(
            connection,
            detail_hash,
            result_hash,
            args.force,
        )

        boundaries = get_split_boundaries(
            connection
        )

        groups = get_groups(
            connection
        )

        if not groups:
            raise ValueError(
                "No market/timeframe/horizon groups"
            )

        log(
            f"[PLAN] Groups: {len(groups):,}"
        )

        total_source_rows = 0
        total_selected_rows = 0
        total_excluded_rows = 0

        for number, (
            timeframe,
            market,
            horizon,
        ) in enumerate(groups, start=1):
            cutoff = boundaries[
                timeframe
            ]["cutoff_epoch"]

            source_count, selected, excluded = (
                process_group(
                    connection=connection,
                    timeframe=timeframe,
                    market=market,
                    horizon=horizon,
                    cutoff=cutoff,
                    detail_hash=detail_hash,
                    result_hash=result_hash,
                    force=args.force,
                )
            )

            total_source_rows += source_count
            total_selected_rows += selected
            total_excluded_rows += excluded

            if number % 25 == 0 or number == len(groups):
                log(
                    f"[PROGRESS] {number:,}/{len(groups):,} "
                    f"selected={total_selected_rows:,} "
                    f"excluded={total_excluded_rows:,}"
                )

        if total_source_rows != imported_rows:
            raise ValueError(
                "Source row accounting mismatch"
            )

        if (
            total_selected_rows + total_excluded_rows
            != imported_rows
        ):
            raise ValueError(
                "Selected/excluded row accounting mismatch"
            )

        log("[MERGE] Combining independent signals")

        merged_rows = combine_detail(groups)

        if merged_rows != total_selected_rows:
            raise ValueError(
                "Merged independent row count mismatch"
            )

        log("[SUMMARY] Calculating independent hit rates")

        summary = calculate_summary()

        comparison = calculate_comparison(
            summary
        )

        atomic_csv(
            OUTPUT_SUMMARY,
            SUMMARY_COLUMNS,
            summary,
        )

        atomic_csv(
            OUTPUT_COMPARISON,
            COMPARISON_COLUMNS,
            comparison,
        )

        verify_outputs(
            total_selected_rows
        )

        if sha256_file(SOURCE_DETAIL) != detail_hash:
            raise RuntimeError(
                "Source detail changed during validation"
            )

        if sha256_file(SOURCE_RESULT) != result_hash:
            raise RuntimeError(
                "Source result changed during validation"
            )

        elapsed = round(
            time.monotonic() - start,
            3,
        )

        warnings = [
            (
                "Chronological split is based on historical "
                "data already used during research; it is "
                "not a genuinely unseen out-of-sample test."
            ),
            (
                "Overlap windows use nominal timeframe "
                "duration; missing OHLCV candles may "
                "extend actual evaluation periods."
            ),
            (
                "Repeated markets and correlated market "
                "conditions can invalidate independence "
                "assumptions behind confidence intervals."
            ),
            (
                "A future high reaching +30% is not "
                "equivalent to an executable net profit."
            ),
            (
                "Only selected non-overlapping signals "
                "are used for descriptive comparisons; "
                "no strategy parameters are optimized."
            ),
        ]

        result = {
            "program": PROGRAM,
            "version": VERSION,
            "status": "PASS",
            "created_at_utc": utc_now(),
            "source_validation_status": (
                source_result["status"]
            ),
            "source_detail_sha256": detail_hash,
            "source_result_sha256": result_hash,
            "target_return_pct": 30.0,
            "train_fraction": TRAIN_FRACTION,
            "split_method": (
                "CHRONOLOGICAL_TIME_SPAN_PER_TIMEFRAME"
            ),
            "overlap_policy": (
                "GREEDY_NON_OVERLAPPING_PER_MARKET_"
                "TIMEFRAME_HORIZON"
            ),
            "timeframes": list(TIMEFRAMES),
            "horizons": list(HORIZONS),
            "classifications": list(CLASSIFICATIONS),
            "boundaries": boundaries,
            "imported_rows": imported_rows,
            "source_rows_accounted": total_source_rows,
            "selected_rows": total_selected_rows,
            "excluded_rows": total_excluded_rows,
            "summary_rows": len(summary),
            "comparison_rows": len(comparison),
            "elapsed_seconds": elapsed,
            "detail_sha256": sha256_file(
                OUTPUT_DETAIL
            ),
            "summary_sha256": sha256_file(
                OUTPUT_SUMMARY
            ),
            "comparison_sha256": sha256_file(
                OUTPUT_COMPARISON
            ),
            "warnings": warnings,
        }

        manifest = {
            "program": PROGRAM,
            "version": VERSION,
            "created_at_utc": utc_now(),
            "inputs": {
                "validation_detail.csv": detail_hash,
                "validation_result.json": result_hash,
            },
            "outputs": {
                "independent_detail.csv": sha256_file(
                    OUTPUT_DETAIL
                ),
                "independent_summary.csv": sha256_file(
                    OUTPUT_SUMMARY
                ),
                "independent_comparison.csv": sha256_file(
                    OUTPUT_COMPARISON
                ),
            },
            "checkpoint_database": str(
                DATABASE_FILE.relative_to(ROOT)
            ),
            "group_count": len(groups),
        }

        atomic_json(
            OUTPUT_MANIFEST,
            manifest,
        )

        atomic_json(
            OUTPUT_RESULT,
            result,
        )

        log("=" * 72)
        log("[PASS] Independent validation completed")
        log(
            f"[PASS] Source rows: {imported_rows:,}"
        )
        log(
            f"[PASS] Selected: {total_selected_rows:,}"
        )
        log(
            f"[PASS] Excluded: {total_excluded_rows:,}"
        )
        log(
            f"[PASS] Summary rows: {len(summary):,}"
        )
        log(
            f"[PASS] Comparison rows: {len(comparison):,}"
        )
        log(
            f"[PASS] Elapsed: {elapsed:.1f}s"
        )
        log(
            f"[OUTPUT] {OUTPUT_DIR}"
        )
        log("=" * 72)

        return 0

    finally:
        connection.close()


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log("[STOP] Interrupted safely.")
        sys.exit(130)

    except Exception:
        log("[FAIL] Independent validation error")
        traceback.print_exc()
        sys.exit(1)
