
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
UPBIT SURGE MONITOR
Candidate Independent Backtest
Clean V003

Existing OOS reproducibility audit.

IMPORTANT:
- This is NOT a new independent OOS test.
- No automatic trading.
- No existing production input is modified.
- SQLite checkpoints support safe resume.
- Previously completed rows are skipped only after
  checkpoint integrity and provenance checks.
- A calculation mismatch must not be hidden.
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

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


PROGRAM = (
    "backtest_pattern_256_accumulation_"
    "candidate_independent.py"
)

VERSION = "Clean V003"

DETAIL_RELATIVE = Path(
    "data/reports/pattern_256_accumulation_30pct/"
    "validation_detail.csv"
)

OOS_RELATIVE = Path(
    "data/reports/pattern_256_accumulation_candidate_oos/"
    "candidate_oos_result.json"
)

OUTPUT_RELATIVE = Path(
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest"
)

TIMEFRAME_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

HORIZONS = (24, 72, 168)

TARGET_PCT = 30.0

CHECKPOINT_INTERVAL = 1000

TRADE_FIELDS = [
    "rule_id",
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "max_return_pct",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
    "source_status",
    "source_hit_30pct",
    "status_match",
    "entry_price_match",
    "first_hit_match",
    "observed_bars_match",
    "max_return_match",
    "max_drawdown_match",
    "all_checks_match",
]

SUMMARY_FIELDS = [
    "rule_id",
    "timeframe",
    "horizon",
    "evaluation_rows",
    "hits",
    "misses",
    "censored",
    "hit_rate_observed",
    "status_mismatches",
    "entry_price_mismatches",
    "first_hit_mismatches",
    "observed_bars_mismatches",
    "max_return_mismatches",
    "max_drawdown_mismatches",
    "assessment",
]

CHECK_FIELDS = [
    "check",
    "status",
    "detail",
]


def log(message):
    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(
        f"{now} | {message}",
        flush=True,
    )


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def parse_time(value):
    text = str(value).strip()

    if not text:
        raise ValueError(
            "Empty timestamp"
        )

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    # Existing research timestamps are ISO8601.
    # Numeric timestamps are deliberately rejected.
    try:
        result = datetime.fromisoformat(
            text
        )
    except ValueError as exc:
        raise ValueError(
            f"Invalid ISO8601 timestamp: {value}"
        ) from exc

    if result.tzinfo is None:
        raise ValueError(
            f"Timestamp timezone missing: {value}"
        )

    return result.astimezone(
        timezone.utc
    )


def finite_float(value):
    result = float(value)

    if not math.isfinite(result):
        raise ValueError(
            f"Non-finite value: {value}"
        )

    return result


def optional_float(value):
    if value is None:
        return None

    if str(value).strip() == "":
        return None

    return finite_float(value)


def optional_int(value):
    if value is None:
        return None

    if str(value).strip() == "":
        return None

    number = finite_float(value)

    if not number.is_integer():
        raise ValueError(
            f"Expected integer: {value}"
        )

    return int(number)


def nearly_equal(left, right):
    if left is None or right is None:
        return left is None and right is None

    return math.isclose(
        float(left),
        float(right),
        rel_tol=1e-7,
        abs_tol=1e-7,
    )


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            chunk = stream.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_json(path, payload):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(
                stream.name
            )

            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )

            stream.write("\n")
            stream.flush()
            os.fsync(
                stream.fileno()
            )

        os.replace(
            temporary,
            path,
        )

    finally:
        if (
            temporary is not None
            and temporary.exists()
        ):
            temporary.unlink()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        result = json.load(stream)

    if not isinstance(result, dict):
        raise ValueError(
            f"JSON object required: {path}"
        )

    return result


def load_oos(path):
    payload = read_json(path)

    if payload.get("status") != "PASS":
        raise ValueError(
            "OOS source status is not PASS"
        )

    splits = payload.get("splits")

    if not isinstance(splits, dict):
        raise ValueError(
            "OOS splits missing"
        )

    boundaries = {}

    for timeframe in TIMEFRAME_SECONDS:
        item = splits.get(timeframe)

        if not isinstance(item, dict):
            raise ValueError(
                f"Missing OOS split: {timeframe}"
            )

        boundaries[timeframe] = parse_time(
            item["oos_boundary"]
        )

    rules = payload.get("signal_rules")

    if not isinstance(rules, list):
        raise ValueError(
            "Signal rules missing"
        )

    expected = {
        "S00_ALL_SIGNALS",
        "S01_PATTERN_60",
        "S02_PATTERN_70",
        "S03_BOTH_PATTERN_60",
        "S04_BOTH_STRICT",
    }

    actual = [
        rule.get("rule_id")
        for rule in rules
    ]

    if (
        len(actual) != len(expected)
        or set(actual) != expected
    ):
        raise ValueError(
            "Unexpected candidate rules"
        )

    for rule in rules:
        for field in (
            "rule_id",
            "min_pattern_256_score",
            "min_accumulation_score",
            "require_both",
        ):
            if field not in rule:
                raise ValueError(
                    f"Missing rule field: {field}"
                )

        if not isinstance(
            rule["require_both"],
            bool,
        ):
            raise ValueError(
                "require_both must be boolean"
            )

    return boundaries, rules


def rule_matches(row, rule):
    pattern = finite_float(
        row["pattern_256_score"]
    )

    accumulation = finite_float(
        row["accumulation_score"]
    )

    if pattern < finite_float(
        rule["min_pattern_256_score"]
    ):
        return False

    if accumulation < finite_float(
        rule["min_accumulation_score"]
    ):
        return False

    if (
        rule["require_both"]
        and row["classification"] != "BOTH"
    ):
        return False

    return True


def find_column(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate

    raise ValueError(
        f"OHLCV column missing: {candidates}"
    )


def find_ohlcv(root, timeframe, market):
    directory = root / timeframe

    candidates = [
        directory / f"{market}.csv",
        directory / (
            market.replace("-", "_") + ".csv"
        ),
    ]

    found = [
        path
        for path in candidates
        if path.is_file()
    ]

    if len(found) != 1:
        raise FileNotFoundError(
            f"OHLCV missing or ambiguous: "
            f"{timeframe}/{market}"
        )

    return found[0]


def load_candles(path):
    candles = []
    positions = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        columns = reader.fieldnames or []

        time_key = find_column(
            columns,
            (
                "candle_date_time_utc",
                "timestamp",
            ),
        )

        open_key = find_column(
            columns,
            (
                "opening_price",
                "open",
            ),
        )

        high_key = find_column(
            columns,
            (
                "high_price",
                "high",
            ),
        )

        low_key = find_column(
            columns,
            (
                "low_price",
                "low",
            ),
        )

        close_key = find_column(
            columns,
            (
                "trade_price",
                "close",
            ),
        )

        previous = None

        for row in reader:
            timestamp = parse_time(
                row[time_key]
            )

            opening = finite_float(
                row[open_key]
            )

            high = finite_float(
                row[high_key]
            )

            low = finite_float(
                row[low_key]
            )

            close = finite_float(
                row[close_key]
            )

            if min(
                opening,
                high,
                low,
                close,
            ) <= 0:
                raise ValueError(
                    f"Non-positive OHLCV: {path}"
                )

            if (
                high < max(opening, close, low)
                or low > min(opening, close, high)
            ):
                raise ValueError(
                    f"Invalid OHLCV range: {path}"
                )

            if (
                previous is not None
                and timestamp <= previous
            ):
                raise ValueError(
                    f"OHLCV not strictly sorted: {path}"
                )

            if timestamp in positions:
                raise ValueError(
                    f"Duplicate OHLCV timestamp: {path}"
                )

            positions[timestamp] = len(candles)

            candles.append({
                "timestamp": timestamp,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
            })

            previous = timestamp

    if not candles:
        raise ValueError(
            f"Empty OHLCV file: {path}"
        )

    return candles, positions


def calculate_event(row, candles, positions):
    timeframe = row["timeframe"]

    signal_time = parse_time(
        row["signal_timestamp"]
    )

    entry_time = parse_time(
        row["entry_timestamp"]
    )

    horizon = optional_int(
        row["horizon"]
    )

    if horizon not in HORIZONS:
        raise ValueError(
            f"Unexpected horizon: {horizon}"
        )

    seconds = TIMEFRAME_SECONDS[
        timeframe
    ]

    expected_entry = (
        signal_time.timestamp()
        + seconds
    )

    if entry_time.timestamp() != expected_entry:
        raise ValueError(
            "Entry timestamp is not the next "
            f"expected candle: "
            f"{timeframe}/{row['market']}"
        )

    if entry_time not in positions:
        raise ValueError(
            "Entry candle missing: "
            f"{timeframe}/{row['market']}/"
            f"{entry_time.isoformat()}"
        )

    entry_index = positions[
        entry_time
    ]

    entry_price = candles[
        entry_index
    ]["open"]

    source_entry = finite_float(
        row["entry_price"]
    )

    entry_match = nearly_equal(
        entry_price,
        source_entry,
    )

    target_price = entry_price * (
        1.0 + TARGET_PCT / 100.0
    )

    highest = entry_price
    lowest = entry_price

    first_hit = None
    observed = 0

    for offset in range(horizon):
        index = entry_index + offset

        if index >= len(candles):
            break

        candle = candles[index]

        expected_timestamp = (
            entry_time.timestamp()
            + offset * seconds
        )

        if (
            candle["timestamp"].timestamp()
            != expected_timestamp
        ):
            break

        observed += 1

        highest = max(
            highest,
            candle["high"],
        )

        lowest = min(
            lowest,
            candle["low"],
        )

        if (
            first_hit is None
            and candle["high"] >= target_price
        ):
            first_hit = offset + 1

    # Critical Clean V003 correction:
    #
    # A target reached within the observed
    # candles is HIT even when the complete
    # horizon is not available.
    #
    # CENSORED means the target was not
    # observed and the full horizon is
    # unavailable.
    #
    # MISS requires the full horizon.

    if first_hit is not None:
        status = "HIT"
        hit_value = 1

    elif observed < horizon:
        status = "CENSORED"
        hit_value = ""

    else:
        status = "MISS"
        hit_value = 0

    max_return = (
        (highest / entry_price - 1.0)
        * 100.0
    )

    max_drawdown = (
        (lowest / entry_price - 1.0)
        * 100.0
    )

    source_status = str(
        row["status"]
    ).strip().upper()

    if source_status not in (
        "HIT",
        "MISS",
        "CENSORED",
    ):
        raise ValueError(
            f"Unexpected source status: "
            f"{source_status}"
        )

    source_hit = optional_int(
        row["hit_30pct"]
    )

    if source_hit not in (
        None,
        0,
        1,
    ):
        raise ValueError(
            "Invalid source hit_30pct"
        )

    status_match = (
        status == source_status
    )

    # Source CENSORED records may encode
    # hit_30pct as 0 or blank.
    # Neither is treated as a confirmed MISS.

    if status == "HIT":
        status_match = (
            status_match
            and source_hit == 1
        )

    elif status == "MISS":
        status_match = (
            status_match
            and source_hit == 0
        )

    first_hit_source = optional_int(
        row.get("first_hit_bars")
    )

    first_hit_match = (
        first_hit == first_hit_source
    )

    source_observed = optional_int(
        row.get("observed_bars")
    )

    observed_match = (
        observed == source_observed
    )

    source_return = optional_float(
        row.get("max_return_pct")
    )

    return_match = nearly_equal(
        max_return,
        source_return,
    )

    source_drawdown = optional_float(
        row.get("max_drawdown_pct")
    )

    drawdown_match = nearly_equal(
        max_drawdown,
        source_drawdown,
    )

    checks = [
        status_match,
        entry_match,
        first_hit_match,
        observed_match,
        return_match,
        drawdown_match,
    ]

    return {
        "timeframe": timeframe,
        "market": row["market"],
        "signal_timestamp": (
            signal_time.isoformat()
        ),
        "entry_timestamp": (
            entry_time.isoformat()
        ),
        "entry_price": entry_price,
        "horizon": horizon,
        "status": status,
        "hit_30pct": hit_value,
        "first_hit_bars": (
            first_hit
            if first_hit is not None
            else ""
        ),
        "max_return_pct": max_return,
        "max_drawdown_pct": max_drawdown,
        "observed_bars": observed,
        "required_bars": horizon,
        "source_status": source_status,
        "source_hit_30pct": (
            row["hit_30pct"]
        ),
        "status_match": status_match,
        "entry_price_match": entry_match,
        "first_hit_match": first_hit_match,
        "observed_bars_match": observed_match,
        "max_return_match": return_match,
        "max_drawdown_match": drawdown_match,
        "all_checks_match": all(checks),
    }


def create_checkpoint(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS progress (
            id INTEGER PRIMARY KEY
                CHECK (id = 1),
            source_rows INTEGER NOT NULL,
            oos_rows INTEGER NOT NULL,
            matched_rows INTEGER NOT NULL,
            missing_entry_rows INTEGER NOT NULL
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_row INTEGER NOT NULL,
            rule_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            payload TEXT NOT NULL,
            UNIQUE(source_row, rule_id)
        )
        """
    )

    connection.commit()


def metadata_get(connection, key):
    row = connection.execute(
        "SELECT value FROM metadata WHERE key = ?",
        (key,),
    ).fetchone()

    return row[0] if row else None


def metadata_set(connection, key, value):
    connection.execute(
        """
        INSERT INTO metadata(key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def progress_get(connection):
    row = connection.execute(
        """
        SELECT
            source_rows,
            oos_rows,
            matched_rows,
            missing_entry_rows
        FROM progress
        WHERE id = 1
        """
    ).fetchone()

    if row is None:
        return {
            "source_rows": 0,
            "oos_rows": 0,
            "matched_rows": 0,
            "missing_entry_rows": 0,
        }

    return {
        "source_rows": int(row[0]),
        "oos_rows": int(row[1]),
        "matched_rows": int(row[2]),
        "missing_entry_rows": int(row[3]),
    }


def progress_save(connection, progress):
    connection.execute(
        """
        INSERT INTO progress(
            id,
            source_rows,
            oos_rows,
            matched_rows,
            missing_entry_rows
        )
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT(id)
        DO UPDATE SET
            source_rows = excluded.source_rows,
            oos_rows = excluded.oos_rows,
            matched_rows = excluded.matched_rows,
            missing_entry_rows =
                excluded.missing_entry_rows
        """,
        (
            progress["source_rows"],
            progress["oos_rows"],
            progress["matched_rows"],
            progress["missing_entry_rows"],
        ),
    )


def checkpoint_integrity(connection):
    result = connection.execute(
        "PRAGMA integrity_check"
    ).fetchone()

    if result is None or result[0] != "ok":
        raise RuntimeError(
            "Checkpoint database integrity failed"
        )

    duplicate = connection.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT source_row, rule_id
            FROM trades
            GROUP BY source_row, rule_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate:
        raise RuntimeError(
            "Duplicate checkpoint trade records"
        )

    progress = progress_get(
        connection
    )

    invalid = connection.execute(
        """
        SELECT COUNT(*)
        FROM trades
        WHERE source_row > ?
           OR source_row < 1
        """,
        (progress["source_rows"],),
    ).fetchone()[0]

    if invalid:
        raise RuntimeError(
            "Checkpoint progress inconsistent"
        )

    return progress


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Existing OOS candidate "
            "reproducibility audit"
        )
    )

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--detail",
    )

    parser.add_argument(
        "--oos-result",
    )

    parser.add_argument(
        "--ohlcv-root",
    )

    parser.add_argument(
        "--output-dir",
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--resume",
        action="store_true",
    )

    return parser.parse_args()


def write_csv(path, fields, rows):
    temporary = path.with_name(
        f".{path.name}.tmp"
    )

    if temporary.exists():
        raise FileExistsError(
            f"Temporary output exists: {temporary}"
        )

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=fields,
                lineterminator="\n",
            )

            writer.writeheader()

            for row in rows:
                writer.writerow(row)

            stream.flush()
            os.fsync(
                stream.fileno()
            )

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def main():
    args = parse_args()

    if args.max_events < 0:
        raise ValueError(
            "max-events cannot be negative"
        )

    root = Path(
        args.root
    ).expanduser().resolve()

    detail = (
        Path(args.detail).expanduser().resolve()
        if args.detail
        else root / DETAIL_RELATIVE
    )

    oos = (
        Path(args.oos_result).expanduser().resolve()
        if args.oos_result
        else root / OOS_RELATIVE
    )

    ohlcv_root = (
        Path(args.ohlcv_root).expanduser().resolve()
        if args.ohlcv_root
        else root / "data" / "ohlcv"
    )

    output = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else root / OUTPUT_RELATIVE
    )

    for path in (detail, oos):
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing input: {path}"
            )

    if not ohlcv_root.is_dir():
        raise FileNotFoundError(
            f"Missing OHLCV root: {ohlcv_root}"
        )

    # Never allow output to overlap source data.

    output = output.resolve()

    for protected in (
        detail,
        oos,
        ohlcv_root,
    ):
        protected = protected.resolve()

        if (
            output == protected
            or output in protected.parents
            or protected in output.parents
        ):
            raise ValueError(
                f"Output overlaps input: {protected}"
            )

    log("=" * 70)
    log(f"{PROGRAM} | {VERSION}")
    log("=" * 70)

    boundaries, rules = load_oos(
        oos
    )

    for timeframe, boundary in boundaries.items():
        log(
            f"[OOS] {timeframe}: "
            f"{boundary.isoformat()}"
        )

    required_columns = [
        "timeframe",
        "market",
        "signal_timestamp",
        "classification",
        "pattern_256_score",
        "accumulation_score",
        "entry_timestamp",
        "entry_price",
        "horizon",
        "status",
        "hit_30pct",
        "first_hit_bars",
        "max_return_pct",
        "max_drawdown_pct",
        "observed_bars",
    ]

    with detail.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames or []

    missing = [
        name
        for name in required_columns
        if name not in columns
    ]

    if missing:
        raise ValueError(
            f"Missing detail columns: {missing}"
        )

    checkpoint_path = (
        output / "checkpoint.sqlite3"
    )

    if args.resume:
        if not checkpoint_path.is_file():
            raise FileNotFoundError(
                "Resume requested but checkpoint missing"
            )
    else:
        if output.exists() and any(
            output.iterdir()
        ):
            raise FileExistsError(
                "Output directory is not empty. "
                "Use a new output path or --resume."
            )

        output.mkdir(
            parents=True,
            exist_ok=True,
        )

    connection = sqlite3.connect(
        str(checkpoint_path),
        timeout=60,
    )

    connection.execute(
        "PRAGMA synchronous=FULL"
    )

    try:
        create_checkpoint(
            connection
        )

        input_hashes = {
            str(detail): sha256(detail),
            str(oos): sha256(oos),
        }

        # Include exact source script and
        # processing parameters in checkpoint
        # provenance. A modified script cannot
        # silently resume the old checkpoint.

        script_hash = sha256(
            Path(__file__).resolve()
        )

        provenance = {
            "version": VERSION,
            "script_sha256": script_hash,
            "detail": str(detail),
            "oos": str(oos),
            "ohlcv_root": str(ohlcv_root),
            "max_events": args.max_events,
            "input_sha256": input_hashes,
        }

        provenance_text = json.dumps(
            provenance,
            sort_keys=True,
            ensure_ascii=False,
        )

        existing = metadata_get(
            connection,
            "provenance",
        )

        if existing is None:
            if args.resume:
                raise RuntimeError(
                    "Checkpoint provenance missing"
                )

            metadata_set(
                connection,
                "provenance",
                provenance_text,
            )

            connection.commit()

        elif existing != provenance_text:
            raise RuntimeError(
                "Checkpoint provenance mismatch. "
                "Resume refused."
            )

        progress = checkpoint_integrity(
            connection
        )

        resume_rows = progress[
            "source_rows"
        ]

        if args.resume:
            log(
                f"[RESUME] Verified rows: "
                f"{resume_rows:,}"
            )

        # OHLCV input hashes are stored in
        # checkpoint metadata and verified
        # before reusing each cached result.

        def verified_ohlcv(path):
            key = (
                "ohlcv_sha256:"
                + str(path.resolve())
            )

            actual = sha256(path)

            previous = metadata_get(
                connection,
                key,
            )

            if previous is not None:
                if previous != actual:
                    raise RuntimeError(
                        "OHLCV changed since checkpoint: "
                        f"{path}"
                    )
            else:
                metadata_set(
                    connection,
                    key,
                    actual,
                )

            input_hashes[
                str(path.resolve())
            ] = actual

        cache_key = None
        cache_candles = None
        cache_positions = None

        log("[READ] Validation detail")

        with detail.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as stream:
            reader = csv.DictReader(
                stream
            )

            # Resuming skips previously committed
            # source rows. No previous result is
            # accepted without checkpoint checks.

            for index, row in enumerate(
                reader,
                start=1,
            ):
                if index <= resume_rows:
                    continue

                progress["source_rows"] = index

                timeframe = str(
                    row["timeframe"]
                ).strip().lower()

                if timeframe not in boundaries:
                    raise ValueError(
                        f"Unknown timeframe: {timeframe}"
                    )

                row["timeframe"] = timeframe

                signal_time = parse_time(
                    row["signal_timestamp"]
                )

                if signal_time >= boundaries[timeframe]:
                    progress["oos_rows"] += 1

                    matched_rules = [
                        rule
                        for rule in rules
                        if rule_matches(
                            row,
                            rule,
                        )
                    ]

                    if matched_rules:
                        progress["matched_rows"] += 1

                        if not str(
                            row["entry_timestamp"]
                        ).strip():
                            progress[
                                "missing_entry_rows"
                            ] += 1

                        else:
                            market = str(
                                row["market"]
                            ).strip().upper()

                            if not market.startswith("KRW-"):
                                raise ValueError(
                                    f"Invalid market: {market}"
                                )

                            row["market"] = market

                            key = (
                                timeframe,
                                market,
                            )

                            if key != cache_key:
                                path = find_ohlcv(
                                    ohlcv_root,
                                    timeframe,
                                    market,
                                )

                                verified_ohlcv(
                                    path
                                )

                                (
                                    cache_candles,
                                    cache_positions,
                                ) = load_candles(
                                    path
                                )

                                cache_key = key

                                log(
                                    "[OHLCV] "
                                    f"{timeframe}/{market} "
                                    f"candles={len(cache_candles)}"
                                )

                            event = calculate_event(
                                row,
                                cache_candles,
                                cache_positions,
                            )

                            for rule in matched_rules:
                                trade = {
                                    "rule_id": rule["rule_id"],
                                    **event,
                                }

                                connection.execute(
                                    """
                                    INSERT INTO trades(
                                        source_row,
                                        rule_id,
                                        timeframe,
                                        market,
                                        horizon,
                                        payload
                                    )
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        index,
                                        rule["rule_id"],
                                        timeframe,
                                        market,
                                        event["horizon"],
                                        json.dumps(
                                            trade,
                                            ensure_ascii=False,
                                            allow_nan=False,
                                        ),
                                    ),
                                )

                if (
                    index % CHECKPOINT_INTERVAL == 0
                ):
                    progress_save(
                        connection,
                        progress,
                    )

                    connection.commit()

                    log(
                        "[CHECKPOINT] "
                        f"source={index:,} "
                        f"matched="
                        f"{progress['matched_rows']:,}"
                    )

                if (
                    args.max_events > 0
                    and progress["matched_rows"]
                    >= args.max_events
                ):
                    log(
                        "[SMOKE TEST] "
                        "Event limit reached"
                    )
                    break

        progress_save(
            connection,
            progress,
        )

        connection.commit()

        checkpoint_integrity(
            connection
        )

        # Recheck every OHLCV used in this
        # checkpoint, including files processed
        # before the current resume session.

        metadata_rows = connection.execute(
            """
            SELECT key, value
            FROM metadata
            WHERE key LIKE 'ohlcv_sha256:%'
            """
        ).fetchall()

        for key, expected_hash in metadata_rows:
            path = Path(
                key[len("ohlcv_sha256:"):]
            )

            if not path.is_file():
                raise RuntimeError(
                    f"OHLCV disappeared: {path}"
                )

            actual_hash = sha256(
                path
            )

            if actual_hash != expected_hash:
                raise RuntimeError(
                    f"OHLCV SHA256 changed: {path}"
                )

            input_hashes[
                str(path)
            ] = actual_hash

        for path_text, expected_hash in list(
            input_hashes.items()
        ):
            actual_hash = sha256(
                Path(path_text)
            )

            if actual_hash != expected_hash:
                raise RuntimeError(
                    "Source SHA256 changed: "
                    f"{path_text}"
                )

        log("[WRITE] Trade CSV")

        trades_path = (
            output
            / "independent_backtest_trades.csv"
        )

        summary_path = (
            output
            / "independent_backtest_summary.csv"
        )

        checks_path = (
            output
            / "independent_backtest_checks.csv"
        )

        result_path = (
            output
            / "independent_backtest_result.json"
        )

        manifest_path = (
            output
            / "independent_backtest_manifest.json"
        )

        # Export rows from verified checkpoint.

        def trade_rows():
            cursor = connection.execute(
                """
                SELECT payload
                FROM trades
                ORDER BY source_row, id
                """
            )

            for (payload,) in cursor:
                yield json.loads(
                    payload
                )

        write_csv(
            trades_path,
            TRADE_FIELDS,
            trade_rows(),
        )

        summary_groups = defaultdict(
            lambda: {
                "evaluation_rows": 0,
                "hits": 0,
                "misses": 0,
                "censored": 0,
                "status_mismatches": 0,
                "entry_price_mismatches": 0,
                "first_hit_mismatches": 0,
                "observed_bars_mismatches": 0,
                "max_return_mismatches": 0,
                "max_drawdown_mismatches": 0,
            }
        )

        mismatch_fields = {
            "status_match": "status_mismatches",
            "entry_price_match": (
                "entry_price_mismatches"
            ),
            "first_hit_match": (
                "first_hit_mismatches"
            ),
            "observed_bars_match": (
                "observed_bars_mismatches"
            ),
            "max_return_match": (
                "max_return_mismatches"
            ),
            "max_drawdown_match": (
                "max_drawdown_mismatches"
            ),
        }

        output_rows = 0

        for trade in trade_rows():
            output_rows += 1

            key = (
                trade["rule_id"],
                trade["timeframe"],
                trade["horizon"],
            )

            group = summary_groups[
                key
            ]

            group[
                "evaluation_rows"
            ] += 1

            status = trade[
                "status"
            ]

            if status == "HIT":
                group["hits"] += 1

            elif status == "MISS":
                group["misses"] += 1

            elif status == "CENSORED":
                group["censored"] += 1

            else:
                raise ValueError(
                    f"Unknown result status: {status}"
                )

            for check_field, count_field in (
                mismatch_fields.items()
            ):
                if not trade[check_field]:
                    group[count_field] += 1

        summary_rows = []

        mismatch_totals = {
            value: 0
            for value in mismatch_fields.values()
        }

        for key, group in sorted(
            summary_groups.items()
        ):
            rule_id, timeframe, horizon = key

            for name in mismatch_totals:
                mismatch_totals[name] += group[
                    name
                ]

            observed_outcomes = (
                group["hits"]
                + group["misses"]
            )

            hit_rate = (
                group["hits"] / observed_outcomes
                if observed_outcomes
                else ""
            )

            mismatch_count = sum(
                group[name]
                for name in mismatch_totals
            )

            if mismatch_count:
                assessment = "MISMATCH"

            elif observed_outcomes == 0:
                assessment = (
                    "INSUFFICIENT_OBSERVED_OUTCOMES"
                )

            else:
                assessment = (
                    "REPRODUCED_DESCRIPTIVE_ONLY"
                )

            summary_rows.append({
                "rule_id": rule_id,
                "timeframe": timeframe,
                "horizon": horizon,
                **group,
                "hit_rate_observed": hit_rate,
                "assessment": assessment,
            })

        write_csv(
            summary_path,
            SUMMARY_FIELDS,
            summary_rows,
        )

        total_mismatches = sum(
            mismatch_totals.values()
        )

        is_smoke = (
            args.max_events > 0
        )

        if (
            total_mismatches > 0
            or output_rows == 0
        ):
            execution_status = "FAIL"

        elif is_smoke:
            execution_status = (
                "SMOKE_TEST_PASS"
            )

        else:
            execution_status = "PASS"

        checks = [
            {
                "check": "source_schema",
                "status": "PASS",
                "detail": (
                    "Required source columns present"
                ),
            },
            {
                "check": "oos_boundaries",
                "status": "PASS",
                "detail": (
                    "Original timeframe boundaries used"
                ),
            },
            {
                "check": "candidate_rules",
                "status": "PASS",
                "detail": (
                    "Frozen S00-S04 rules used"
                ),
            },
            {
                "check": "future_outcome_leakage",
                "status": "PASS",
                "detail": (
                    "Future outcomes not used "
                    "for signal selection"
                ),
            },
            {
                "check": "checkpoint_integrity",
                "status": "PASS",
                "detail": (
                    "SQLite integrity and progress checked"
                ),
            },
            {
                "check": "source_sha256",
                "status": "PASS",
                "detail": (
                    "All registered input hashes verified"
                ),
            },
        ]

        for name, count in mismatch_totals.items():
            checks.append({
                "check": name,
                "status": (
                    "PASS"
                    if count == 0
                    else "FAIL"
                ),
                "detail": (
                    f"mismatches={count}"
                ),
            })

        checks.extend([
            {
                "check": "independent_oos_proven",
                "status": "NOT_VERIFIED",
                "detail": (
                    "Existing evaluated OOS data reused"
                ),
            },
            {
                "check": "feature_causality",
                "status": "NOT_VERIFIED",
                "detail": (
                    "Original signal construction "
                    "not independently audited"
                ),
            },
            {
                "check": "execution_realism",
                "status": "NOT_VERIFIED",
                "detail": (
                    "Fees, slippage and intrabar "
                    "ordering not modeled"
                ),
            },
        ])

        write_csv(
            checks_path,
            CHECK_FIELDS,
            checks,
        )

        result = {
            "program": PROGRAM,
            "version": VERSION,
            "created_at_utc": utc_now(),
            "execution_status": execution_status,
            "assessment": (
                "EXISTING_OOS_REPRODUCIBILITY_ONLY"
            ),
            "independent_oos_proven": False,
            "approved_for_live_trading": False,
            "automatic_trading_enabled": False,
            "smoke_test": is_smoke,
            "max_events": args.max_events,
            "target_return_pct": TARGET_PCT,
            "horizons": list(HORIZONS),
            "source_rows_read": progress[
                "source_rows"
            ],
            "oos_rows_read": progress[
                "oos_rows"
            ],
            "matched_source_rows": progress[
                "matched_rows"
            ],
            "missing_entry_rows_excluded": (
                progress["missing_entry_rows"]
            ),
            "output_rows": output_rows,
            "summary_rows": len(summary_rows),
            "status_mismatches": mismatch_totals[
                "status_mismatches"
            ],
            "entry_price_mismatches": (
                mismatch_totals[
                    "entry_price_mismatches"
                ]
            ),
            "first_hit_mismatches": (
                mismatch_totals[
                    "first_hit_mismatches"
                ]
            ),
            "observed_bars_mismatches": (
                mismatch_totals[
                    "observed_bars_mismatches"
                ]
            ),
            "max_return_mismatches": (
                mismatch_totals[
                    "max_return_mismatches"
                ]
            ),
            "max_drawdown_mismatches": (
                mismatch_totals[
                    "max_drawdown_mismatches"
                ]
            ),
            "total_field_mismatches": (
                total_mismatches
            ),
            "oos_boundaries": {
                key: value.isoformat()
                for key, value in boundaries.items()
            },
            "rule_ids": [
                rule["rule_id"]
                for rule in rules
            ],
            "input_sha256": input_hashes,
            "checkpoint": {
                "file": checkpoint_path.name,
                "resume_supported": True,
                "source_rows_committed": progress[
                    "source_rows"
                ],
            },
            "limitations": [
                "Existing OOS data reused.",
                "Independent OOS not proven.",
                "Signal construction causality "
                "not independently audited.",
                "Events may be statistically dependent.",
                "Censored events are excluded "
                "from observed-outcome hit rate.",
                "Fees and slippage not modeled.",
                "No live trading approval.",
            ],
        }

        atomic_json(
            result_path,
            result,
        )

        output_hashes = {}

        for path in (
            trades_path,
            summary_path,
            checks_path,
            result_path,
        ):
            output_hashes[
                path.name
            ] = sha256(path)

        manifest = {
            "program": PROGRAM,
            "version": VERSION,
            "created_at_utc": utc_now(),
            "input_sha256": input_hashes,
            "output_sha256": output_hashes,
            "checkpoint_sha256": sha256(
                checkpoint_path
            ),
        }

        atomic_json(
            manifest_path,
            manifest,
        )

        log("=" * 70)
        log(
            f"EXECUTION STATUS: {execution_status}"
        )
        log(
            f"SOURCE ROWS: "
            f"{progress['source_rows']:,}"
        )
        log(
            f"OOS ROWS: "
            f"{progress['oos_rows']:,}"
        )
        log(
            f"MATCHED SOURCE ROWS: "
            f"{progress['matched_rows']:,}"
        )
        log(
            f"OUTPUT ROWS: {output_rows:,}"
        )

        for name, count in mismatch_totals.items():
            log(
                f"{name.upper()}: {count:,}"
            )

        log(
            "INDEPENDENT OOS PROVEN: FALSE"
        )
        log(
            "LIVE TRADING APPROVED: FALSE"
        )
        log("=" * 70)

        return (
            0
            if execution_status != "FAIL"
            else 1
        )

    finally:
        connection.close()


if __name__ == "__main__":
    try:
        sys.exit(
            main()
        )

    except Exception as exc:
        log(
            "[FAIL] "
            f"{type(exc).__name__}: {exc}"
        )

        sys.exit(1)
