
# ============================================================
# Upbit Surge Monitor
# research_pattern_256_accumulation_crosscheck.py
# Clean V001
#
# Independent research cross-check:
#   Pattern 256 + Accumulation Candle
#
# READ ONLY:
#   - Production OHLCV
#   - Pattern 256 research outputs
#   - Accumulation candle research outputs
#
# This stage does NOT:
#   - Generate future labels
#   - Calculate future returns
#   - Perform backtests
#   - Execute trading
#   - Define a final trading formula
# ============================================================

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
import traceback

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


PROGRAM_NAME = "research_pattern_256_accumulation_crosscheck.py"
VERSION = "Clean V001"
STAGE = "PATTERN 256 ACCUMULATION CROSSCHECK"

PATTERN_IDS = (
    "PATTERN_256",
    "ACCUMULATION_CANDLE",
)

OHLCV_READ_ONLY = True
SOURCE_REPORTS_READ_ONLY = True

FUTURE_CANDLES_ALLOWED = False
NEGATIVE_SHIFT_ALLOWED = False
CENTERED_ROLLING_ALLOWED = False
FUTURE_LABELS_GENERATED = False
FUTURE_RETURNS_GENERATED = False
THIRTY_PERCENT_LABEL_GENERATED = False

BACKTEST_GENERATED = False
PREDICTION_GENERATED = False
TRADING_SIGNAL_GENERATED = False
FINAL_FORMULA_DEFINED = False

TIMEFRAMES = ("h1", "h4", "d1")

REPORT_NAMESPACE = "pattern_256_accumulation_crosscheck"

RESULT_FILE = "crosscheck_result.json"
SUMMARY_FILE = "crosscheck_summary.csv"
DETAIL_FILE = "crosscheck_detail.csv"
CANDIDATE_FILE = "crosscheck_candidates.csv"
MANIFEST_FILE = "crosscheck_source_manifest.csv"
CONTRACT_FILE = "crosscheck_contract.json"
README_FILE = "README.txt"

CHECKPOINT_FILE = (
    "research_pattern_256_accumulation_crosscheck_checkpoint.json"
)

PATTERN_256_DETAIL = (
    "data/reports/pattern_research_256/"
    "pattern_256_research_detail.csv"
)

ACCUMULATION_DETAIL = (
    "data/reports/accumulation_candle_research/"
    "accumulation_candle_research_detail.csv"
)

REQUIRED_256 = {
    "timeframe",
    "market",
    "timestamp",
    "pattern_256_research_score",
    "pattern_256_candidate",
    "pattern_256_strong_candidate",
    "pattern_256_research_stage",
}

REQUIRED_ACCUMULATION = {
    "timeframe",
    "market",
    "timestamp",
    "accumulation_candle_research_score",
    "accumulation_candle_candidate",
    "accumulation_candle_strong_candidate",
    "accumulation_candle_research_stage",
}

DETAIL_FIELDS = [
    "timeframe",
    "market",
    "timestamp",
    "pattern_256_score",
    "accumulation_score",
    "pattern_256_candidate",
    "accumulation_candidate",
    "pattern_256_strong",
    "accumulation_strong",
    "pattern_256_stage",
    "accumulation_stage",
    "classification",
    "both_candidate",
    "both_strong",
    "accumulation_preceded_256",
    "accumulation_lag_bars",
]

SUMMARY_FIELDS = [
    "timeframe",
    "market",
    "matched_rows",
    "neither_rows",
    "pattern_256_only_rows",
    "accumulation_only_rows",
    "both_rows",
    "both_strong_rows",
    "accumulation_preceded_256_rows",
    "status",
]


def log(message: str) -> None:
    print(message, flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def atomic_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_name(path.name + ".tmp")

    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(
            data,
            stream,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )

    os.replace(temporary, path)


def parse_bool(value: Any) -> bool:
    text = str(value).strip().lower()

    if text in ("true", "1"):
        return True

    if text in ("false", "0", ""):
        return False

    raise ValueError(f"Invalid boolean: {value!r}")


def parse_score(value: Any) -> Optional[float]:
    text = str(value or "").strip()

    if not text or text.lower() == "nan":
        return None

    result = float(text)

    if not math.isfinite(result):
        raise ValueError("Non-finite research score")

    if not 0 <= result <= 100:
        raise ValueError(f"Score outside 0..100: {result}")

    return result


def normalize_timestamp(value: Any) -> str:
    text = str(value or "").strip()

    if not text:
        raise ValueError("Empty timestamp")

    normalized = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"Invalid timestamp: {text}"
        ) from exc

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat(timespec="microseconds")

    return parsed.isoformat(timespec="microseconds")


def validate_source(path: Path, required: set[str]) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"Source missing: {path}")

    if path.stat().st_size <= 0:
        raise ValueError(f"Source empty: {path}")

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        columns = set(reader.fieldnames or [])

    missing = required - columns

    if missing:
        raise ValueError(
            f"Missing columns in {path}: {sorted(missing)}"
        )


def read_source(
    path: Path,
    allowed_timeframes: set[str],
) -> Iterable[Dict[str, str]]:
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        for row in reader:
            if row.get("timeframe") not in allowed_timeframes:
                continue

            yield row


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA temp_store = FILE;

        CREATE TABLE pattern256 (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            score REAL,
            candidate INTEGER NOT NULL,
            strong INTEGER NOT NULL,
            stage TEXT NOT NULL,
            PRIMARY KEY (timeframe, market, timestamp)
        );

        CREATE TABLE accumulation (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            score REAL,
            candidate INTEGER NOT NULL,
            strong INTEGER NOT NULL,
            stage TEXT NOT NULL,
            PRIMARY KEY (timeframe, market, timestamp)
        );
        """
    )


def load_table(
    connection: sqlite3.Connection,
    path: Path,
    table: str,
    prefix: str,
    timeframes: set[str],
) -> int:
    if table not in ("pattern256", "accumulation"):
        raise ValueError("Invalid database table")

    score_field = prefix + "_research_score"
    candidate_field = prefix + "_candidate"
    strong_field = prefix + "_strong_candidate"
    stage_field = prefix + "_research_stage"

    sql = (
        f"INSERT INTO {table} "
        "(timeframe, market, timestamp, score, "
        "candidate, strong, stage) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    batch = []
    total = 0

    for row in read_source(path, timeframes):
        timeframe = row["timeframe"].strip()
        market = row["market"].strip()
        timestamp = normalize_timestamp(row["timestamp"])

        if not market:
            raise ValueError("Empty market in research source")

        candidate = parse_bool(row[candidate_field])
        strong = parse_bool(row[strong_field])
        score = parse_score(row[score_field])

        if strong and not candidate:
            raise ValueError(
                f"Strong candidate without candidate: "
                f"{timeframe} {market} {timestamp}"
            )

        if candidate and (score is None or score < 60):
            raise ValueError(
                f"Candidate score below threshold: "
                f"{timeframe} {market} {timestamp}"
            )

        if strong and (score is None or score < 75):
            raise ValueError(
                f"Strong score below threshold: "
                f"{timeframe} {market} {timestamp}"
            )

        stage = str(row[stage_field] or "").strip()

        batch.append(
            (
                timeframe,
                market,
                timestamp,
                score,
                int(candidate),
                int(strong),
                stage,
            )
        )

        if len(batch) >= 10000:
            connection.executemany(sql, batch)
            connection.commit()

            total += len(batch)
            batch.clear()

            if total % 100000 == 0:
                log(f"[LOAD] {table}: {total:,}")

    if batch:
        connection.executemany(sql, batch)
        connection.commit()
        total += len(batch)

    log(f"[PASS] {table} rows: {total:,}")

    if total <= 0:
        raise ValueError(f"No usable rows: {table}")

    return total


def classify(pattern_candidate: bool, accumulation_candidate: bool) -> str:
    if pattern_candidate and accumulation_candidate:
        return "BOTH"

    if pattern_candidate:
        return "PATTERN_256_ONLY"

    if accumulation_candidate:
        return "ACCUMULATION_ONLY"

    return "NEITHER"


def create_contract(lookback: int) -> Dict[str, Any]:
    return {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "stage": STAGE,
        "pattern_ids": list(PATTERN_IDS),
        "research_mode": "INDEPENDENT_RESULT_CROSSCHECK",
        "matching_keys": [
            "timeframe",
            "market",
            "timestamp",
        ],
        "classifications": [
            "NEITHER",
            "PATTERN_256_ONLY",
            "ACCUMULATION_ONLY",
            "BOTH",
        ],
        "precedence": {
            "description": (
                "A prior accumulation candidate within "
                "the configured historical bar window "
                "before a current pattern 256 candidate."
            ),
            "lookback_bars": lookback,
            "same_timeframe": True,
            "same_market": True,
            "current_bar_excluded": True,
            "uses_future_information": False,
        },
        "safety": {
            "ohlcv_read_only": OHLCV_READ_ONLY,
            "source_reports_read_only": SOURCE_REPORTS_READ_ONLY,
            "future_candles_allowed": FUTURE_CANDLES_ALLOWED,
            "negative_shift_allowed": NEGATIVE_SHIFT_ALLOWED,
            "centered_rolling_allowed": CENTERED_ROLLING_ALLOWED,
            "future_labels_generated": FUTURE_LABELS_GENERATED,
            "future_returns_generated": FUTURE_RETURNS_GENERATED,
            "thirty_percent_label_generated": (
                THIRTY_PERCENT_LABEL_GENERATED
            ),
            "backtest_generated": BACKTEST_GENERATED,
            "prediction_generated": PREDICTION_GENERATED,
            "trading_signal_generated": TRADING_SIGNAL_GENERATED,
        },
        "final_formula_defined": FINAL_FORMULA_DEFINED,
    }


def crosscheck(
    connection: sqlite3.Connection,
    detail_path: Path,
    candidate_path: Path,
    summary_path: Path,
    lookback: int,
) -> Dict[str, Any]:
    from collections import deque

    query = """
        SELECT
            p.timeframe,
            p.market,
            p.timestamp,
            p.score,
            a.score,
            p.candidate,
            a.candidate,
            p.strong,
            a.strong,
            p.stage,
            a.stage
        FROM pattern256 AS p
        INNER JOIN accumulation AS a
            ON p.timeframe = a.timeframe
           AND p.market = a.market
           AND p.timestamp = a.timestamp
        ORDER BY
            p.timeframe,
            p.market,
            p.timestamp
    """

    totals = Counter()
    group_totals = defaultdict(Counter)

    current_group = None
    prior_accumulation_positions = deque()
    position = 0

    with (
        detail_path.open(
            "w", encoding="utf-8", newline=""
        ) as detail_stream,
        candidate_path.open(
            "w", encoding="utf-8", newline=""
        ) as candidate_stream,
    ):
        detail_writer = csv.DictWriter(
            detail_stream,
            fieldnames=DETAIL_FIELDS,
        )

        candidate_writer = csv.DictWriter(
            candidate_stream,
            fieldnames=DETAIL_FIELDS,
        )

        detail_writer.writeheader()
        candidate_writer.writeheader()

        cursor = connection.execute(query)

        for values in cursor:
            (
                timeframe,
                market,
                timestamp,
                pattern_score,
                accumulation_score,
                pattern_candidate,
                accumulation_candidate,
                pattern_strong,
                accumulation_strong,
                pattern_stage,
                accumulation_stage,
            ) = values

            group = (timeframe, market)

            if group != current_group:
                current_group = group
                prior_accumulation_positions.clear()
                position = 0

            position += 1

            while (
                prior_accumulation_positions
                and position - prior_accumulation_positions[0]
                > lookback
            ):
                prior_accumulation_positions.popleft()

            p_candidate = bool(pattern_candidate)
            a_candidate = bool(accumulation_candidate)

            both = p_candidate and a_candidate
            both_strong = (
                bool(pattern_strong)
                and bool(accumulation_strong)
            )

            preceded = (
                p_candidate
                and bool(prior_accumulation_positions)
            )

            lag = (
                position - prior_accumulation_positions[-1]
                if preceded
                else ""
            )

            category = classify(p_candidate, a_candidate)

            record = {
                "timeframe": timeframe,
                "market": market,
                "timestamp": timestamp,
                "pattern_256_score": pattern_score,
                "accumulation_score": accumulation_score,
                "pattern_256_candidate": p_candidate,
                "accumulation_candidate": a_candidate,
                "pattern_256_strong": bool(pattern_strong),
                "accumulation_strong": bool(accumulation_strong),
                "pattern_256_stage": pattern_stage,
                "accumulation_stage": accumulation_stage,
                "classification": category,
                "both_candidate": both,
                "both_strong": both_strong,
                "accumulation_preceded_256": preceded,
                "accumulation_lag_bars": lag,
            }

            detail_writer.writerow(record)

            if both or preceded:
                candidate_writer.writerow(record)
                totals["output_candidates"] += 1

            totals["matched_rows"] += 1
            totals[category] += 1

            group_totals[group]["matched_rows"] += 1
            group_totals[group][category] += 1

            if both_strong:
                totals["both_strong_rows"] += 1
                group_totals[group]["both_strong_rows"] += 1

            if preceded:
                totals["preceded_rows"] += 1
                group_totals[group]["preceded_rows"] += 1

            # Update historical state AFTER current-row analysis.
            # This prevents the current candle from being treated
            # as a preceding accumulation candle.
            if a_candidate:
                prior_accumulation_positions.append(position)

            if totals["matched_rows"] % 100000 == 0:
                log(
                    "[CROSSCHECK] "
                    f"{totals['matched_rows']:,} rows"
                )

    with summary_path.open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=SUMMARY_FIELDS,
        )
        writer.writeheader()

        for (timeframe, market), counts in sorted(
            group_totals.items()
        ):
            writer.writerow(
                {
                    "timeframe": timeframe,
                    "market": market,
                    "matched_rows": counts["matched_rows"],
                    "neither_rows": counts["NEITHER"],
                    "pattern_256_only_rows": counts[
                        "PATTERN_256_ONLY"
                    ],
                    "accumulation_only_rows": counts[
                        "ACCUMULATION_ONLY"
                    ],
                    "both_rows": counts["BOTH"],
                    "both_strong_rows": counts[
                        "both_strong_rows"
                    ],
                    "accumulation_preceded_256_rows": counts[
                        "preceded_rows"
                    ],
                    "status": "PASS",
                }
            )

    if totals["matched_rows"] <= 0:
        raise ValueError("No matching research rows")

    if sum(
        totals[key]
        for key in (
            "NEITHER",
            "PATTERN_256_ONLY",
            "ACCUMULATION_ONLY",
            "BOTH",
        )
    ) != totals["matched_rows"]:
        raise ValueError("Classification count mismatch")

    return {
        "matched_rows": totals["matched_rows"],
        "neither_rows": totals["NEITHER"],
        "pattern_256_only_rows": totals[
            "PATTERN_256_ONLY"
        ],
        "accumulation_only_rows": totals[
            "ACCUMULATION_ONLY"
        ],
        "both_rows": totals["BOTH"],
        "both_strong_rows": totals["both_strong_rows"],
        "accumulation_preceded_256_rows": totals[
            "preceded_rows"
        ],
        "output_candidate_rows": totals[
            "output_candidates"
        ],
        "summary_rows": len(group_totals),
    }


def write_readme(path: Path, lookback: int) -> None:
    content = f"""Upbit Surge Monitor
Pattern 256 + Accumulation Candle Crosscheck
Clean V001

Purpose:
Compare two independently researched historical patterns.

Classification:
NEITHER
PATTERN_256_ONLY
ACCUMULATION_ONLY
BOTH

Precedence:
Historical accumulation candidate preceding a 256 candidate.
Lookback: {lookback} matched historical bars.
Current bar is excluded from preceding-event detection.

IMPORTANT:
This is descriptive research, not a profitability backtest.
A simultaneous or preceding pattern does not prove future gains.
No future candle, future return, or 30 percent outcome is used.

Source OHLCV and independent research reports are read only.
No trading or prediction is performed.
Final trading formula remains undefined.
"""

    path.write_text(content, encoding="utf-8")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=STAGE
    )

    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parent),
    )

    parser.add_argument(
        "--timeframes",
        nargs="+",
        choices=TIMEFRAMES,
        default=list(TIMEFRAMES),
    )

    parser.add_argument(
        "--lookback-bars",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    return parser.parse_args()


def main() -> int:
    args = arguments()

    if args.lookback_bars < 1 or args.lookback_bars > 448:
        raise ValueError(
            "lookback-bars must be between 1 and 448"
        )

    root = Path(args.project_root).resolve()

    if not root.is_dir():
        raise FileNotFoundError(
            f"Project root missing: {root}"
        )

    source256 = root / PATTERN_256_DETAIL
    source_acc = root / ACCUMULATION_DETAIL

    validate_source(source256, REQUIRED_256)
    validate_source(source_acc, REQUIRED_ACCUMULATION)

    before256 = sha256_file(source256)
    before_acc = sha256_file(source_acc)

    report_dir = (
        root / "data" / "reports" / REPORT_NAMESPACE
    )

    checkpoint_path = (
        root / "data" / "validation" / CHECKPOINT_FILE
    )

    report_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(
        parents=True, exist_ok=True
    )

    result_path = report_dir / RESULT_FILE
    summary_path = report_dir / SUMMARY_FILE
    detail_path = report_dir / DETAIL_FILE
    candidate_path = report_dir / CANDIDATE_FILE
    manifest_path = report_dir / MANIFEST_FILE
    contract_path = report_dir / CONTRACT_FILE
    readme_path = report_dir / README_FILE

    source_identity = {
        "pattern_256_sha256": before256,
        "accumulation_sha256": before_acc,
        "timeframes": sorted(set(args.timeframes)),
        "lookback_bars": args.lookback_bars,
        "program_version": VERSION,
    }

    # Checkpoint is only reusable when the source identity
    # and every expected output hash match.
    if checkpoint_path.is_file() and not args.force:
        try:
            previous = json.loads(
                checkpoint_path.read_text(
                    encoding="utf-8"
                )
            )

            expected = previous.get("output_sha256", {})

            output_files = {
                RESULT_FILE: result_path,
                SUMMARY_FILE: summary_path,
                DETAIL_FILE: detail_path,
                CANDIDATE_FILE: candidate_path,
                MANIFEST_FILE: manifest_path,
                CONTRACT_FILE: contract_path,
                README_FILE: readme_path,
            }

            valid = (
                previous.get("status") == "PASS"
                and previous.get("source_identity")
                == source_identity
                and len(expected) == len(output_files)
            )

            if valid:
                for name, path in output_files.items():
                    if (
                        not path.is_file()
                        or expected.get(name)
                        != sha256_file(path)
                    ):
                        valid = False
                        break

            if valid:
                log(
                    "[PASS] Checkpoint and output "
                    "SHA256 validated"
                )
                log("[SKIP] Identical completed research")
                return 0

        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
        ):
            log(
                "[INFO] Existing checkpoint invalid; "
                "recomputing"
            )

    log("=" * 64)
    log(PROGRAM_NAME)
    log(VERSION)
    log("=" * 64)

    log(f"[SOURCE] 256: {source256}")
    log(f"[SOURCE] Accumulation: {source_acc}")
    log(f"[LOOKBACK] {args.lookback_bars} bars")

    # Temporary SQLite database stays outside source folders.
    # It is removed automatically after processing.
    with tempfile.TemporaryDirectory(
        prefix="upbit_crosscheck_"
    ) as temporary_dir:
        database_path = (
            Path(temporary_dir) / "crosscheck.sqlite"
        )

        connection = sqlite3.connect(
            str(database_path)
        )

        try:
            initialize_database(connection)

            count256 = load_table(
                connection,
                source256,
                "pattern256",
                "pattern_256",
                set(args.timeframes),
            )

            count_acc = load_table(
                connection,
                source_acc,
                "accumulation",
                "accumulation_candle",
                set(args.timeframes),
            )

            statistics = crosscheck(
                connection,
                detail_path,
                candidate_path,
                summary_path,
                args.lookback_bars,
            )

        finally:
            connection.close()

    after256 = sha256_file(source256)
    after_acc = sha256_file(source_acc)

    if before256 != after256:
        raise RuntimeError(
            "Pattern 256 source SHA256 changed"
        )

    if before_acc != after_acc:
        raise RuntimeError(
            "Accumulation source SHA256 changed"
        )

    with manifest_path.open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "source",
                "path",
                "rows_loaded",
                "sha256_before",
                "sha256_after",
                "sha256_unchanged",
                "status",
            ],
        )

        writer.writeheader()

        for name, path, count, before, after in (
            (
                "PATTERN_256",
                source256,
                count256,
                before256,
                after256,
            ),
            (
                "ACCUMULATION_CANDLE",
                source_acc,
                count_acc,
                before_acc,
                after_acc,
            ),
        ):
            writer.writerow(
                {
                    "source": name,
                    "path": str(path),
                    "rows_loaded": count,
                    "sha256_before": before,
                    "sha256_after": after,
                    "sha256_unchanged": True,
                    "status": "PASS",
                }
            )

    contract = create_contract(args.lookback_bars)

    atomic_json(contract_path, contract)
    write_readme(readme_path, args.lookback_bars)

    result = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "stage": STAGE,
        "status": "PASS",
        "completed_at_utc": utc_now(),
        "pattern_ids": list(PATTERN_IDS),
        "source_identity": source_identity,
        "processing": {
            "pattern_256_rows_loaded": count256,
            "accumulation_rows_loaded": count_acc,
        },
        "statistics": statistics,
        "safety": {
            "ohlcv_read_only": True,
            "source_reports_read_only": True,
            "source_sha256_unchanged": True,
            "future_information_used": False,
            "future_labels_generated": False,
            "backtest_executed": False,
            "prediction_executed": False,
            "trading_executed": False,
        },
        "final_formula_defined": False,
    }

    atomic_json(result_path, result)

    output_files = {
        RESULT_FILE: result_path,
        SUMMARY_FILE: summary_path,
        DETAIL_FILE: detail_path,
        CANDIDATE_FILE: candidate_path,
        MANIFEST_FILE: manifest_path,
        CONTRACT_FILE: contract_path,
        README_FILE: readme_path,
    }

    checkpoint = {
        "checkpoint_version": 1,
        "program": PROGRAM_NAME,
        "version": VERSION,
        "status": "PASS",
        "completed_at_utc": utc_now(),
        "source_identity": source_identity,
        "output_sha256": {
            name: sha256_file(path)
            for name, path in output_files.items()
        },
    }

    atomic_json(checkpoint_path, checkpoint)

    log("")
    log("=" * 64)
    log("CROSSCHECK COMPLETED")
    log("=" * 64)

    for key, value in statistics.items():
        log(f"[RESULT] {key}: {value:,}")

    log("[PASS] Source SHA256 unchanged")
    log("[PASS] Checkpoint recorded")
    log("[PASS] Future information not used")
    log("[PASS] Final formula remains undefined")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        log("")
        log("CROSSCHECK FATAL ERROR")
        log(f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
        sys.exit(1)
