# ============================================================
# Upbit Surge Monitor
# audit_history_gap_recovery.py
# Clean V001
# ============================================================
#
# PURPOSE
# ------------------------------------------------------------
# Audit the evidence left by historical gap recovery.
#
# This program DOES NOT repair gaps.
# This program DOES NOT call the Upbit API.
# This program DOES NOT modify OHLCV source files.
#
# Main questions:
#
#   1. What recovery targets existed?
#   2. What recovery work was actually attempted?
#   3. What evidence exists for API-found candles?
#   4. What evidence exists for written candles?
#   5. What recovery targets are verifiable in current OHLCV?
#   6. Are recovery status/checkpoint/report records consistent?
#   7. Why did PRE and POST gap analysis remain unchanged?
#   8. Which jobs are safe retry candidates?
#   9. Which jobs require manual review?
#
# SAFETY
# ------------------------------------------------------------
# OHLCV write     : DISABLED
# OHLCV delete    : DISABLED
# API download    : DISABLED
# Gap repair      : DISABLED
# Feature build   : DISABLED
# 256 Detector    : DISABLED
# Future labels   : DISABLED
# Prediction      : DISABLED
# Trading         : DISABLED
# Git reset       : DISABLED
# Git clean       : DISABLED
# Git commit      : DISABLED
# Git push        : DISABLED
#
# RESUME
# ------------------------------------------------------------
# Persistent checkpoint is supported.
#
# Resume is allowed only when the current input fingerprint
# matches the checkpoint input fingerprint.
#
# A stale or incompatible checkpoint is NOT trusted.
#
# ============================================================

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


# ============================================================
# 1. VERSION / EXECUTION MODE
# ============================================================

PROGRAM_NAME = "audit_history_gap_recovery.py"
VERSION = "Clean V001"
EXECUTION_MODE = "READ_ONLY_RECOVERY_AUDIT"


# ============================================================
# 2. PROJECT CONSTANTS
# ============================================================

EXPECTED_MARKETS = 290
EXPECTED_TIMEFRAMES = 3
EXPECTED_OHLCV_FILES = EXPECTED_MARKETS * EXPECTED_TIMEFRAMES

TIMEFRAMES = ("h1", "h4", "d1")

CHECKPOINT_SAVE_INTERVAL = 25


# ============================================================
# 3. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"

OHLCV_ROOT = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_ROOT / "h1"
H4_DIR = OHLCV_ROOT / "h4"
D1_DIR = OHLCV_ROOT / "d1"

REPORTS_DIR = DATA_DIR / "reports"
VALIDATION_DIR = DATA_DIR / "validation"
RECOVERY_DIR = DATA_DIR / "recovery"

HISTORY_GAP_DIR = REPORTS_DIR / "history_gap"

POST_RECOVERY_AUDIT_DIR = (
    REPORTS_DIR / "post_recovery_gap_audit"
)

AUDIT_REPORT_DIR = (
    REPORTS_DIR / "history_gap_recovery_audit"
)

AUDIT_SUMMARY_PATH = (
    AUDIT_REPORT_DIR
    / "history_gap_recovery_audit_summary.csv"
)

AUDIT_DETAIL_PATH = (
    AUDIT_REPORT_DIR
    / "history_gap_recovery_audit_detail.csv"
)

AUDIT_RESULT_PATH = (
    AUDIT_REPORT_DIR
    / "history_gap_recovery_audit_result.json"
)

RETRY_CANDIDATES_PATH = (
    RECOVERY_DIR
    / "history_gap_recovery_retry_candidates.csv"
)

CHECKPOINT_PATH = (
    VALIDATION_DIR
    / "audit_history_gap_recovery_checkpoint.json"
)


# ============================================================
# 4. SAFETY DECLARATION
# ============================================================

SAFETY_FLAGS = {
    "ohlcv_write": False,
    "ohlcv_delete": False,
    "api_download": False,
    "gap_repair": False,
    "feature_build": False,
    "detector_256": False,
    "future_labels": False,
    "prediction": False,
    "trading": False,
    "git_reset": False,
    "git_clean": False,
    "git_commit": False,
    "git_push": False,
}


# ============================================================
# 5. COLUMN ALIASES
# ============================================================

MARKET_ALIASES = (
    "market",
    "ticker",
    "symbol",
    "code",
)

TIMEFRAME_ALIASES = (
    "timeframe",
    "interval",
    "tf",
    "unit",
)

STATUS_ALIASES = (
    "status",
    "result",
    "state",
    "recovery_status",
    "repair_status",
)

TIMESTAMP_ALIASES = (
    "timestamp",
    "time",
    "datetime",
    "date_time",
    "candle_time",
    "candle_date_time_utc",
    "candle_date_time_kst",
    "expected_timestamp",
    "missing_timestamp",
    "target_timestamp",
)

START_ALIASES = (
    "start",
    "start_time",
    "start_timestamp",
    "gap_start",
    "gap_start_time",
    "from_timestamp",
)

END_ALIASES = (
    "end",
    "end_time",
    "end_timestamp",
    "gap_end",
    "gap_end_time",
    "to_timestamp",
)

TARGET_COUNT_ALIASES = (
    "target_count",
    "targets",
    "missing_count",
    "gap_count",
    "requested_count",
    "expected_count",
)

ATTEMPT_COUNT_ALIASES = (
    "attempted_count",
    "attempt_count",
    "requested_count",
    "request_count",
    "processed_count",
)

API_FOUND_COUNT_ALIASES = (
    "api_found",
    "api_found_count",
    "found_count",
    "downloaded_count",
    "received_count",
    "fetched_count",
)

WRITTEN_COUNT_ALIASES = (
    "written",
    "written_count",
    "write_count",
    "added_count",
    "inserted_count",
    "recovered_count",
)

API_NOT_FOUND_COUNT_ALIASES = (
    "api_not_found",
    "api_not_found_count",
    "not_found_count",
    "unavailable_count",
)

FAILED_COUNT_ALIASES = (
    "failed",
    "failed_count",
    "error_count",
    "errors",
)

SKIPPED_COUNT_ALIASES = (
    "skipped",
    "skipped_count",
    "skip_count",
)

MESSAGE_ALIASES = (
    "message",
    "reason",
    "detail",
    "note",
    "error",
    "error_message",
)


# ============================================================
# 6. STATUS KEYWORDS
# ============================================================

SUCCESS_WORDS = (
    "success",
    "completed",
    "complete",
    "done",
    "recovered",
    "repaired",
    "written",
    "ok",
    "pass",
)

FAILED_WORDS = (
    "failed",
    "failure",
    "error",
    "exception",
)

SKIPPED_WORDS = (
    "skip",
    "skipped",
    "already_exists",
    "already exists",
)

ATTEMPT_WORDS = (
    "attempt",
    "requested",
    "request",
    "processing",
    "processed",
    "fetch",
    "download",
    "repair",
    "recover",
)

API_NOT_FOUND_WORDS = (
    "api_not_found",
    "not found",
    "not_found",
    "unavailable",
    "no candle",
    "no_candle",
    "empty response",
    "empty_response",
)


# ============================================================
# 7. DATA CLASSES
# ============================================================

@dataclass
class CsvTable:
    path: Path
    fieldnames: list[str]
    rows: list[dict[str, str]]
    normalized_fields: dict[str, str]


@dataclass
class RecoveryEvidence:
    market: str
    timeframe: str

    targeted: bool = False
    attempted: bool = False
    api_found: bool = False
    written: bool = False
    verified: bool = False

    target_count: int = 0
    attempted_count: int = 0
    api_found_count: int = 0
    written_count: int = 0
    api_not_found_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0

    target_timestamps: set[str] = field(
        default_factory=set
    )

    current_verified_timestamps: set[str] = field(
        default_factory=set
    )

    status_values: list[str] = field(
        default_factory=list
    )

    messages: list[str] = field(
        default_factory=list
    )

    sources: set[str] = field(
        default_factory=set
    )

    checkpoint_complete: Optional[bool] = None

    classification: str = "INSUFFICIENT_EVIDENCE"
    retry_candidate: bool = False
    manual_review: bool = False

    conflict_reason: str = ""


# ============================================================
# 8. GENERAL UTILITIES
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    )


def print_header(title: str) -> None:
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()

    chars = []

    for ch in text:
        if ch.isalnum():
            chars.append(ch)
        else:
            chars.append("_")

    normalized = "".join(chars)

    while "__" in normalized:
        normalized = normalized.replace("__", "_")

    return normalized.strip("_")


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def normalize_market(value: Any) -> str:
    value = clean_text(value).upper()

    if not value:
        return ""

    value = value.replace("_", "-")
    value = value.replace("/", "-")

    if value.startswith("KRW") and "-" not in value:
        if len(value) > 3:
            value = "KRW-" + value[3:]

    return value


def normalize_timeframe(value: Any) -> str:
    value = normalize_name(value)

    aliases = {
        "1h": "h1",
        "h1": "h1",
        "60m": "h1",
        "60min": "h1",
        "minute60": "h1",

        "4h": "h4",
        "h4": "h4",
        "240m": "h4",
        "240min": "h4",
        "minute240": "h4",

        "1d": "d1",
        "d1": "d1",
        "day": "d1",
        "daily": "d1",
        "days": "d1",
    }

    return aliases.get(value, value)


def safe_int(value: Any) -> int:
    if value is None:
        return 0

    text = str(value).strip()

    if not text:
        return 0

    try:
        return int(float(text.replace(",", "")))
    except Exception:
        return 0


def contains_any(
    value: str,
    keywords: Iterable[str],
) -> bool:
    value = value.lower()

    return any(
        keyword.lower() in value
        for keyword in keywords
    )


def ensure_runtime_directories() -> None:
    AUDIT_REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    VALIDATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    RECOVERY_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


# ============================================================
# 9. HASH UTILITIES
# ============================================================

def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()

    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)

            if not chunk:
                break

            hasher.update(chunk)

    return hasher.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(
        text.encode("utf-8")
    ).hexdigest()


# ============================================================
# 10. CSV UTILITIES
# ============================================================

def detect_csv_encoding(path: Path) -> str:
    candidates = (
        "utf-8-sig",
        "utf-8",
        "cp949",
        "euc-kr",
    )

    for encoding in candidates:
        try:
            with path.open(
                "r",
                encoding=encoding,
                newline="",
            ) as f:
                f.read(4096)

            return encoding

        except UnicodeDecodeError:
            continue

    return "utf-8"


def load_csv_table(path: Path) -> CsvTable:
    encoding = detect_csv_encoding(path)

    with path.open(
        "r",
        encoding=encoding,
        newline="",
    ) as f:

        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError(
                f"CSV has no header: {path}"
            )

        fieldnames = [
            clean_text(x)
            for x in reader.fieldnames
        ]

        rows: list[dict[str, str]] = []

        for raw in reader:
            row: dict[str, str] = {}

            for key, value in raw.items():
                if key is None:
                    continue

                row[clean_text(key)] = clean_text(value)

            rows.append(row)

    normalized_fields = {
        normalize_name(name): name
        for name in fieldnames
    }

    return CsvTable(
        path=path,
        fieldnames=fieldnames,
        rows=rows,
        normalized_fields=normalized_fields,
    )


def find_column(
    table: CsvTable,
    aliases: Iterable[str],
) -> Optional[str]:

    for alias in aliases:
        normalized = normalize_name(alias)

        if normalized in table.normalized_fields:
            return table.normalized_fields[normalized]

    return None


def get_row_value(
    row: dict[str, str],
    column: Optional[str],
) -> str:

    if not column:
        return ""

    return clean_text(row.get(column, ""))


# ============================================================
# 11. JSON UTILITIES
# ============================================================

def load_json(path: Path) -> Any:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as f:
        return json.load(f)


def write_json(
    path: Path,
    payload: Any,
) -> None:

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:

        json.dump(
            payload,
            f,
            ensure_ascii=False,
            indent=2,
            sort_keys=False,
        )

        f.write("\n")


# ============================================================
# 12. CSV WRITER
# ============================================================

def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


# ============================================================
# 13. OHLCV FILE DISCOVERY
# ============================================================

def list_csv_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []

    return sorted(
        path
        for path in directory.glob("*.csv")
        if path.is_file()
    )


def discover_ohlcv_inventory() -> dict[str, list[Path]]:
    inventory = {
        "h1": list_csv_files(H1_DIR),
        "h4": list_csv_files(H4_DIR),
        "d1": list_csv_files(D1_DIR),
    }

    return inventory


def verify_ohlcv_inventory(
    inventory: dict[str, list[Path]],
) -> None:

    print_header("OHLCV INVENTORY")

    total = 0

    for timeframe in TIMEFRAMES:
        count = len(inventory[timeframe])

        print(
            f"{timeframe.upper()} files : {count}"
        )

        total += count

    print(f"Total    : {total}")

    if total != EXPECTED_OHLCV_FILES:
        raise RuntimeError(
            "OHLCV file count mismatch. "
            f"expected={EXPECTED_OHLCV_FILES}, "
            f"found={total}"
        )

    for timeframe in TIMEFRAMES:
        count = len(inventory[timeframe])

        if count != EXPECTED_MARKETS:
            raise RuntimeError(
                f"{timeframe.upper()} market count mismatch. "
                f"expected={EXPECTED_MARKETS}, "
                f"found={count}"
            )


# ============================================================
# 14. MARKET FROM OHLCV FILE NAME
# ============================================================

def market_from_ohlcv_path(path: Path) -> str:
    stem = path.stem.upper()

    stem = stem.replace("_", "-")

    if stem.startswith("KRW-"):
        return stem

    if stem.startswith("KRW") and len(stem) > 3:
        return "KRW-" + stem[3:].lstrip("-")

    return stem


# ============================================================
# 15. OHLCV TIMESTAMP COLUMN DETECTION
# ============================================================

def detect_timestamp_column(
    table: CsvTable,
) -> Optional[str]:

    column = find_column(
        table,
        TIMESTAMP_ALIASES,
    )

    if column:
        return column

    for name in table.fieldnames:
        normalized = normalize_name(name)

        if (
            "timestamp" in normalized
            or "date_time" in normalized
            or "datetime" in normalized
            or "candle_date_time" in normalized
        ):
            return name

    return None


# ============================================================
# 16. OHLCV STRUCTURAL VALIDATION
# ============================================================

def validate_ohlcv_file(
    path: Path,
) -> dict[str, Any]:

    table = load_csv_table(path)

    timestamp_column = detect_timestamp_column(
        table
    )

    if not timestamp_column:
        raise RuntimeError(
            f"Unable to detect timestamp column: {path}"
        )

    timestamps: list[str] = []

    for row in table.rows:
        value = get_row_value(
            row,
            timestamp_column,
        )

        if value:
            timestamps.append(value)

    if not timestamps:
        raise RuntimeError(
            f"No timestamps found in OHLCV file: {path}"
        )

    unique_count = len(set(timestamps))

    duplicate_count = (
        len(timestamps) - unique_count
    )

    if duplicate_count != 0:
        raise RuntimeError(
            "Duplicate timestamp detected. "
            f"file={path}, "
            f"duplicates={duplicate_count}"
        )

    return {
        "path": str(path),
        "rows": len(table.rows),
        "timestamp_column": timestamp_column,
        "timestamps": set(timestamps),
    }


# ============================================================
# 17. GAP REPORT VALIDATION
# ============================================================

def gap_report_score(
    table: CsvTable,
) -> tuple[int, str]:

    market_col = find_column(
        table,
        MARKET_ALIASES,
    )

    timeframe_col = find_column(
        table,
        TIMEFRAME_ALIASES,
    )

    gap_like_columns = []

    for name in table.fieldnames:
        normalized = normalize_name(name)

        if (
            "gap" in normalized
            or "missing" in normalized
        ):
            gap_like_columns.append(name)

    score = 0

    if market_col:
        score += 5

    if timeframe_col:
        score += 5

    if gap_like_columns:
        score += 10

    if table.rows:
        score += 2

    if score >= 12:
        return score, "valid-gap-report"

    return score, "insufficient-gap-schema"


# ============================================================
# 18. PRE REPORT DISCOVERY
# ============================================================

def discover_pre_gap_report() -> Optional[Path]:
    print_header(
        "DISCOVER PRE-RECOVERY GAP REPORT"
    )

    preferred = [
        HISTORY_GAP_DIR
        / "data_history_gap_summary.csv",

        HISTORY_GAP_DIR
        / "data_history_gap_report.csv",
    ]

    candidates: list[Path] = []

    for path in preferred:
        if path.exists():
            candidates.append(path)

    if HISTORY_GAP_DIR.exists():
        for path in sorted(
            HISTORY_GAP_DIR.glob("*.csv")
        ):
            if path not in candidates:
                candidates.append(path)

    best_path: Optional[Path] = None
    best_score = -1

    for path in candidates:
        try:
            table = load_csv_table(path)

            score, reason = gap_report_score(
                table
            )

            if score >= 12:
                print(
                    f"[ACCEPT] PRE: {path}"
                )
                print(
                    f"         reason: {reason}"
                )

                if score > best_score:
                    best_score = score
                    best_path = path

            else:
                print(
                    f"[REJECT] PRE: {path}"
                )
                print(
                    f"         reason: {reason}"
                )

        except Exception as exc:
            print(
                f"[REJECT] PRE: {path}"
            )
            print(
                f"         reason: {exc}"
            )

    if best_path:
        print(
            f"[SELECTED PRE] {best_path}"
        )

    else:
        print(
            "[INFO] No validated PRE gap report found."
        )

    return best_path


# ============================================================
# 19. POST REPORT DISCOVERY
# ============================================================

def discover_post_gap_report() -> Optional[Path]:
    print_header(
        "DISCOVER POST-RECOVERY GAP REPORT"
    )

    preferred = [
        POST_RECOVERY_AUDIT_DIR
        / "data_history_gap_summary.csv",

        POST_RECOVERY_AUDIT_DIR
        / "data_history_gap_report.csv",
    ]

    candidates: list[Path] = []

    for path in preferred:
        if path.exists():
            candidates.append(path)

    if POST_RECOVERY_AUDIT_DIR.exists():
        for path in sorted(
            POST_RECOVERY_AUDIT_DIR.glob("*.csv")
        ):
            if path not in candidates:
                candidates.append(path)

    best_path: Optional[Path] = None
    best_score = -1

    for path in candidates:
        try:
            table = load_csv_table(path)

            score, reason = gap_report_score(
                table
            )

            if score >= 12:
                print(
                    f"[ACCEPT] POST: {path}"
                )
                print(
                    f"          reason: {reason}"
                )

                if score > best_score:
                    best_score = score
                    best_path = path

            else:
                print(
                    f"[REJECT] POST: {path}"
                )
                print(
                    f"          reason: {reason}"
                )

        except Exception as exc:
            print(
                f"[REJECT] POST: {path}"
            )
            print(
                f"          reason: {exc}"
            )

    if best_path:
        print(
            f"[SELECTED POST] {best_path}"
        )

    else:
        print(
            "[INFO] No validated POST gap report found."
        )

    return best_path


# ============================================================
# 20. GENERIC RECOVERY CSV DISCOVERY
# ============================================================

def recovery_csv_candidates() -> list[Path]:
    candidates: set[Path] = set()

    roots = [
        DATA_DIR,
        RECOVERY_DIR,
        REPORTS_DIR,
        VALIDATION_DIR,
    ]

    for root in roots:
        if not root.exists():
            continue

        try:
            for path in root.rglob("*.csv"):
                if not path.is_file():
                    continue

                normalized = normalize_name(
                    path.name
                )

                if any(
                    keyword in normalized
                    for keyword in (
                        "recover",
                        "recovery",
                        "repair",
                    )
                ):
                    candidates.add(path)

        except OSError:
            continue

    return sorted(candidates)


# ============================================================
# 21. RECOVERY TABLE SCORING
# ============================================================

def recovery_table_score(
    table: CsvTable,
) -> tuple[int, dict[str, Optional[str]]]:

    columns = {
        "market": find_column(
            table,
            MARKET_ALIASES,
        ),

        "timeframe": find_column(
            table,
            TIMEFRAME_ALIASES,
        ),

        "status": find_column(
            table,
            STATUS_ALIASES,
        ),

        "timestamp": find_column(
            table,
            TIMESTAMP_ALIASES,
        ),

        "start": find_column(
            table,
            START_ALIASES,
        ),

        "end": find_column(
            table,
            END_ALIASES,
        ),

        "target_count": find_column(
            table,
            TARGET_COUNT_ALIASES,
        ),

        "attempted_count": find_column(
            table,
            ATTEMPT_COUNT_ALIASES,
        ),

        "api_found_count": find_column(
            table,
            API_FOUND_COUNT_ALIASES,
        ),

        "written_count": find_column(
            table,
            WRITTEN_COUNT_ALIASES,
        ),

        "api_not_found_count": find_column(
            table,
            API_NOT_FOUND_COUNT_ALIASES,
        ),

        "failed_count": find_column(
            table,
            FAILED_COUNT_ALIASES,
        ),

        "skipped_count": find_column(
            table,
            SKIPPED_COUNT_ALIASES,
        ),

        "message": find_column(
            table,
            MESSAGE_ALIASES,
        ),
    }

    score = 0

    if columns["market"]:
        score += 5

    if columns["timeframe"]:
        score += 5

    if columns["status"]:
        score += 4

    if columns["timestamp"]:
        score += 4

    if columns["start"]:
        score += 2

    if columns["end"]:
        score += 2

    if columns["target_count"]:
        score += 2

    if columns["attempted_count"]:
        score += 2

    if columns["api_found_count"]:
        score += 3

    if columns["written_count"]:
        score += 3

    if columns["api_not_found_count"]:
        score += 3

    if columns["failed_count"]:
        score += 2

    if columns["skipped_count"]:
        score += 1

    if columns["message"]:
        score += 1

    if table.rows:
        score += 1

    return score, columns


# ============================================================
# 22. RECOVERY TABLE DISCOVERY
# ============================================================

def discover_recovery_tables() -> list[
    tuple[CsvTable, dict[str, Optional[str]]]
]:

    print_header(
        "DISCOVER RECOVERY EVIDENCE TABLES"
    )

    accepted: list[
        tuple[
            CsvTable,
            dict[str, Optional[str]],
        ]
    ] = []

    for path in recovery_csv_candidates():
        try:
            table = load_csv_table(path)

            score, columns = recovery_table_score(
                table
            )

            if (
                score >= 11
                and columns["market"]
                and columns["timeframe"]
            ):
                print(
                    f"[ACCEPT] {path}"
                )
                print(
                    f"         score={score}"
                )

                accepted.append(
                    (table, columns)
                )

            else:
                print(
                    f"[REJECT] {path}"
                )
                print(
                    f"         score={score}"
                )

        except Exception as exc:
            print(
                f"[REJECT] {path}"
            )
            print(
                f"         reason={exc}"
            )

    print()
    print(
        "Accepted recovery evidence tables : "
        f"{len(accepted)}"
    )

    return accepted


# ============================================================
# 23. JOB KEY
# ============================================================

def make_job_key(
    market: str,
    timeframe: str,
) -> str:

    return (
        f"{normalize_market(market)}"
        f"|{normalize_timeframe(timeframe)}"
    )


# ============================================================
# 24. CREATE / GET EVIDENCE
# ============================================================

def get_evidence(
    evidence_map: dict[str, RecoveryEvidence],
    market: str,
    timeframe: str,
) -> Optional[RecoveryEvidence]:

    market = normalize_market(market)
    timeframe = normalize_timeframe(timeframe)

    if not market:
        return None

    if timeframe not in TIMEFRAMES:
        return None

    key = make_job_key(
        market,
        timeframe,
    )

    if key not in evidence_map:
        evidence_map[key] = RecoveryEvidence(
            market=market,
            timeframe=timeframe,
        )

    return evidence_map[key]


# ============================================================
# 25. TARGET FILE DETECTION
# ============================================================

def looks_like_target_file(path: Path) -> bool:
    name = normalize_name(path.name)

    return (
        "target" in name
        or "recovery_plan" in name
    )


# ============================================================
# 26. INGEST RECOVERY CSV TABLE
# ============================================================

def ingest_recovery_table(
    table: CsvTable,
    columns: dict[str, Optional[str]],
    evidence_map: dict[str, RecoveryEvidence],
) -> None:

    is_target_file = looks_like_target_file(
        table.path
    )

    for row in table.rows:
        market = get_row_value(
            row,
            columns["market"],
        )

        timeframe = get_row_value(
            row,
            columns["timeframe"],
        )

        evidence = get_evidence(
            evidence_map,
            market,
            timeframe,
        )

        if evidence is None:
            continue

        evidence.sources.add(
            str(table.path)
        )

        status = get_row_value(
            row,
            columns["status"],
        )

        message = get_row_value(
            row,
            columns["message"],
        )

        timestamp = get_row_value(
            row,
            columns["timestamp"],
        )

        target_count = safe_int(
            get_row_value(
                row,
                columns["target_count"],
            )
        )

        attempted_count = safe_int(
            get_row_value(
                row,
                columns["attempted_count"],
            )
        )

        api_found_count = safe_int(
            get_row_value(
                row,
                columns["api_found_count"],
            )
        )

        written_count = safe_int(
            get_row_value(
                row,
                columns["written_count"],
            )
        )

        api_not_found_count = safe_int(
            get_row_value(
                row,
                columns["api_not_found_count"],
            )
        )

        failed_count = safe_int(
            get_row_value(
                row,
                columns["failed_count"],
            )
        )

        skipped_count = safe_int(
            get_row_value(
                row,
                columns["skipped_count"],
            )
        )

        combined_text = (
            f"{status} {message}"
        ).lower()

        if is_target_file:
            evidence.targeted = True

        if target_count > 0:
            evidence.targeted = True

        if timestamp:
            evidence.target_timestamps.add(
                timestamp
            )

            if is_target_file:
                evidence.targeted = True

        if status:
            evidence.status_values.append(
                status
            )

        if message:
            evidence.messages.append(
                message
            )

        evidence.target_count += target_count
        evidence.attempted_count += attempted_count
        evidence.api_found_count += api_found_count
        evidence.written_count += written_count
        evidence.api_not_found_count += (
            api_not_found_count
        )
        evidence.failed_count += failed_count
        evidence.skipped_count += skipped_count

        if attempted_count > 0:
            evidence.attempted = True

        if api_found_count > 0:
            evidence.api_found = True
            evidence.attempted = True

        if written_count > 0:
            evidence.written = True
            evidence.api_found = True
            evidence.attempted = True

        if api_not_found_count > 0:
            evidence.attempted = True

        if failed_count > 0:
            evidence.attempted = True

        if contains_any(
            combined_text,
            ATTEMPT_WORDS,
        ):
            evidence.attempted = True

        if contains_any(
            combined_text,
            SUCCESS_WORDS,
        ):
            evidence.attempted = True

        if contains_any(
            combined_text,
            API_NOT_FOUND_WORDS,
        ):
            evidence.attempted = True

            if evidence.api_not_found_count == 0:
                evidence.api_not_found_count += 1

        if contains_any(
            combined_text,
            FAILED_WORDS,
        ):
            evidence.attempted = True

            if evidence.failed_count == 0:
                evidence.failed_count += 1

        if contains_any(
            combined_text,
            SKIPPED_WORDS,
        ):
            if evidence.skipped_count == 0:
                evidence.skipped_count += 1


# ============================================================
# 27. CHECKPOINT FILE DISCOVERY
# ============================================================

def recovery_checkpoint_candidates() -> list[Path]:
    candidates: set[Path] = set()

    roots = [
        VALIDATION_DIR,
        RECOVERY_DIR,
        DATA_DIR,
    ]

    for root in roots:
        if not root.exists():
            continue

        try:
            for path in root.rglob("*.json"):
                name = normalize_name(
                    path.name
                )

                if (
                    "recover" in name
                    or "recovery" in name
                    or "repair" in name
                ) and (
                    "checkpoint" in name
                    or "progress" in name
                    or "status" in name
                ):
                    candidates.add(path)

        except OSError:
            continue

    return sorted(candidates)


# ============================================================
# 28. RECURSIVE CHECKPOINT OBJECT WALKER
# ============================================================

def walk_json_objects(
    value: Any,
) -> Iterable[dict[str, Any]]:

    if isinstance(value, dict):
        yield value

        for child in value.values():
            yield from walk_json_objects(
                child
            )

    elif isinstance(value, list):
        for child in value:
            yield from walk_json_objects(
                child
            )


# ============================================================
# 29. DICT ALIAS VALUE
# ============================================================

def dict_alias_value(
    obj: dict[str, Any],
    aliases: Iterable[str],
) -> Any:

    normalized_map = {
        normalize_name(key): key
        for key in obj.keys()
    }

    for alias in aliases:
        normalized = normalize_name(alias)

        if normalized in normalized_map:
            return obj[
                normalized_map[normalized]
            ]

    return None


# ============================================================
# 30. CHECKPOINT COMPLETE DETECTION
# ============================================================

def checkpoint_complete_value(
    obj: dict[str, Any],
) -> Optional[bool]:

    status = dict_alias_value(
        obj,
        STATUS_ALIASES,
    )

    if status is not None:
        text = clean_text(status).lower()

        if contains_any(
            text,
            SUCCESS_WORDS,
        ):
            return True

        if contains_any(
            text,
            FAILED_WORDS,
        ):
            return False

    for alias in (
        "complete",
        "completed",
        "done",
        "success",
        "processed",
    ):
        value = dict_alias_value(
            obj,
            (alias,),
        )

        if isinstance(value, bool):
            return value

    return None


# ============================================================
# 31. INGEST CHECKPOINTS
# ============================================================

def ingest_recovery_checkpoints(
    evidence_map: dict[str, RecoveryEvidence],
) -> list[str]:

    print_header(
        "DISCOVER RECOVERY CHECKPOINTS"
    )

    used: list[str] = []

    for path in recovery_checkpoint_candidates():
        try:
            payload = load_json(path)

        except Exception as exc:
            print(
                f"[REJECT] {path}"
            )
            print(
                f"         reason={exc}"
            )
            continue

        matched = 0

        for obj in walk_json_objects(payload):
            market = dict_alias_value(
                obj,
                MARKET_ALIASES,
            )

            timeframe = dict_alias_value(
                obj,
                TIMEFRAME_ALIASES,
            )

            if market is None or timeframe is None:
                continue

            evidence = get_evidence(
                evidence_map,
                clean_text(market),
                clean_text(timeframe),
            )

            if evidence is None:
                continue

            evidence.sources.add(
                str(path)
            )

            complete = checkpoint_complete_value(
                obj
            )

            if complete is not None:
                if evidence.checkpoint_complete is None:
                    evidence.checkpoint_complete = (
                        complete
                    )

                elif (
                    evidence.checkpoint_complete
                    != complete
                ):
                    evidence.conflict_reason = (
                        "Conflicting checkpoint "
                        "completion states."
                    )

                    evidence.manual_review = True

            matched += 1

        if matched:
            print(
                f"[ACCEPT] {path}"
            )
            print(
                f"         matched_jobs={matched}"
            )

            used.append(str(path))

        else:
            print(
                f"[REJECT] {path}"
            )
            print(
                "         reason=no market/timeframe "
                "checkpoint objects"
            )

    return used


# ============================================================
# 32. OHLCV CACHE
# ============================================================

def build_ohlcv_cache(
    inventory: dict[str, list[Path]],
) -> dict[str, dict[str, Any]]:

    print_header(
        "VALIDATE CURRENT OHLCV"
    )

    cache: dict[str, dict[str, Any]] = {}

    processed = 0

    for timeframe in TIMEFRAMES:
        for path in inventory[timeframe]:
            result = validate_ohlcv_file(
                path
            )

            market = market_from_ohlcv_path(
                path
            )

            key = make_job_key(
                market,
                timeframe,
            )

            cache[key] = {
                "market": market,
                "timeframe": timeframe,
                "path": str(path),
                "rows": result["rows"],
                "timestamp_column": (
                    result["timestamp_column"]
                ),
                "timestamps": result["timestamps"],
            }

            processed += 1

            if (
                processed % 50 == 0
                or processed == EXPECTED_OHLCV_FILES
            ):
                print(
                    "[OHLCV] "
                    f"{processed}/"
                    f"{EXPECTED_OHLCV_FILES}"
                )

    if len(cache) != EXPECTED_OHLCV_FILES:
        raise RuntimeError(
            "OHLCV cache job count mismatch. "
            f"expected={EXPECTED_OHLCV_FILES}, "
            f"found={len(cache)}"
        )

    return cache


# ============================================================
# 33. VERIFY TARGET TIMESTAMPS IN CURRENT OHLCV
# ============================================================

def verify_target_timestamps(
    evidence_map: dict[str, RecoveryEvidence],
    ohlcv_cache: dict[str, dict[str, Any]],
) -> None:

    print_header(
        "VERIFY RECOVERY TARGETS IN CURRENT OHLCV"
    )

    checked_jobs = 0
    checked_timestamps = 0
    verified_timestamps = 0

    for key, evidence in sorted(
        evidence_map.items()
    ):
        if not evidence.target_timestamps:
            continue

        checked_jobs += 1

        ohlcv = ohlcv_cache.get(key)

        if not ohlcv:
            evidence.conflict_reason = (
                "Current OHLCV file not found "
                "for recovery job."
            )

            evidence.manual_review = True
            continue

        current_timestamps = ohlcv[
            "timestamps"
        ]

        for timestamp in evidence.target_timestamps:
            checked_timestamps += 1

            if timestamp in current_timestamps:
                evidence.current_verified_timestamps.add(
                    timestamp
                )

                verified_timestamps += 1

        if (
            evidence.target_timestamps
            and evidence.current_verified_timestamps
        ):
            evidence.verified = True

    print(
        f"Jobs with explicit target timestamps : "
        f"{checked_jobs}"
    )

    print(
        f"Target timestamps checked            : "
        f"{checked_timestamps}"
    )

    print(
        f"Target timestamps present            : "
        f"{verified_timestamps}"
    )


# ============================================================
# 34. PRE / POST REPORT HASH AUDIT
# ============================================================

def audit_pre_post_reports(
    pre_path: Optional[Path],
    post_path: Optional[Path],
) -> dict[str, Any]:

    print_header(
        "AUDIT PRE / POST GAP REPORTS"
    )

    result = {
        "pre_path": (
            str(pre_path)
            if pre_path
            else None
        ),

        "post_path": (
            str(post_path)
            if post_path
            else None
        ),

        "pre_sha256": None,
        "post_sha256": None,
        "sha256_equal": None,
    }

    if pre_path:
        result["pre_sha256"] = (
            sha256_file(pre_path)
        )

        print(
            "PRE SHA256  : "
            f"{result['pre_sha256']}"
        )

    else:
        print(
            "PRE SHA256  : NOT AVAILABLE"
        )

    if post_path:
        result["post_sha256"] = (
            sha256_file(post_path)
        )

        print(
            "POST SHA256 : "
            f"{result['post_sha256']}"
        )

    else:
        print(
            "POST SHA256 : NOT AVAILABLE"
        )

    if pre_path and post_path:
        result["sha256_equal"] = (
            result["pre_sha256"]
            == result["post_sha256"]
        )

        print(
            "Equal       : "
            f"{'YES' if result['sha256_equal'] else 'NO'}"
        )

    else:
        print(
            "Equal       : UNKNOWN"
        )

    return result


# ============================================================
# 35. INPUT FINGERPRINT
# ============================================================

def build_input_fingerprint(
    inventory: dict[str, list[Path]],
    recovery_tables: list[
        tuple[
            CsvTable,
            dict[str, Optional[str]],
        ]
    ],
    checkpoint_paths: list[str],
    pre_path: Optional[Path],
    post_path: Optional[Path],
) -> str:

    records: list[str] = []

    records.append(
        f"program={PROGRAM_NAME}"
    )

    records.append(
        f"version={VERSION}"
    )

    for timeframe in TIMEFRAMES:
        for path in inventory[timeframe]:
            stat = path.stat()

            records.append(
                "OHLCV|"
                f"{timeframe}|"
                f"{path.name}|"
                f"{stat.st_size}|"
                f"{stat.st_mtime_ns}"
            )

    for table, _columns in recovery_tables:
        path = table.path

        records.append(
            "RECOVERY|"
            f"{path}|"
            f"{sha256_file(path)}"
        )

    for path_string in checkpoint_paths:
        path = Path(path_string)

        if path.exists():
            records.append(
                "CHECKPOINT|"
                f"{path}|"
                f"{sha256_file(path)}"
            )

    if pre_path and pre_path.exists():
        records.append(
            "PRE|"
            f"{pre_path}|"
            f"{sha256_file(pre_path)}"
        )

    if post_path and post_path.exists():
        records.append(
            "POST|"
            f"{post_path}|"
            f"{sha256_file(post_path)}"
        )

    records.sort()

    return sha256_text(
        "\n".join(records)
    )


# ============================================================
# 36. LOAD AUDIT CHECKPOINT
# ============================================================

def load_audit_checkpoint() -> Optional[
    dict[str, Any]
]:

    if not CHECKPOINT_PATH.exists():
        return None

    try:
        payload = load_json(
            CHECKPOINT_PATH
        )

    except Exception as exc:
        print(
            "[CHECKPOINT] Existing checkpoint "
            f"is unreadable: {exc}"
        )

        return None

    if (
        payload.get("program")
        != PROGRAM_NAME
    ):
        return None

    if (
        payload.get("version")
        != VERSION
    ):
        return None

    return payload


# ============================================================
# 37. SAVE AUDIT CHECKPOINT
# ============================================================

def save_audit_checkpoint(
    input_fingerprint: str,
    processed_keys: set[str],
    complete: bool,
) -> None:

    payload = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "execution_mode": EXECUTION_MODE,
        "updated_utc": utc_now_iso(),
        "input_fingerprint": input_fingerprint,
        "processed_jobs": sorted(
            processed_keys
        ),
        "processed_job_count": len(
            processed_keys
        ),
        "complete": complete,
    }

    write_json(
        CHECKPOINT_PATH,
        payload,
    )


# ============================================================
# 38. RESUME STATE
# ============================================================

def resolve_resume_state(
    input_fingerprint: str,
) -> set[str]:

    checkpoint = load_audit_checkpoint()

    if not checkpoint:
        print(
            "[RESUME] No compatible audit checkpoint."
        )

        return set()

    previous_fingerprint = clean_text(
        checkpoint.get(
            "input_fingerprint",
            "",
        )
    )

    if (
        previous_fingerprint
        != input_fingerprint
    ):
        print(
            "[RESUME] Existing checkpoint is stale."
        )
        print(
            "[RESUME] Input fingerprint changed."
        )
        print(
            "[RESUME] Starting fresh audit state."
        )

        return set()

    processed = checkpoint.get(
        "processed_jobs",
        [],
    )

    if not isinstance(processed, list):
        return set()

    processed_set = {
        clean_text(x)
        for x in processed
        if clean_text(x)
    }

    print(
        "[RESUME] Compatible checkpoint found."
    )

    print(
        "[RESUME] Previously processed jobs : "
        f"{len(processed_set)}"
    )

    return processed_set


# ============================================================
# 39. CLASSIFICATION HELPERS
# ============================================================

def status_claims_success(
    evidence: RecoveryEvidence,
) -> bool:

    text = " ".join(
        evidence.status_values
        + evidence.messages
    )

    return contains_any(
        text,
        SUCCESS_WORDS,
    )


def status_claims_failure(
    evidence: RecoveryEvidence,
) -> bool:

    text = " ".join(
        evidence.status_values
        + evidence.messages
    )

    return contains_any(
        text,
        FAILED_WORDS,
    )


# ============================================================
# 40. CLASSIFY ONE JOB
# ============================================================

def classify_evidence(
    evidence: RecoveryEvidence,
) -> None:

    evidence.retry_candidate = False

    # --------------------------------------------------------
    # Explicit checkpoint conflict
    # --------------------------------------------------------

    if evidence.conflict_reason:
        evidence.classification = (
            "CHECKPOINT_CONFLICT"
        )

        evidence.manual_review = True
        return

    # --------------------------------------------------------
    # Status says success but evidence says nothing was written
    # --------------------------------------------------------

    if (
        status_claims_success(evidence)
        and evidence.written_count > 0
        and evidence.target_timestamps
        and not evidence.current_verified_timestamps
    ):
        evidence.classification = (
            "STATUS_CONFLICT"
        )

        evidence.conflict_reason = (
            "Recovery record claims written candles, "
            "but explicit target timestamps are not "
            "present in current OHLCV."
        )

        evidence.manual_review = True
        return

    # --------------------------------------------------------
    # Fully verified using explicit target timestamps
    # --------------------------------------------------------

    if evidence.target_timestamps:
        target_count = len(
            evidence.target_timestamps
        )

        verified_count = len(
            evidence.current_verified_timestamps
        )

        if (
            target_count > 0
            and verified_count == target_count
        ):
            evidence.classification = (
                "VERIFIED_RECOVERED"
            )

            evidence.verified = True
            return

        if verified_count > 0:
            evidence.classification = (
                "PARTIALLY_RECOVERED"
            )

            evidence.verified = True
            evidence.retry_candidate = True
            return

    # --------------------------------------------------------
    # Explicit API not-found evidence
    # --------------------------------------------------------

    if (
        evidence.api_not_found_count > 0
        and evidence.written_count == 0
        and not evidence.verified
    ):
        evidence.classification = (
            "API_NOT_FOUND"
        )

        evidence.retry_candidate = False
        return

    # --------------------------------------------------------
    # Attempted + API found + no write
    # --------------------------------------------------------

    if (
        evidence.attempted
        and evidence.api_found
        and not evidence.written
        and not evidence.verified
    ):
        evidence.classification = (
            "ATTEMPTED_NOT_WRITTEN"
        )

        evidence.retry_candidate = True
        return

    # --------------------------------------------------------
    # Failed attempt
    # --------------------------------------------------------

    if (
        evidence.attempted
        and (
            evidence.failed_count > 0
            or status_claims_failure(evidence)
        )
    ):
        evidence.classification = (
            "RETRY_CANDIDATE"
        )

        evidence.retry_candidate = True
        return

    # --------------------------------------------------------
    # Targeted but no attempt evidence
    # --------------------------------------------------------

    if (
        evidence.targeted
        and not evidence.attempted
    ):
        evidence.classification = (
            "NOT_ATTEMPTED"
        )

        evidence.retry_candidate = True
        return

    # --------------------------------------------------------
    # Attempted but no measurable recovery evidence
    # --------------------------------------------------------

    if (
        evidence.attempted
        and not evidence.api_found
        and not evidence.written
        and not evidence.verified
        and evidence.api_not_found_count == 0
    ):
        evidence.classification = (
            "UNCHANGED"
        )

        evidence.retry_candidate = True
        return

    # --------------------------------------------------------
    # Insufficient evidence
    # --------------------------------------------------------

    evidence.classification = (
        "INSUFFICIENT_EVIDENCE"
    )

    evidence.manual_review = True


# ============================================================
# 41. CLASSIFY ALL JOBS WITH CHECKPOINT
# ============================================================

def classify_all_jobs(
    evidence_map: dict[str, RecoveryEvidence],
    input_fingerprint: str,
    resume_processed: set[str],
) -> set[str]:

    print_header(
        "CLASSIFY RECOVERY EVIDENCE"
    )

    # We intentionally re-evaluate all evidence in memory.
    #
    # Resume is used as persistent progress metadata, but a
    # previous "processed" flag alone is never trusted as proof
    # of a valid classification.
    #
    # This prevents a stale or partially written result from
    # being silently skipped.

    processed: set[str] = set()

    keys = sorted(
        evidence_map.keys()
    )

    total = len(keys)

    for index, key in enumerate(
        keys,
        start=1,
    ):
        evidence = evidence_map[key]

        classify_evidence(
            evidence
        )

        processed.add(key)

        if (
            index % CHECKPOINT_SAVE_INTERVAL == 0
            or index == total
        ):
            save_audit_checkpoint(
                input_fingerprint=input_fingerprint,
                processed_keys=processed,
                complete=False,
            )

            print(
                "[AUDIT] "
                f"{index}/{total}"
            )

    return processed


# ============================================================
# 42. RESULT COUNTERS
# ============================================================

def build_counts(
    evidence_map: dict[str, RecoveryEvidence],
) -> dict[str, int]:

    counts = {
        "target_jobs": 0,
        "attempted_jobs": 0,
        "api_found_jobs": 0,
        "written_jobs": 0,
        "verified_jobs": 0,

        "verified_recovered_jobs": 0,
        "partially_recovered_jobs": 0,
        "unchanged_jobs": 0,
        "not_attempted_jobs": 0,
        "api_not_found_jobs": 0,
        "attempted_not_written_jobs": 0,
        "status_conflict_jobs": 0,
        "checkpoint_conflict_jobs": 0,
        "insufficient_evidence_jobs": 0,
        "retry_candidate_jobs": 0,
        "manual_review_jobs": 0,

        "target_timestamp_count": 0,
        "verified_timestamp_count": 0,

        "target_count_recorded": 0,
        "attempted_count_recorded": 0,
        "api_found_count_recorded": 0,
        "written_count_recorded": 0,
        "api_not_found_count_recorded": 0,
        "failed_count_recorded": 0,
        "skipped_count_recorded": 0,
    }

    for evidence in evidence_map.values():
        if evidence.targeted:
            counts["target_jobs"] += 1

        if evidence.attempted:
            counts["attempted_jobs"] += 1

        if evidence.api_found:
            counts["api_found_jobs"] += 1

        if evidence.written:
            counts["written_jobs"] += 1

        if evidence.verified:
            counts["verified_jobs"] += 1

        classification = evidence.classification

        mapping = {
            "VERIFIED_RECOVERED":
                "verified_recovered_jobs",

            "PARTIALLY_RECOVERED":
                "partially_recovered_jobs",

            "UNCHANGED":
                "unchanged_jobs",

            "NOT_ATTEMPTED":
                "not_attempted_jobs",

            "API_NOT_FOUND":
                "api_not_found_jobs",

            "ATTEMPTED_NOT_WRITTEN":
                "attempted_not_written_jobs",

            "STATUS_CONFLICT":
                "status_conflict_jobs",

            "CHECKPOINT_CONFLICT":
                "checkpoint_conflict_jobs",

            "INSUFFICIENT_EVIDENCE":
                "insufficient_evidence_jobs",
        }

        counter = mapping.get(
            classification
        )

        if counter:
            counts[counter] += 1

        if evidence.retry_candidate:
            counts["retry_candidate_jobs"] += 1

        if evidence.manual_review:
            counts["manual_review_jobs"] += 1

        counts["target_timestamp_count"] += len(
            evidence.target_timestamps
        )

        counts["verified_timestamp_count"] += len(
            evidence.current_verified_timestamps
        )

        counts["target_count_recorded"] += (
            evidence.target_count
        )

        counts["attempted_count_recorded"] += (
            evidence.attempted_count
        )

        counts["api_found_count_recorded"] += (
            evidence.api_found_count
        )

        counts["written_count_recorded"] += (
            evidence.written_count
        )

        counts["api_not_found_count_recorded"] += (
            evidence.api_not_found_count
        )

        counts["failed_count_recorded"] += (
            evidence.failed_count
        )

        counts["skipped_count_recorded"] += (
            evidence.skipped_count
        )

    return counts


# ============================================================
# 43. POST REPORT SUSPECT DECISION
# ============================================================

def determine_post_report_suspect(
    report_audit: dict[str, Any],
    counts: dict[str, int],
) -> bool:

    if report_audit.get(
        "sha256_equal"
    ) is not True:
        return False

    verified_recovery = (
        counts["verified_recovered_jobs"] > 0
        or counts["partially_recovered_jobs"] > 0
        or counts["verified_timestamp_count"] > 0
    )

    written_recovery = (
        counts["written_count_recorded"] > 0
    )

    return (
        verified_recovery
        or written_recovery
    )


# ============================================================
# 44. DETAIL ROW
# ============================================================

def evidence_to_detail_row(
    evidence: RecoveryEvidence,
) -> dict[str, Any]:

    return {
        "market": evidence.market,
        "timeframe": evidence.timeframe,

        "classification":
            evidence.classification,

        "retry_candidate":
            int(evidence.retry_candidate),

        "manual_review":
            int(evidence.manual_review),

        "targeted":
            int(evidence.targeted),

        "attempted":
            int(evidence.attempted),

        "api_found":
            int(evidence.api_found),

        "written":
            int(evidence.written),

        "verified":
            int(evidence.verified),

        "target_count_recorded":
            evidence.target_count,

        "attempted_count_recorded":
            evidence.attempted_count,

        "api_found_count_recorded":
            evidence.api_found_count,

        "written_count_recorded":
            evidence.written_count,

        "api_not_found_count_recorded":
            evidence.api_not_found_count,

        "failed_count_recorded":
            evidence.failed_count,

        "skipped_count_recorded":
            evidence.skipped_count,

        "explicit_target_timestamps":
            len(evidence.target_timestamps),

        "verified_target_timestamps":
            len(
                evidence.current_verified_timestamps
            ),

        "checkpoint_complete":
            (
                ""
                if evidence.checkpoint_complete is None
                else int(
                    evidence.checkpoint_complete
                )
            ),

        "status_values":
            " | ".join(
                evidence.status_values
            ),

        "messages":
            " | ".join(
                evidence.messages
            ),

        "conflict_reason":
            evidence.conflict_reason,

        "sources":
            " | ".join(
                sorted(evidence.sources)
            ),
    }


# ============================================================
# 45. DETAIL CSV
# ============================================================

DETAIL_FIELDS = [
    "market",
    "timeframe",
    "classification",
    "retry_candidate",
    "manual_review",
    "targeted",
    "attempted",
    "api_found",
    "written",
    "verified",
    "target_count_recorded",
    "attempted_count_recorded",
    "api_found_count_recorded",
    "written_count_recorded",
    "api_not_found_count_recorded",
    "failed_count_recorded",
    "skipped_count_recorded",
    "explicit_target_timestamps",
    "verified_target_timestamps",
    "checkpoint_complete",
    "status_values",
    "messages",
    "conflict_reason",
    "sources",
]


# ============================================================
# 46. WRITE DETAIL REPORT
# ============================================================

def write_detail_report(
    evidence_map: dict[str, RecoveryEvidence],
) -> None:

    rows = [
        evidence_to_detail_row(
            evidence_map[key]
        )
        for key in sorted(
            evidence_map.keys()
        )
    ]

    write_csv(
        AUDIT_DETAIL_PATH,
        DETAIL_FIELDS,
        rows,
    )


# ============================================================
# 47. WRITE RETRY CANDIDATES
# ============================================================

def write_retry_candidates(
    evidence_map: dict[str, RecoveryEvidence],
) -> None:

    rows = []

    for key in sorted(
        evidence_map.keys()
    ):
        evidence = evidence_map[key]

        if not evidence.retry_candidate:
            continue

        rows.append(
            evidence_to_detail_row(
                evidence
            )
        )

    write_csv(
        RETRY_CANDIDATES_PATH,
        DETAIL_FIELDS,
        rows,
    )


# ============================================================
# 48. WRITE SUMMARY CSV
# ============================================================

def write_summary_report(
    counts: dict[str, int],
    report_audit: dict[str, Any],
    post_report_suspect: bool,
) -> None:

    rows: list[dict[str, Any]] = []

    for key, value in counts.items():
        rows.append(
            {
                "section": "recovery_audit",
                "metric": key,
                "value": value,
            }
        )

    rows.extend(
        [
            {
                "section": "pre_post_report",
                "metric": "pre_report",
                "value": (
                    report_audit.get(
                        "pre_path"
                    )
                    or ""
                ),
            },
            {
                "section": "pre_post_report",
                "metric": "post_report",
                "value": (
                    report_audit.get(
                        "post_path"
                    )
                    or ""
                ),
            },
            {
                "section": "pre_post_report",
                "metric": "pre_sha256",
                "value": (
                    report_audit.get(
                        "pre_sha256"
                    )
                    or ""
                ),
            },
            {
                "section": "pre_post_report",
                "metric": "post_sha256",
                "value": (
                    report_audit.get(
                        "post_sha256"
                    )
                    or ""
                ),
            },
            {
                "section": "pre_post_report",
                "metric": "sha256_equal",
                "value": (
                    report_audit.get(
                        "sha256_equal"
                    )
                ),
            },
            {
                "section": "pre_post_report",
                "metric": "post_report_suspect",
                "value": post_report_suspect,
            },
        ]
    )

    write_csv(
        AUDIT_SUMMARY_PATH,
        [
            "section",
            "metric",
            "value",
        ],
        rows,
    )


# ============================================================
# 49. WRITE RESULT JSON
# ============================================================

def write_result_json(
    inventory: dict[str, list[Path]],
    counts: dict[str, int],
    report_audit: dict[str, Any],
    post_report_suspect: bool,
    recovery_tables: list[
        tuple[
            CsvTable,
            dict[str, Optional[str]],
        ]
    ],
    checkpoint_paths: list[str],
    input_fingerprint: str,
) -> None:

    payload = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "execution_mode": EXECUTION_MODE,
        "created_utc": utc_now_iso(),

        "input_fingerprint":
            input_fingerprint,

        "ohlcv_inventory": {
            "h1": len(
                inventory["h1"]
            ),
            "h4": len(
                inventory["h4"]
            ),
            "d1": len(
                inventory["d1"]
            ),
            "total": sum(
                len(inventory[x])
                for x in TIMEFRAMES
            ),
        },

        "recovery_sources": {
            "csv_tables": [
                str(table.path)
                for table, _columns
                in recovery_tables
            ],

            "checkpoint_files":
                checkpoint_paths,
        },

        "recovery": {
            "target_jobs":
                counts["target_jobs"],

            "attempted_jobs":
                counts["attempted_jobs"],

            "api_found_jobs":
                counts["api_found_jobs"],

            "written_jobs":
                counts["written_jobs"],

            "verified_jobs":
                counts["verified_jobs"],

            "verified_recovered_jobs":
                counts[
                    "verified_recovered_jobs"
                ],

            "partially_recovered_jobs":
                counts[
                    "partially_recovered_jobs"
                ],

            "unchanged_jobs":
                counts["unchanged_jobs"],

            "not_attempted_jobs":
                counts["not_attempted_jobs"],

            "api_not_found_jobs":
                counts["api_not_found_jobs"],

            "attempted_not_written_jobs":
                counts[
                    "attempted_not_written_jobs"
                ],

            "status_conflict_jobs":
                counts["status_conflict_jobs"],

            "checkpoint_conflict_jobs":
                counts[
                    "checkpoint_conflict_jobs"
                ],

            "insufficient_evidence_jobs":
                counts[
                    "insufficient_evidence_jobs"
                ],
        },

        "evidence": {
            "target_timestamp_count":
                counts[
                    "target_timestamp_count"
                ],

            "verified_timestamp_count":
                counts[
                    "verified_timestamp_count"
                ],

            "target_count_recorded":
                counts[
                    "target_count_recorded"
                ],

            "attempted_count_recorded":
                counts[
                    "attempted_count_recorded"
                ],

            "api_found_count_recorded":
                counts[
                    "api_found_count_recorded"
                ],

            "written_count_recorded":
                counts[
                    "written_count_recorded"
                ],

            "api_not_found_count_recorded":
                counts[
                    "api_not_found_count_recorded"
                ],

            "failed_count_recorded":
                counts[
                    "failed_count_recorded"
                ],

            "skipped_count_recorded":
                counts[
                    "skipped_count_recorded"
                ],
        },

        "pre_post_report": {
            "pre_recovery_gap_report":
                report_audit.get(
                    "pre_path"
                ),

            "post_recovery_gap_report":
                report_audit.get(
                    "post_path"
                ),

            "pre_sha256":
                report_audit.get(
                    "pre_sha256"
                ),

            "post_sha256":
                report_audit.get(
                    "post_sha256"
                ),

            "pre_post_sha256_equal":
                report_audit.get(
                    "sha256_equal"
                ),

            "post_report_suspect":
                post_report_suspect,
        },

        "decision": {
            "retry_candidates":
                counts[
                    "retry_candidate_jobs"
                ],

            "manual_review":
                counts[
                    "manual_review_jobs"
                ],
        },

        "outputs": {
            "summary_csv":
                str(AUDIT_SUMMARY_PATH),

            "detail_csv":
                str(AUDIT_DETAIL_PATH),

            "retry_candidates_csv":
                str(RETRY_CANDIDATES_PATH),

            "checkpoint_json":
                str(CHECKPOINT_PATH),
        },

        "safety": SAFETY_FLAGS,
    }

    write_json(
        AUDIT_RESULT_PATH,
        payload,
    )


# ============================================================
# 50. PRINT SUMMARY
# ============================================================

def print_final_summary(
    inventory: dict[str, list[Path]],
    counts: dict[str, int],
    report_audit: dict[str, Any],
    post_report_suspect: bool,
) -> None:

    print_header(
        "HISTORY GAP RECOVERY AUDIT SUMMARY"
    )

    print(
        f"Markets                     : "
        f"{EXPECTED_MARKETS}"
    )

    print(
        f"Timeframes                  : "
        f"{EXPECTED_TIMEFRAMES}"
    )

    print(
        f"OHLCV files                 : "
        f"{sum(len(inventory[x]) for x in TIMEFRAMES)}"
    )

    print()

    print(
        f"Recovery target jobs        : "
        f"{counts['target_jobs']}"
    )

    print(
        f"Attempted jobs              : "
        f"{counts['attempted_jobs']}"
    )

    print()

    print(
        f"Verified recovered          : "
        f"{counts['verified_recovered_jobs']}"
    )

    print(
        f"Partially recovered         : "
        f"{counts['partially_recovered_jobs']}"
    )

    print(
        f"Unchanged                   : "
        f"{counts['unchanged_jobs']}"
    )

    print(
        f"Not attempted               : "
        f"{counts['not_attempted_jobs']}"
    )

    print(
        f"API not found               : "
        f"{counts['api_not_found_jobs']}"
    )

    print(
        f"Attempted not written       : "
        f"{counts['attempted_not_written_jobs']}"
    )

    print(
        f"Status conflict             : "
        f"{counts['status_conflict_jobs']}"
    )

    print(
        f"Checkpoint conflict         : "
        f"{counts['checkpoint_conflict_jobs']}"
    )

    print(
        f"Insufficient evidence       : "
        f"{counts['insufficient_evidence_jobs']}"
    )

    print()

    print(
        f"Explicit target timestamps  : "
        f"{counts['target_timestamp_count']}"
    )

    print(
        f"Verified target timestamps  : "
        f"{counts['verified_timestamp_count']}"
    )

    print()

    equal = report_audit.get(
        "sha256_equal"
    )

    if equal is True:
        equal_text = "YES"

    elif equal is False:
        equal_text = "NO"

    else:
        equal_text = "UNKNOWN"

    print(
        f"PRE/POST SHA256 equal       : "
        f"{equal_text}"
    )

    print(
        f"POST report suspect         : "
        f"{'YES' if post_report_suspect else 'NO'}"
    )

    print()

    print(
        f"Retry candidates            : "
        f"{counts['retry_candidate_jobs']}"
    )

    print(
        f"Manual review               : "
        f"{counts['manual_review_jobs']}"
    )

    print()
    print("-" * 60)
    print("Safety")
    print("-" * 60)

    print(
        "OHLCV write                 : DISABLED"
    )
    print(
        "OHLCV delete                : DISABLED"
    )
    print(
        "API download                : DISABLED"
    )
    print(
        "Gap repair                  : DISABLED"
    )
    print(
        "Feature build               : DISABLED"
    )
    print(
        "256 Detector                : DISABLED"
    )
    print(
        "Future labels               : DISABLED"
    )
    print(
        "Prediction                  : DISABLED"
    )
    print(
        "Trading                     : DISABLED"
    )
    print(
        "Git reset                   : DISABLED"
    )
    print(
        "Git clean                   : DISABLED"
    )
    print(
        "Git commit                  : DISABLED"
    )
    print(
        "Git push                    : DISABLED"
    )

    print()
    print(
        "[PASS] Historical gap recovery audit completed."
    )

    print(
        "[NEXT] Review recovery evidence before "
        "second-pass repair."
    )

    print("=" * 60)


# ============================================================
# 51. PRINT STARTUP
# ============================================================

def print_startup() -> None:
    print_header(
        "UPBIT SURGE MONITOR"
    )

    print(
        "HISTORY GAP RECOVERY AUDITOR"
    )

    print(VERSION)

    print()
    print(
        f"Execution mode : {EXECUTION_MODE}"
    )

    print(
        f"Expected markets : {EXPECTED_MARKETS}"
    )

    print(
        f"Expected jobs    : {EXPECTED_OHLCV_FILES}"
    )

    print()
    print(
        "Safety:"
    )

    print(
        "  OHLCV write      : DISABLED"
    )
    print(
        "  OHLCV delete     : DISABLED"
    )
    print(
        "  API download     : DISABLED"
    )
    print(
        "  Gap repair       : DISABLED"
    )
    print(
        "  Git reset/clean  : DISABLED"
    )


# ============================================================
# 52. MAIN
# ============================================================

def main() -> int:
    print_startup()

    ensure_runtime_directories()

    # --------------------------------------------------------
    # Step 1
    # Discover and validate OHLCV inventory
    # --------------------------------------------------------

    inventory = discover_ohlcv_inventory()

    verify_ohlcv_inventory(
        inventory
    )

    # --------------------------------------------------------
    # Step 2
    # Discover PRE / POST gap reports
    # --------------------------------------------------------

    pre_gap_report = (
        discover_pre_gap_report()
    )

    post_gap_report = (
        discover_post_gap_report()
    )

    # PRE / POST are valuable evidence.
    #
    # However, absence of either report does not automatically
    # make the recovery evidence audit impossible.
    #
    # The condition is recorded as UNKNOWN rather than guessed.

    report_audit = (
        audit_pre_post_reports(
            pre_gap_report,
            post_gap_report,
        )
    )

    # --------------------------------------------------------
    # Step 3
    # Discover recovery evidence CSV files
    # --------------------------------------------------------

    recovery_tables = (
        discover_recovery_tables()
    )

    if not recovery_tables:
        print()
        print(
            "[WARNING] No validated recovery CSV "
            "evidence tables were found."
        )
        print(
            "[WARNING] Audit will continue using "
            "available checkpoint/report evidence."
        )

    # --------------------------------------------------------
    # Step 4
    # Build recovery evidence map
    # --------------------------------------------------------

    evidence_map: dict[
        str,
        RecoveryEvidence,
    ] = {}

    for table, columns in recovery_tables:
        ingest_recovery_table(
            table=table,
            columns=columns,
            evidence_map=evidence_map,
        )

    # --------------------------------------------------------
    # Step 5
    # Ingest recovery checkpoints
    # --------------------------------------------------------

    checkpoint_paths = (
        ingest_recovery_checkpoints(
            evidence_map
        )
    )

    print()
    print(
        "Recovery evidence jobs discovered : "
        f"{len(evidence_map)}"
    )

    # --------------------------------------------------------
    # Step 6
    # Validate all current OHLCV files
    # --------------------------------------------------------

    ohlcv_cache = build_ohlcv_cache(
        inventory
    )

    # --------------------------------------------------------
    # Step 7
    # Verify explicit recovery timestamps
    # --------------------------------------------------------

    verify_target_timestamps(
        evidence_map=evidence_map,
        ohlcv_cache=ohlcv_cache,
    )

    # --------------------------------------------------------
    # Step 8
    # Build input fingerprint
    # --------------------------------------------------------

    input_fingerprint = (
        build_input_fingerprint(
            inventory=inventory,
            recovery_tables=recovery_tables,
            checkpoint_paths=checkpoint_paths,
            pre_path=pre_gap_report,
            post_path=post_gap_report,
        )
    )

    print_header(
        "AUDIT INPUT FINGERPRINT"
    )

    print(
        input_fingerprint
    )

    # --------------------------------------------------------
    # Step 9
    # Resolve persistent resume state
    # --------------------------------------------------------

    resume_processed = (
        resolve_resume_state(
            input_fingerprint
        )
    )

    # --------------------------------------------------------
    # Step 10
    # Classify recovery evidence
    # --------------------------------------------------------

    processed_keys = classify_all_jobs(
        evidence_map=evidence_map,
        input_fingerprint=input_fingerprint,
        resume_processed=resume_processed,
    )

    # --------------------------------------------------------
    # Step 11
    # Build counters
    # --------------------------------------------------------

    counts = build_counts(
        evidence_map
    )

    # --------------------------------------------------------
    # Step 12
    # Decide whether POST report is suspicious
    # --------------------------------------------------------

    post_report_suspect = (
        determine_post_report_suspect(
            report_audit=report_audit,
            counts=counts,
        )
    )

    # --------------------------------------------------------
    # Step 13
    # Write runtime outputs
    # --------------------------------------------------------

    write_detail_report(
        evidence_map
    )

    write_retry_candidates(
        evidence_map
    )

    write_summary_report(
        counts=counts,
        report_audit=report_audit,
        post_report_suspect=post_report_suspect,
    )

    write_result_json(
        inventory=inventory,
        counts=counts,
        report_audit=report_audit,
        post_report_suspect=post_report_suspect,
        recovery_tables=recovery_tables,
        checkpoint_paths=checkpoint_paths,
        input_fingerprint=input_fingerprint,
    )

    # --------------------------------------------------------
    # Step 14
    # Final persistent checkpoint
    # --------------------------------------------------------

    save_audit_checkpoint(
        input_fingerprint=input_fingerprint,
        processed_keys=processed_keys,
        complete=True,
    )

    # --------------------------------------------------------
    # Step 15
    # Final summary
    # --------------------------------------------------------

    print_final_summary(
        inventory=inventory,
        counts=counts,
        report_audit=report_audit,
        post_report_suspect=post_report_suspect,
    )

    return 0


# ============================================================
# 53. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        exit_code = main()

    except KeyboardInterrupt:
        print()
        print("=" * 60)
        print("HISTORY GAP RECOVERY AUDIT INTERRUPTED")
        print("=" * 60)
        print(
            "[RESUME] Existing valid checkpoint "
            "remains available."
        )
        print(
            "[SAFETY] No OHLCV deletion was executed."
        )
        print(
            "[SAFETY] No historical gap repair was executed."
        )
        print(
            "[SAFETY] No API candle download was executed."
        )
        print(
            "[SAFETY] No Git reset/clean was executed."
        )

        exit_code = 130

    except Exception as exc:
        print()
        print("=" * 60)
        print("HISTORY GAP RECOVERY AUDIT FATAL ERROR")
        print("=" * 60)

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print()
        traceback.print_exc()

        print()
        print(
            "[SAFETY] No OHLCV deletion was executed."
        )
        print(
            "[SAFETY] No historical gap repair was executed."
        )
        print(
            "[SAFETY] No API candle download was executed."
        )
        print(
            "[SAFETY] No Git reset/clean was executed."
        )

        exit_code = 1

    sys.exit(exit_code)


# ============================================================
# END
# ============================================================
