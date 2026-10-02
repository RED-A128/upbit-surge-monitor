# ============================================================
# Upbit Surge Monitor
# recover_history_gaps_second_pass.py
# Clean V001
# ============================================================
#
# PURPOSE
#
#   Execute SAFE second-pass historical OHLCV gap recovery
#   using ONLY the validated planner output:
#
#       data/recovery/
#           history_gap_second_pass_targets.csv
#
#
# INPUT CONTRACT
#
#   Only rows with:
#
#       classification == READY_SECOND_PASS
#
#   are allowed.
#
#
# IMPORTANT SAFETY RULES
#
#   1. Existing OHLCV candles are NEVER overwritten.
#   2. Existing OHLCV candles are NEVER deleted.
#   3. Only timestamps that are currently missing are eligible.
#   4. API candles outside the exact missing timestamp set
#      are ignored.
#   5. Original OHLCV is replaced only after full validation.
#   6. Every modified file is written through a temporary file.
#   7. BEFORE / AFTER SHA256 are recorded.
#   8. Checkpoint + actual OHLCV state are both used for resume.
#   9. A checkpoint alone is NEVER enough to skip work.
#  10. Ctrl+C / runner stop / network failure can be resumed.
#  11. No feature generation is executed.
#  12. No 256 detector is executed.
#  13. No future labels are generated.
#  14. No prediction is executed.
#  15. No trading is executed.
#  16. No Git reset / clean / commit / push is executed.
#
#
# EXECUTION MODE
#
#   WRITE_SAFE_SECOND_PASS_RECOVERY
#
#
# OUTPUT
#
#   data/reports/history_gap_second_pass_recovery/
#
#       history_gap_second_pass_recovery_summary.csv
#       history_gap_second_pass_recovery_detail.csv
#       history_gap_second_pass_recovery_result.json
#       history_gap_second_pass_before_sha256.csv
#       history_gap_second_pass_after_sha256.csv
#
#   data/validation/
#
#       recover_history_gaps_second_pass_checkpoint.json
#
#   data/
#
#       recover_history_gaps_second_pass_status.csv
#
#
# RESULT STATUS
#
#   SUCCESS
#   PARTIAL
#   ALREADY_RECOVERED
#   NO_DATA
#   FAILED
#   SKIPPED
#
# ============================================================


from __future__ import annotations


import csv
import hashlib
import json
import os
import sys
import time
import traceback

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


import requests


# ============================================================
# 1. PROGRAM
# ============================================================


PROGRAM = "recover_history_gaps_second_pass.py"

VERSION = "Clean V001"

EXECUTION_MODE = "WRITE_SAFE_SECOND_PASS_RECOVERY"


# ============================================================
# 2. EXPECTED PROJECT STATE
# ============================================================


EXPECTED_MARKETS = 290

EXPECTED_TIMEFRAMES = 3

EXPECTED_OHLCV_FILES = 870


TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)


TIMEFRAME_SECONDS = {
    "h1": 60 * 60,
    "h4": 4 * 60 * 60,
    "d1": 24 * 60 * 60,
}


# ============================================================
# 3. API
# ============================================================


UPBIT_API_BASE = "https://api.upbit.com"


API_PATHS = {
    "h1": "/v1/candles/minutes/60",
    "h4": "/v1/candles/minutes/240",
    "d1": "/v1/candles/days",
}


API_MAX_COUNT = 200

API_TIMEOUT_SECONDS = 15

API_MAX_RETRIES = 6

API_RETRY_BASE_SECONDS = 1.0

API_REQUEST_INTERVAL_SECONDS = 0.15


USER_AGENT = (
    "Upbit-Surge-Monitor/"
    "recover-history-gaps-second-pass-Clean-V001"
)


# ============================================================
# 4. ROOT PATHS
# ============================================================


PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"

OHLCV_DIR = DATA_DIR / "ohlcv"

RECOVERY_DIR = DATA_DIR / "recovery"

REPORTS_DIR = DATA_DIR / "reports"

VALIDATION_DIR = DATA_DIR / "validation"


RECOVERY_REPORT_DIR = (
    REPORTS_DIR
    / "history_gap_second_pass_recovery"
)


# ============================================================
# 5. INPUT FILES
# ============================================================


SECOND_PASS_TARGET_FILE = (
    RECOVERY_DIR
    / "history_gap_second_pass_targets.csv"
)


SECOND_PASS_PLAN_FILE = (
    RECOVERY_DIR
    / "history_gap_second_pass_plan.json"
)


# ============================================================
# 6. OUTPUT FILES
# ============================================================


RECOVERY_SUMMARY_FILE = (
    RECOVERY_REPORT_DIR
    / "history_gap_second_pass_recovery_summary.csv"
)


RECOVERY_DETAIL_FILE = (
    RECOVERY_REPORT_DIR
    / "history_gap_second_pass_recovery_detail.csv"
)


RECOVERY_RESULT_FILE = (
    RECOVERY_REPORT_DIR
    / "history_gap_second_pass_recovery_result.json"
)


BEFORE_SHA256_FILE = (
    RECOVERY_REPORT_DIR
    / "history_gap_second_pass_before_sha256.csv"
)


AFTER_SHA256_FILE = (
    RECOVERY_REPORT_DIR
    / "history_gap_second_pass_after_sha256.csv"
)


CHECKPOINT_FILE = (
    VALIDATION_DIR
    / "recover_history_gaps_second_pass_checkpoint.json"
)


STATUS_FILE = (
    DATA_DIR
    / "recover_history_gaps_second_pass_status.csv"
)


# ============================================================
# 7. SAFETY
# ============================================================


SAFETY = {
    "ohlcv_write": True,
    "ohlcv_delete": False,
    "existing_candle_overwrite": False,
    "missing_candle_insert_only": True,
    "api_download": True,
    "gap_repair": True,
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
# 8. CONSTANTS
# ============================================================


READY_SECOND_PASS = "READY_SECOND_PASS"


STATUS_SUCCESS = "SUCCESS"

STATUS_PARTIAL = "PARTIAL"

STATUS_ALREADY_RECOVERED = "ALREADY_RECOVERED"

STATUS_NO_DATA = "NO_DATA"

STATUS_FAILED = "FAILED"

STATUS_SKIPPED = "SKIPPED"


FINAL_STATUSES = {
    STATUS_SUCCESS,
    STATUS_ALREADY_RECOVERED,
}


# ============================================================
# 9. TARGET COLUMN ALIASES
# ============================================================


MARKET_ALIASES = (
    "market",
    "ticker",
    "symbol",
    "code",
    "market_code",
)


TIMEFRAME_ALIASES = (
    "timeframe",
    "interval",
    "tf",
    "candle",
    "candle_type",
)


GAP_START_ALIASES = (
    "gap_start",
    "start",
    "start_time",
    "start_timestamp",
    "missing_start",
    "from_time",
    "gap_from",
)


GAP_END_ALIASES = (
    "gap_end",
    "end",
    "end_time",
    "end_timestamp",
    "missing_end",
    "to_time",
    "gap_to",
)


EXPECTED_MISSING_ALIASES = (
    "expected_missing",
    "missing_count",
    "missing",
    "current_missing_count",
)


CLASSIFICATION_ALIASES = (
    "classification",
    "gap_classification",
)


EVENT_KEY_ALIASES = (
    "event_key",
    "gap_key",
    "recovery_key",
    "key",
)


PLANNER_VERSION_ALIASES = (
    "planner_version",
    "version",
)


SOURCE_REPORT_ALIASES = (
    "source_report",
    "report",
    "source_file",
)


SOURCE_ROW_ALIASES = (
    "source_row",
    "row",
    "row_number",
)


OHLCV_TIMESTAMP_ALIASES = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "date_time",
    "time",
    "utc_time",
)


# ============================================================
# 10. OUTPUT SCHEMAS
# ============================================================


HASH_FIELDS = (
    "timeframe",
    "market",
    "path",
    "rows",
    "sha256",
)


DETAIL_FIELDS = (
    "utc_time",
    "version",
    "target_index",
    "target_total",
    "timeframe",
    "market",
    "gap_start",
    "gap_end",
    "expected_missing_from_plan",
    "missing_before",
    "downloaded_matching_candles",
    "inserted_candles",
    "missing_after",
    "rows_before",
    "rows_after",
    "before_sha256",
    "after_sha256",
    "file_changed",
    "status",
    "message",
    "event_key",
    "source_report",
    "source_row",
    "planner_version",
)


SUMMARY_FIELDS = (
    "timeframe",
    "targets",
    "success",
    "partial",
    "already_recovered",
    "no_data",
    "failed",
    "skipped",
    "missing_before",
    "inserted_candles",
    "missing_after",
)


STATUS_FIELDS = (
    "utc_time",
    "version",
    "mode",
    "target_index",
    "target_total",
    "timeframe",
    "market",
    "event_key",
    "status",
    "missing_before",
    "inserted_candles",
    "missing_after",
    "message",
)


# ============================================================
# 11. DATA CLASS
# ============================================================


@dataclass
class RecoveryTarget:

    market: str

    timeframe: str

    gap_start: str

    gap_end: str

    expected_missing: int

    classification: str

    event_key: str

    source_report: str

    source_row: str

    planner_version: str

    raw: Dict[str, str]


# ============================================================
# 12. BASIC UTILITIES
# ============================================================


def utc_now_iso() -> str:

    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def clean_text(
    value: Any,
) -> str:

    if value is None:
        return ""

    return str(value).strip()


def normalize_column_name(
    value: str,
) -> str:

    text = clean_text(value).lower()

    result = []

    previous_underscore = False

    for char in text:

        if char.isalnum():

            result.append(char)

            previous_underscore = False

        else:

            if not previous_underscore:

                result.append("_")

                previous_underscore = True

    return "".join(result).strip("_")


def normalize_market(
    value: Any,
) -> str:

    return clean_text(value).upper()


def normalize_timeframe(
    value: Any,
) -> str:

    text = clean_text(value).lower()

    mapping = {
        "1h": "h1",
        "h1": "h1",
        "60": "h1",
        "60m": "h1",
        "60min": "h1",

        "4h": "h4",
        "h4": "h4",
        "240": "h4",
        "240m": "h4",
        "240min": "h4",

        "1d": "d1",
        "d1": "d1",
        "day": "d1",
        "daily": "d1",
    }

    return mapping.get(
        text,
        text,
    )


def safe_int(
    value: Any,
    default: int = 0,
) -> int:

    text = clean_text(value)

    if not text:
        return default

    try:

        return int(float(text))

    except Exception:

        return default


def sha256_file(
    path: Path,
) -> str:

    digest = hashlib.sha256()

    with path.open("rb") as handle:

        while True:

            chunk = handle.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_json_write(
    path: Path,
    payload: Any,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = Path(
        str(path) + ".tmp"
    )

    with temp.open(
        "w",
        encoding="utf-8",
    ) as handle:

        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
        )

        handle.flush()

        os.fsync(
            handle.fileno()
        )

    os.replace(
        temp,
        path,
    )


def atomic_csv_write(
    path: Path,
    fieldnames: Sequence[str],
    rows: Sequence[Dict[str, Any]],
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = Path(
        str(path) + ".tmp"
    )

    with temp.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:

            writer.writerow(
                {
                    field:
                    row.get(
                        field,
                        "",
                    )
                    for field
                    in fieldnames
                }
            )

        handle.flush()

        os.fsync(
            handle.fileno()
        )

    os.replace(
        temp,
        path,
    )


def read_csv(
    path: Path,
) -> Tuple[
    List[str],
    List[Dict[str, str]],
]:

    if not path.is_file():

        raise FileNotFoundError(
            f"CSV file not found: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        reader = csv.DictReader(
            handle
        )

        fieldnames = list(
            reader.fieldnames
            or []
        )

        rows = [
            {
                str(key):
                clean_text(value)
                for key, value
                in row.items()
                if key is not None
            }
            for row
            in reader
        ]

    if not fieldnames:

        raise RuntimeError(
            f"CSV header missing: {path}"
        )

    return (
        fieldnames,
        rows,
    )


# ============================================================
# 13. DATETIME
# ============================================================


def parse_datetime(
    value: Any,
) -> Optional[datetime]:

    text = clean_text(value)

    if not text:
        return None

    # Upbit millisecond timestamp support.
    if text.isdigit():

        try:

            number = int(text)

            if number > 10_000_000_000:

                return datetime.fromtimestamp(
                    number / 1000.0,
                    tz=timezone.utc,
                )

            return datetime.fromtimestamp(
                number,
                tz=timezone.utc,
            )

        except Exception:

            pass

    text = text.replace(
        "Z",
        "+00:00",
    )

    candidates = (
        text,
        text.replace(
            " ",
            "T",
        ),
    )

    for candidate in candidates:

        try:

            dt = datetime.fromisoformat(
                candidate
            )

            if dt.tzinfo is None:

                dt = dt.replace(
                    tzinfo=timezone.utc
                )

            return dt.astimezone(
                timezone.utc
            )

        except Exception:

            pass

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    )

    for fmt in formats:

        try:

            dt = datetime.strptime(
                text,
                fmt,
            )

            return dt.replace(
                tzinfo=timezone.utc
            )

        except Exception:

            pass

    return None


def datetime_key(
    value: datetime,
) -> int:

    return int(
        value.timestamp()
    )


def datetime_to_upbit_to(
    value: datetime,
) -> str:

    value = value.astimezone(
        timezone.utc
    )

    return value.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# ============================================================
# 14. COLUMN DETECTION
# ============================================================


def detect_column(
    fieldnames: Sequence[str],
    aliases: Sequence[str],
) -> Optional[str]:

    normalized = {
        normalize_column_name(name):
        name
        for name
        in fieldnames
    }

    for alias in aliases:

        key = normalize_column_name(
            alias
        )

        if key in normalized:

            return normalized[key]

    return None


def value_from_aliases(
    row: Dict[str, str],
    fieldnames: Sequence[str],
    aliases: Sequence[str],
) -> str:

    column = detect_column(
        fieldnames,
        aliases,
    )

    if column is None:

        return ""

    return clean_text(
        row.get(
            column,
            "",
        )
    )


# ============================================================
# 15. DIRECTORY PREPARATION
# ============================================================


def prepare_directories() -> None:

    directories = (
        RECOVERY_REPORT_DIR,
        VALIDATION_DIR,
    )

    for directory in directories:

        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


# ============================================================
# 16. OHLCV INVENTORY
# ============================================================


def inventory_ohlcv() -> Dict[str, Any]:

    result: Dict[str, Any] = {
        "h1": 0,
        "h4": 0,
        "d1": 0,
        "total": 0,
        "markets": 0,
    }

    market_sets: Dict[
        str,
        Set[str],
    ] = {}

    for timeframe in TIMEFRAMES:

        directory = (
            OHLCV_DIR
            / timeframe
        )

        if not directory.is_dir():

            raise FileNotFoundError(
                f"Missing OHLCV directory: {directory}"
            )

        files = sorted(
            directory.glob("*.csv")
        )

        result[timeframe] = len(
            files
        )

        result["total"] += len(
            files
        )

        market_sets[timeframe] = {
            path.stem.upper()
            for path
            in files
        }

        if (
            result[timeframe]
            != EXPECTED_MARKETS
        ):

            raise RuntimeError(
                f"{timeframe.upper()} inventory mismatch. "
                f"expected={EXPECTED_MARKETS} "
                f"found={result[timeframe]}"
            )

    if (
        result["total"]
        != EXPECTED_OHLCV_FILES
    ):

        raise RuntimeError(
            "Total OHLCV inventory mismatch. "
            f"expected={EXPECTED_OHLCV_FILES} "
            f"found={result['total']}"
        )

    reference = market_sets["h1"]

    for timeframe in (
        "h4",
        "d1",
    ):

        if (
            market_sets[timeframe]
            != reference
        ):

            raise RuntimeError(
                "OHLCV market sets differ between "
                f"H1 and {timeframe.upper()}."
            )

    result["markets"] = len(
        reference
    )

    return result


# ============================================================
# 17. TARGET INPUT VALIDATION
# ============================================================


def validate_target_inputs() -> Dict[str, Any]:

    if not SECOND_PASS_TARGET_FILE.is_file():

        raise FileNotFoundError(
            "Second-pass target file missing: "
            f"{SECOND_PASS_TARGET_FILE}"
        )

    if (
        SECOND_PASS_TARGET_FILE.stat().st_size
        <= 0
    ):

        raise RuntimeError(
            "Second-pass target file is empty."
        )

    if not SECOND_PASS_PLAN_FILE.is_file():

        raise FileNotFoundError(
            "Second-pass plan JSON missing: "
            f"{SECOND_PASS_PLAN_FILE}"
        )

    with SECOND_PASS_PLAN_FILE.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:

        plan = json.load(
            handle
        )

    if not isinstance(
        plan,
        dict,
    ):

        raise RuntimeError(
            "Second-pass plan JSON root must be an object."
        )

    if (
        clean_text(
            plan.get(
                "program"
            )
        )
        != "plan_history_gap_retry.py"
    ):

        raise RuntimeError(
            "Unexpected planner program in "
            "history_gap_second_pass_plan.json."
        )

    if (
        clean_text(
            plan.get(
                "version"
            )
        )
        != "Clean V001"
    ):

        raise RuntimeError(
            "Unexpected planner version: "
            f"{plan.get('version')}"
        )

    next_stage = plan.get(
        "next_stage",
        {},
    )

    if isinstance(
        next_stage,
        dict,
    ):

        allowed = clean_text(
            next_stage.get(
                "allowed_classification"
            )
        )

        if (
            allowed
            and
            allowed != READY_SECOND_PASS
        ):

            raise RuntimeError(
                "Planner next-stage classification "
                f"is not {READY_SECOND_PASS}: {allowed}"
            )

    return plan


# ============================================================
# 18. LOAD TARGETS
# ============================================================


def load_targets() -> List[RecoveryTarget]:

    fieldnames, rows = read_csv(
        SECOND_PASS_TARGET_FILE
    )

    required_alias_groups = (
        MARKET_ALIASES,
        TIMEFRAME_ALIASES,
        GAP_START_ALIASES,
        GAP_END_ALIASES,
        CLASSIFICATION_ALIASES,
    )

    for aliases in required_alias_groups:

        if (
            detect_column(
                fieldnames,
                aliases,
            )
            is None
        ):

            raise RuntimeError(
                "Second-pass target CSV is missing "
                f"required column group: {aliases}"
            )

    targets: List[
        RecoveryTarget
    ] = []

    seen_event_keys: Set[str] = set()

    for row_number, row in enumerate(
        rows,
        start=2,
    ):

        market = normalize_market(
            value_from_aliases(
                row,
                fieldnames,
                MARKET_ALIASES,
            )
        )

        timeframe = normalize_timeframe(
            value_from_aliases(
                row,
                fieldnames,
                TIMEFRAME_ALIASES,
            )
        )

        gap_start = value_from_aliases(
            row,
            fieldnames,
            GAP_START_ALIASES,
        )

        gap_end = value_from_aliases(
            row,
            fieldnames,
            GAP_END_ALIASES,
        )

        expected_missing = safe_int(
            value_from_aliases(
                row,
                fieldnames,
                EXPECTED_MISSING_ALIASES,
            )
        )

        classification = clean_text(
            value_from_aliases(
                row,
                fieldnames,
                CLASSIFICATION_ALIASES,
            )
        )

        event_key = clean_text(
            value_from_aliases(
                row,
                fieldnames,
                EVENT_KEY_ALIASES,
            )
        )

        planner_version = clean_text(
            value_from_aliases(
                row,
                fieldnames,
                PLANNER_VERSION_ALIASES,
            )
        )

        source_report = clean_text(
            value_from_aliases(
                row,
                fieldnames,
                SOURCE_REPORT_ALIASES,
            )
        )

        source_row = clean_text(
            value_from_aliases(
                row,
                fieldnames,
                SOURCE_ROW_ALIASES,
            )
        )

        if not market:

            raise RuntimeError(
                f"Target row {row_number} has no market."
            )

        if timeframe not in TIMEFRAMES:

            raise RuntimeError(
                f"Target row {row_number} has invalid "
                f"timeframe: {timeframe}"
            )

        if (
            classification
            != READY_SECOND_PASS
        ):

            raise RuntimeError(
                f"Target row {row_number} contains "
                "unauthorized classification: "
                f"{classification}"
            )

        if (
            planner_version
            and
            planner_version != "Clean V001"
        ):

            raise RuntimeError(
                f"Target row {row_number} has unexpected "
                f"planner version: {planner_version}"
            )

        start_dt = parse_datetime(
            gap_start
        )

        end_dt = parse_datetime(
            gap_end
        )

        if (
            start_dt is None
            or end_dt is None
        ):

            raise RuntimeError(
                f"Target row {row_number} has invalid "
                "gap_start/gap_end."
            )

        if end_dt < start_dt:

            raise RuntimeError(
                f"Target row {row_number}: "
                "gap_end is earlier than gap_start."
            )

        if not event_key:

            event_key = (
                f"{timeframe}|"
                f"{market}|"
                f"{gap_start}|"
                f"{gap_end}"
            )

        if event_key in seen_event_keys:

            raise RuntimeError(
                "Duplicate event_key in second-pass targets: "
                f"{event_key}"
            )

        seen_event_keys.add(
            event_key
        )

        targets.append(
            RecoveryTarget(
                market=market,
                timeframe=timeframe,
                gap_start=gap_start,
                gap_end=gap_end,
                expected_missing=expected_missing,
                classification=classification,
                event_key=event_key,
                source_report=source_report,
                source_row=source_row,
                planner_version=planner_version,
                raw=row,
            )
        )

    return targets


# ============================================================
# 19. OHLCV FILE DISCOVERY
# ============================================================


def find_ohlcv_file(
    timeframe: str,
    market: str,
) -> Optional[Path]:

    directory = (
        OHLCV_DIR
        / timeframe
    )

    direct_candidates = (
        directory
        / f"{market}.csv",

        directory
        / f"{market.lower()}.csv",

        directory
        / f"{market.upper()}.csv",
    )

    for candidate in direct_candidates:

        if candidate.is_file():

            return candidate

    market_lower = market.lower()

    for path in directory.glob(
        "*.csv"
    ):

        if (
            path.stem.lower()
            == market_lower
        ):

            return path

    return None


# ============================================================
# 20. OHLCV DATA
# ============================================================


def read_ohlcv(
    path: Path,
) -> Tuple[
    List[str],
    List[Dict[str, str]],
    str,
]:

    fieldnames, rows = read_csv(
        path
    )

    timestamp_column = detect_column(
        fieldnames,
        OHLCV_TIMESTAMP_ALIASES,
    )

    if timestamp_column is None:

        raise RuntimeError(
            "Unable to detect OHLCV timestamp column: "
            f"{path}"
        )

    valid_rows: List[
        Dict[str, str]
    ] = []

    seen: Set[int] = set()

    for row_number, row in enumerate(
        rows,
        start=2,
    ):

        dt = parse_datetime(
            row.get(
                timestamp_column,
                "",
            )
        )

        if dt is None:

            raise RuntimeError(
                f"Invalid OHLCV timestamp at "
                f"{path}:{row_number}"
            )

        key = datetime_key(
            dt
        )

        if key in seen:

            raise RuntimeError(
                "Duplicate OHLCV timestamp detected "
                f"before recovery: {path} "
                f"timestamp={dt.isoformat()}"
            )

        seen.add(
            key
        )

        valid_rows.append(
            row
        )

    if not valid_rows:

        raise RuntimeError(
            f"OHLCV file contains no rows: {path}"
        )

    return (
        fieldnames,
        valid_rows,
        timestamp_column,
    )


def build_timestamp_map(
    rows: Sequence[Dict[str, str]],
    timestamp_column: str,
) -> Dict[int, Dict[str, str]]:

    result: Dict[
        int,
        Dict[str, str],
    ] = {}

    for row in rows:

        dt = parse_datetime(
            row.get(
                timestamp_column,
                "",
            )
        )

        if dt is None:

            raise RuntimeError(
                "Invalid timestamp while building OHLCV map."
            )

        key = datetime_key(
            dt
        )

        if key in result:

            raise RuntimeError(
                "Duplicate timestamp while building OHLCV map: "
                f"{dt.isoformat()}"
            )

        result[key] = dict(
            row
        )

    return result


# ============================================================
# 21. EXPECTED GAP GRID
# ============================================================


def expected_timestamp_keys(
    *,
    gap_start: datetime,
    gap_end: datetime,
    timeframe: str,
) -> List[int]:

    seconds = TIMEFRAME_SECONDS[
        timeframe
    ]

    start_key = datetime_key(
        gap_start
    )

    end_key = datetime_key(
        gap_end
    )

    if end_key < start_key:

        return []

    result: List[int] = []

    current = start_key

    max_points = 1_000_000

    while (
        current <= end_key
        and
        len(result) < max_points
    ):

        result.append(
            current
        )

        current += seconds

    if len(result) >= max_points:

        raise RuntimeError(
            "Gap range is unexpectedly large."
        )

    return result


def current_missing_keys(
    *,
    target: RecoveryTarget,
    existing_timestamp_keys: Set[int],
) -> List[int]:

    start_dt = parse_datetime(
        target.gap_start
    )

    end_dt = parse_datetime(
        target.gap_end
    )

    if (
        start_dt is None
        or end_dt is None
    ):

        raise RuntimeError(
            "Unable to parse target gap boundaries."
        )

    expected = expected_timestamp_keys(
        gap_start=start_dt,
        gap_end=end_dt,
        timeframe=target.timeframe,
    )

    return [
        key
        for key
        in expected
        if key not in existing_timestamp_keys
    ]


# ============================================================
# 22. API SESSION
# ============================================================


def build_session() -> requests.Session:

    session = requests.Session()

    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
    )

    return session


# ============================================================
# 23. API REQUEST WITH RETRY
# ============================================================


def api_get_candles(
    *,
    session: requests.Session,
    timeframe: str,
    market: str,
    to_time: datetime,
    count: int,
) -> List[Dict[str, Any]]:

    if timeframe not in API_PATHS:

        raise RuntimeError(
            f"Unsupported timeframe: {timeframe}"
        )

    count = max(
        1,
        min(
            API_MAX_COUNT,
            int(count),
        ),
    )

    url = (
        UPBIT_API_BASE
        + API_PATHS[timeframe]
    )

    params = {
        "market": market,
        "to": datetime_to_upbit_to(
            to_time
        ),
        "count": count,
    }

    last_error = ""

    for attempt in range(
        1,
        API_MAX_RETRIES + 1,
    ):

        try:

            response = session.get(
                url,
                params=params,
                timeout=API_TIMEOUT_SECONDS,
            )

            if response.status_code == 200:

                payload = response.json()

                if not isinstance(
                    payload,
                    list,
                ):

                    raise RuntimeError(
                        "Upbit candle API returned "
                        "non-list JSON."
                    )

                result: List[
                    Dict[str, Any]
                ] = []

                for item in payload:

                    if isinstance(
                        item,
                        dict,
                    ):

                        result.append(
                            item
                        )

                time.sleep(
                    API_REQUEST_INTERVAL_SECONDS
                )

                return result

            if (
                response.status_code == 429
                or
                500 <= response.status_code < 600
            ):

                last_error = (
                    f"HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )

            else:

                raise RuntimeError(
                    "Upbit API request failed. "
                    f"HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )

        except KeyboardInterrupt:

            raise

        except Exception as exc:

            last_error = (
                f"{type(exc).__name__}: {exc}"
            )

        if attempt < API_MAX_RETRIES:

            wait_seconds = min(
                30.0,
                API_RETRY_BASE_SECONDS
                * (2 ** (attempt - 1)),
            )

            print(
                f"[RETRY] API attempt "
                f"{attempt}/{API_MAX_RETRIES} failed."
            )

            print(
                f"        {last_error}"
            )

            print(
                f"        wait={wait_seconds:.1f}s"
            )

            time.sleep(
                wait_seconds
            )

    raise RuntimeError(
        "Upbit API request failed after retries: "
        f"{last_error}"
    )


# ============================================================
# 24. API CANDLE TIMESTAMP
# ============================================================


def api_candle_datetime(
    candle: Dict[str, Any],
) -> Optional[datetime]:

    value = candle.get(
        "candle_date_time_utc"
    )

    if value:

        return parse_datetime(
            value
        )

    value = candle.get(
        "timestamp"
    )

    if value is not None:

        return parse_datetime(
            value
        )

    return None


# ============================================================
# 25. DOWNLOAD EXACT GAP CANDLES
# ============================================================


def download_missing_candles(
    *,
    session: requests.Session,
    target: RecoveryTarget,
    missing_keys: Sequence[int],
) -> Dict[int, Dict[str, Any]]:

    if not missing_keys:

        return {}

    missing_set = set(
        missing_keys
    )

    earliest_needed = min(
        missing_set
    )

    latest_needed = max(
        missing_set
    )

    seconds = TIMEFRAME_SECONDS[
        target.timeframe
    ]

    # Upbit "to" is used as an exclusive upper boundary.
    # Move one timeframe beyond the newest desired candle.
    cursor = datetime.fromtimestamp(
        latest_needed + seconds,
        tz=timezone.utc,
    )

    found: Dict[
        int,
        Dict[str, Any],
    ] = {}

    previous_oldest: Optional[int] = None

    max_pages = max(
        10,
        (
            (
                latest_needed
                - earliest_needed
            )
            // max(
                1,
                seconds * API_MAX_COUNT
            )
        )
        + 20,
    )

    for page_number in range(
        1,
        max_pages + 1,
    ):

        remaining = (
            missing_set
            - set(found.keys())
        )

        if not remaining:

            break

        candles = api_get_candles(
            session=session,
            timeframe=target.timeframe,
            market=target.market,
            to_time=cursor,
            count=API_MAX_COUNT,
        )

        if not candles:

            break

        page_keys: List[int] = []

        for candle in candles:

            dt = api_candle_datetime(
                candle
            )

            if dt is None:

                continue

            key = datetime_key(
                dt
            )

            page_keys.append(
                key
            )

            # Strict filter:
            # only exact missing timestamps are accepted.
            if key in missing_set:

                if key not in found:

                    found[key] = candle

        if not page_keys:

            break

        oldest = min(
            page_keys
        )

        newest = max(
            page_keys
        )

        print(
            f"        API page={page_number} "
            f"candles={len(candles)} "
            f"range="
            f"{datetime.fromtimestamp(oldest, timezone.utc).isoformat()}"
            f" ~ "
            f"{datetime.fromtimestamp(newest, timezone.utc).isoformat()} "
            f"matched={len(found)}/{len(missing_set)}"
        )

        if (
            previous_oldest is not None
            and
            oldest >= previous_oldest
        ):

            # Cursor is not moving backward.
            # Stop to prevent infinite loops.
            break

        previous_oldest = oldest

        if oldest <= earliest_needed:

            break

        # Move the cursor to the oldest returned candle.
        # Since Upbit treats "to" as an upper boundary,
        # the next request moves further backward.
        cursor = datetime.fromtimestamp(
            oldest,
            tz=timezone.utc,
        )

    return found


# ============================================================
# 26. API -> EXISTING CSV ROW
# ============================================================


def api_value_for_column(
    *,
    column: str,
    candle: Dict[str, Any],
) -> Any:

    normalized = normalize_column_name(
        column
    )

    # Exact normalized API-key match first.
    for key, value in candle.items():

        if (
            normalize_column_name(
                str(key)
            )
            == normalized
        ):

            return value

    aliases = {
        "datetime":
            "candle_date_time_utc",

        "date_time":
            "candle_date_time_utc",

        "utc_time":
            "candle_date_time_utc",

        "time":
            "candle_date_time_utc",

        "open":
            "opening_price",

        "high":
            "high_price",

        "low":
            "low_price",

        "close":
            "trade_price",

        "volume":
            "candle_acc_trade_volume",

        "value":
            "candle_acc_trade_price",

        "trade_volume":
            "candle_acc_trade_volume",

        "trade_value":
            "candle_acc_trade_price",
    }

    api_key = aliases.get(
        normalized
    )

    if (
        api_key
        and
        api_key in candle
    ):

        return candle.get(
            api_key
        )

    return ""


def build_csv_row_from_api(
    *,
    fieldnames: Sequence[str],
    timestamp_column: str,
    candle: Dict[str, Any],
) -> Dict[str, str]:

    row: Dict[str, str] = {}

    for column in fieldnames:

        value = api_value_for_column(
            column=column,
            candle=candle,
        )

        if value is None:

            value = ""

        row[column] = str(
            value
        )

    # Timestamp column is safety critical.
    timestamp_value = row.get(
        timestamp_column,
        "",
    )

    parsed = parse_datetime(
        timestamp_value
    )

    if parsed is None:

        candle_dt = api_candle_datetime(
            candle
        )

        if candle_dt is None:

            raise RuntimeError(
                "API candle has no usable timestamp."
            )

        normalized_timestamp_column = (
            normalize_column_name(
                timestamp_column
            )
        )

        if (
            normalized_timestamp_column
            == "timestamp"
        ):

            row[timestamp_column] = str(
                int(
                    candle_dt.timestamp()
                    * 1000
                )
            )

        else:

            row[timestamp_column] = (
                candle_dt.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
            )

    return row


# ============================================================
# 27. SORT OHLCV ROWS
# ============================================================


def sort_rows_by_timestamp(
    *,
    rows: Sequence[Dict[str, str]],
    timestamp_column: str,
) -> List[Dict[str, str]]:

    def key_function(
        row: Dict[str, str],
    ) -> int:

        dt = parse_datetime(
            row.get(
                timestamp_column,
                "",
            )
        )

        if dt is None:

            raise RuntimeError(
                "Unable to sort row with invalid timestamp."
            )

        return datetime_key(
            dt
        )

    return sorted(
        rows,
        key=key_function,
    )


# ============================================================
# 28. TEMPORARY OHLCV WRITE
# ============================================================


def write_ohlcv_temp(
    *,
    original_path: Path,
    fieldnames: Sequence[str],
    rows: Sequence[Dict[str, str]],
) -> Path:

    temp_path = Path(
        str(original_path)
        + ".second_pass.tmp"
    )

    if temp_path.exists():

        temp_path.unlink()

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:

            writer.writerow(
                {
                    field:
                    row.get(
                        field,
                        "",
                    )
                    for field
                    in fieldnames
                }
            )

        handle.flush()

        os.fsync(
            handle.fileno()
        )

    return temp_path


# ============================================================
# 29. TEMPORARY FILE VALIDATION
# ============================================================


def validate_temp_recovery(
    *,
    original_rows: Sequence[Dict[str, str]],
    temp_path: Path,
    fieldnames: Sequence[str],
    timestamp_column: str,
    expected_insert_keys: Set[int],
) -> Tuple[
    List[Dict[str, str]],
    Set[int],
]:

    temp_fields, temp_rows = read_csv(
        temp_path
    )

    if list(temp_fields) != list(fieldnames):

        raise RuntimeError(
            "Temporary OHLCV header changed."
        )

    if (
        len(temp_rows)
        != len(original_rows)
        + len(expected_insert_keys)
    ):

        raise RuntimeError(
            "Temporary OHLCV row count mismatch. "
            f"original={len(original_rows)} "
            f"inserted={len(expected_insert_keys)} "
            f"temp={len(temp_rows)}"
        )

    original_map = build_timestamp_map(
        original_rows,
        timestamp_column,
    )

    temp_map = build_timestamp_map(
        temp_rows,
        timestamp_column,
    )

    original_keys = set(
        original_map.keys()
    )

    temp_keys = set(
        temp_map.keys()
    )

    # Every original timestamp must remain.
    if not original_keys.issubset(
        temp_keys
    ):

        missing_original = (
            original_keys
            - temp_keys
        )

        raise RuntimeError(
            "Temporary OHLCV lost existing timestamps. "
            f"count={len(missing_original)}"
        )

    # No unexpected new timestamps.
    actual_new_keys = (
        temp_keys
        - original_keys
    )

    if (
        actual_new_keys
        != expected_insert_keys
    ):

        raise RuntimeError(
            "Temporary OHLCV contains unexpected "
            "new timestamp set."
        )

    # Existing rows must remain byte-logically identical
    # at the CSV field level.
    for key in original_keys:

        before = original_map[key]

        after = temp_map[key]

        for field in fieldnames:

            if (
                clean_text(
                    before.get(
                        field,
                        "",
                    )
                )
                !=
                clean_text(
                    after.get(
                        field,
                        "",
                    )
                )
            ):

                raise RuntimeError(
                    "Existing OHLCV candle was modified. "
                    f"timestamp={key} "
                    f"field={field}"
                )

    # Verify chronological order.
    ordered_keys: List[int] = []

    for row in temp_rows:

        dt = parse_datetime(
            row.get(
                timestamp_column,
                "",
            )
        )

        if dt is None:

            raise RuntimeError(
                "Temporary OHLCV contains invalid timestamp."
            )

        ordered_keys.append(
            datetime_key(
                dt
            )
        )

    if (
        ordered_keys
        != sorted(
            ordered_keys
        )
    ):

        raise RuntimeError(
            "Temporary OHLCV is not chronologically sorted."
        )

    if (
        len(ordered_keys)
        != len(set(ordered_keys))
    ):

        raise RuntimeError(
            "Temporary OHLCV contains duplicate timestamps."
        )

    return (
        temp_rows,
        temp_keys,
    )


# ============================================================
# 30. HASH INVENTORY
# ============================================================


def build_hash_inventory() -> List[
    Dict[str, Any]
]:

    rows: List[
        Dict[str, Any]
    ] = []

    for timeframe in TIMEFRAMES:

        directory = (
            OHLCV_DIR
            / timeframe
        )

        for path in sorted(
            directory.glob("*.csv")
        ):

            try:

                _, csv_rows = read_csv(
                    path
                )

                row_count = len(
                    csv_rows
                )

            except Exception:

                row_count = -1

            rows.append(
                {
                    "timeframe":
                        timeframe,

                    "market":
                        path.stem.upper(),

                    "path":
                        str(
                            path.relative_to(
                                PROJECT_ROOT
                            )
                        ),

                    "rows":
                        row_count,

                    "sha256":
                        sha256_file(
                            path
                        ),
                }
            )

    if (
        len(rows)
        != EXPECTED_OHLCV_FILES
    ):

        raise RuntimeError(
            "SHA256 inventory count mismatch. "
            f"expected={EXPECTED_OHLCV_FILES} "
            f"found={len(rows)}"
        )

    return rows


def hash_inventory_map(
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:

    result: Dict[
        str,
        Dict[str, Any],
    ] = {}

    for row in rows:

        key = (
            f"{clean_text(row.get('timeframe'))}|"
            f"{clean_text(row.get('market')).upper()}"
        )

        result[key] = dict(
            row
        )

    return result


# ============================================================
# 31. CHECKPOINT
# ============================================================


def load_checkpoint() -> Dict[str, Any]:

    if not CHECKPOINT_FILE.is_file():

        return {
            "program": PROGRAM,
            "version": VERSION,
            "execution_mode": EXECUTION_MODE,
            "created_utc": utc_now_iso(),
            "updated_utc": utc_now_iso(),
            "status": "IN_PROGRESS",
            "target_file_sha256": "",
            "targets": {},
        }

    try:

        with CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8-sig",
        ) as handle:

            data = json.load(
                handle
            )

        if not isinstance(
            data,
            dict,
        ):

            raise RuntimeError(
                "Checkpoint root is not an object."
            )

        if (
            data.get("program")
            != PROGRAM
        ):

            raise RuntimeError(
                "Checkpoint program mismatch."
            )

        if (
            data.get("version")
            != VERSION
        ):

            raise RuntimeError(
                "Checkpoint version mismatch."
            )

        if not isinstance(
            data.get(
                "targets",
                {},
            ),
            dict,
        ):

            data["targets"] = {}

        return data

    except Exception as exc:

        raise RuntimeError(
            "Unable to safely load recovery checkpoint: "
            f"{exc}"
        ) from exc


def save_checkpoint(
    checkpoint: Dict[str, Any],
) -> None:

    checkpoint[
        "updated_utc"
    ] = utc_now_iso()

    atomic_json_write(
        CHECKPOINT_FILE,
        checkpoint,
    )


def checkpoint_target(
    checkpoint: Dict[str, Any],
    event_key: str,
) -> Dict[str, Any]:

    targets = checkpoint.setdefault(
        "targets",
        {},
    )

    value = targets.get(
        event_key,
        {},
    )

    if isinstance(
        value,
        dict,
    ):

        return value

    return {}


def update_checkpoint_target(
    *,
    checkpoint: Dict[str, Any],
    target: RecoveryTarget,
    status: str,
    missing_before: int,
    inserted_candles: int,
    missing_after: int,
    ohlcv_path: Path,
    before_sha256: str,
    after_sha256: str,
    message: str,
) -> None:

    targets = checkpoint.setdefault(
        "targets",
        {},
    )

    targets[target.event_key] = {
        "market":
            target.market,

        "timeframe":
            target.timeframe,

        "gap_start":
            target.gap_start,

        "gap_end":
            target.gap_end,

        "status":
            status,

        "missing_before":
            missing_before,

        "inserted_candles":
            inserted_candles,

        "missing_after":
            missing_after,

        "ohlcv_file":
            str(
                ohlcv_path
            ),

        "before_sha256":
            before_sha256,

        "after_sha256":
            after_sha256,

        "message":
            message,

        "updated_utc":
            utc_now_iso(),
    }

    save_checkpoint(
        checkpoint
    )


# ============================================================
# 32. STATUS LOG
# ============================================================


def append_status(
    row: Dict[str, Any],
) -> None:

    STATUS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    exists = STATUS_FILE.is_file()

    with STATUS_FILE.open(
        "a",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=STATUS_FIELDS,
            extrasaction="ignore",
        )

        if not exists:

            writer.writeheader()

        writer.writerow(
            {
                field:
                row.get(
                    field,
                    "",
                )
                for field
                in STATUS_FIELDS
            }
        )

        handle.flush()


# ============================================================
# 33. CHECK WHETHER COMPLETED TARGET IS REALLY COMPLETE
# ============================================================


def verify_target_current_state(
    target: RecoveryTarget,
) -> Tuple[
    Path,
    List[str],
    List[Dict[str, str]],
    str,
    List[int],
]:

    path = find_ohlcv_file(
        target.timeframe,
        target.market,
    )

    if path is None:

        raise FileNotFoundError(
            "Matching OHLCV file not found: "
            f"{target.timeframe}/{target.market}"
        )

    fieldnames, rows, timestamp_column = (
        read_ohlcv(
            path
        )
    )

    timestamp_map = build_timestamp_map(
        rows,
        timestamp_column,
    )

    missing = current_missing_keys(
        target=target,
        existing_timestamp_keys=set(
            timestamp_map.keys()
        ),
    )

    return (
        path,
        fieldnames,
        rows,
        timestamp_column,
        missing,
    )


# ============================================================
# 34. PROCESS ONE TARGET
# ============================================================


def process_target(
    *,
    session: requests.Session,
    checkpoint: Dict[str, Any],
    target: RecoveryTarget,
    target_index: int,
    target_total: int,
) -> Dict[str, Any]:

    now = utc_now_iso()

    base: Dict[str, Any] = {
        "utc_time":
            now,

        "version":
            VERSION,

        "target_index":
            target_index,

        "target_total":
            target_total,

        "timeframe":
            target.timeframe,

        "market":
            target.market,

        "gap_start":
            target.gap_start,

        "gap_end":
            target.gap_end,

        "expected_missing_from_plan":
            target.expected_missing,

        "missing_before":
            0,

        "downloaded_matching_candles":
            0,

        "inserted_candles":
            0,

        "missing_after":
            0,

        "rows_before":
            0,

        "rows_after":
            0,

        "before_sha256":
            "",

        "after_sha256":
            "",

        "file_changed":
            False,

        "status":
            "",

        "message":
            "",

        "event_key":
            target.event_key,

        "source_report":
            target.source_report,

        "source_row":
            target.source_row,

        "planner_version":
            target.planner_version,
    }

    # --------------------------------------------------------
    # Re-read actual OHLCV every time.
    #
    # This is intentionally done even when checkpoint says
    # SUCCESS. Checkpoint alone is never trusted.
    # --------------------------------------------------------

    (
        ohlcv_path,
        fieldnames,
        original_rows,
        timestamp_column,
        missing_before_keys,
    ) = verify_target_current_state(
        target
    )

    before_sha256 = sha256_file(
        ohlcv_path
    )

    rows_before = len(
        original_rows
    )

    base["before_sha256"] = (
        before_sha256
    )

    base["rows_before"] = (
        rows_before
    )

    base["missing_before"] = len(
        missing_before_keys
    )

    # --------------------------------------------------------
    # Resume / already complete
    # --------------------------------------------------------

    previous = checkpoint_target(
        checkpoint,
        target.event_key,
    )

    if not missing_before_keys:

        if (
            clean_text(
                previous.get(
                    "status"
                )
            )
            in FINAL_STATUSES
        ):

            status = STATUS_SKIPPED

            message = (
                "Checkpoint indicates completed target and "
                "actual OHLCV confirms no missing timestamps."
            )

        else:

            status = STATUS_ALREADY_RECOVERED

            message = (
                "Actual OHLCV already contains every expected "
                "timestamp in this target range."
            )

        base["status"] = status

        base["message"] = message

        base["missing_after"] = 0

        base["rows_after"] = (
            rows_before
        )

        base["after_sha256"] = (
            before_sha256
        )

        update_checkpoint_target(
            checkpoint=checkpoint,
            target=target,
            status=STATUS_ALREADY_RECOVERED,
            missing_before=0,
            inserted_candles=0,
            missing_after=0,
            ohlcv_path=ohlcv_path,
            before_sha256=before_sha256,
            after_sha256=before_sha256,
            message=message,
        )

        return base

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    downloaded = (
        download_missing_candles(
            session=session,
            target=target,
            missing_keys=missing_before_keys,
        )
    )

    base[
        "downloaded_matching_candles"
    ] = len(
        downloaded
    )

    if not downloaded:

        message = (
            "Upbit API returned no candles matching the "
            "currently missing timestamp set."
        )

        base["status"] = (
            STATUS_NO_DATA
        )

        base["message"] = message

        base["missing_after"] = len(
            missing_before_keys
        )

        base["rows_after"] = (
            rows_before
        )

        base["after_sha256"] = (
            before_sha256
        )

        update_checkpoint_target(
            checkpoint=checkpoint,
            target=target,
            status=STATUS_NO_DATA,
            missing_before=len(
                missing_before_keys
            ),
            inserted_candles=0,
            missing_after=len(
                missing_before_keys
            ),
            ohlcv_path=ohlcv_path,
            before_sha256=before_sha256,
            after_sha256=before_sha256,
            message=message,
        )

        return base

    # --------------------------------------------------------
    # Re-read source immediately before write.
    #
    # Prevents using stale state if the file changed while
    # API requests were running.
    # --------------------------------------------------------

    current_sha256 = sha256_file(
        ohlcv_path
    )

    if (
        current_sha256
        != before_sha256
    ):

        raise RuntimeError(
            "OHLCV changed while recovery target was "
            "being downloaded. Write aborted safely. "
            f"{ohlcv_path}"
        )

    (
        current_fields,
        current_rows,
        current_timestamp_column,
    ) = read_ohlcv(
        ohlcv_path
    )

    if (
        list(current_fields)
        != list(fieldnames)
    ):

        raise RuntimeError(
            "OHLCV header changed during recovery."
        )

    if (
        current_timestamp_column
        != timestamp_column
    ):

        raise RuntimeError(
            "OHLCV timestamp column changed during recovery."
        )

    current_map = build_timestamp_map(
        current_rows,
        timestamp_column,
    )

    current_existing_keys = set(
        current_map.keys()
    )

    current_missing = current_missing_keys(
        target=target,
        existing_timestamp_keys=current_existing_keys,
    )

    current_missing_set = set(
        current_missing
    )

    # Only API candles that are STILL missing right now.
    insert_api = {
        key:
        candle
        for key, candle
        in downloaded.items()
        if key in current_missing_set
        and key not in current_existing_keys
    }

    if not insert_api:

        message = (
            "No API candle remained eligible for insertion "
            "after current OHLCV was revalidated."
        )

        base["status"] = (
            STATUS_NO_DATA
        )

        base["message"] = message

        base["missing_after"] = len(
            current_missing
        )

        base["rows_after"] = len(
            current_rows
        )

        base["after_sha256"] = (
            current_sha256
        )

        update_checkpoint_target(
            checkpoint=checkpoint,
            target=target,
            status=STATUS_NO_DATA,
            missing_before=len(
                missing_before_keys
            ),
            inserted_candles=0,
            missing_after=len(
                current_missing
            ),
            ohlcv_path=ohlcv_path,
            before_sha256=before_sha256,
            after_sha256=current_sha256,
            message=message,
        )

        return base

    # --------------------------------------------------------
    # Build new rows
    # --------------------------------------------------------

    new_rows: List[
        Dict[str, str]
    ] = [
        dict(row)
        for row
        in current_rows
    ]

    inserted_keys: Set[int] = set()

    for key in sorted(
        insert_api.keys()
    ):

        if key in current_existing_keys:

            # Existing candle always wins.
            continue

        candle = insert_api[
            key
        ]

        row = build_csv_row_from_api(
            fieldnames=fieldnames,
            timestamp_column=timestamp_column,
            candle=candle,
        )

        row_dt = parse_datetime(
            row.get(
                timestamp_column,
                "",
            )
        )

        if row_dt is None:

            raise RuntimeError(
                "Generated API CSV row has invalid timestamp."
            )

        generated_key = datetime_key(
            row_dt
        )

        if generated_key != key:

            raise RuntimeError(
                "Generated API row timestamp mismatch. "
                f"expected={key} "
                f"generated={generated_key}"
            )

        if generated_key not in current_missing_set:

            raise RuntimeError(
                "Attempted insertion of timestamp that is "
                "not in current missing set."
            )

        new_rows.append(
            row
        )

        inserted_keys.add(
            generated_key
        )

    if not inserted_keys:

        message = (
            "No eligible missing candle was inserted."
        )

        base["status"] = (
            STATUS_NO_DATA
        )

        base["message"] = message

        base["missing_after"] = len(
            current_missing
        )

        base["rows_after"] = len(
            current_rows
        )

        base["after_sha256"] = (
            current_sha256
        )

        update_checkpoint_target(
            checkpoint=checkpoint,
            target=target,
            status=STATUS_NO_DATA,
            missing_before=len(
                missing_before_keys
            ),
            inserted_candles=0,
            missing_after=len(
                current_missing
            ),
            ohlcv_path=ohlcv_path,
            before_sha256=before_sha256,
            after_sha256=current_sha256,
            message=message,
        )

        return base

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    new_rows = sort_rows_by_timestamp(
        rows=new_rows,
        timestamp_column=timestamp_column,
    )

    # --------------------------------------------------------
    # Write temporary file
    # --------------------------------------------------------

    temp_path = write_ohlcv_temp(
        original_path=ohlcv_path,
        fieldnames=fieldnames,
        rows=new_rows,
    )

    try:

        # ----------------------------------------------------
        # Validate temporary file
        # ----------------------------------------------------

        (
            validated_rows,
            validated_keys,
        ) = validate_temp_recovery(
            original_rows=current_rows,
            temp_path=temp_path,
            fieldnames=fieldnames,
            timestamp_column=timestamp_column,
            expected_insert_keys=inserted_keys,
        )

        # ----------------------------------------------------
        # Calculate missing AFTER against temporary state
        # ----------------------------------------------------

        missing_after_keys = (
            current_missing_keys(
                target=target,
                existing_timestamp_keys=validated_keys,
            )
        )

        # ----------------------------------------------------
        # Last source mutation check before replace
        # ----------------------------------------------------

        last_source_sha256 = (
            sha256_file(
                ohlcv_path
            )
        )

        if (
            last_source_sha256
            != current_sha256
        ):

            raise RuntimeError(
                "Original OHLCV changed before atomic "
                "replacement. Replacement aborted."
            )

        # ----------------------------------------------------
        # Atomic replacement
        # ----------------------------------------------------

        os.replace(
            temp_path,
            ohlcv_path,
        )

    finally:

        if temp_path.exists():

            try:

                temp_path.unlink()

            except Exception:

                pass

    # --------------------------------------------------------
    # Verify final file after replacement
    # --------------------------------------------------------

    (
        final_fields,
        final_rows,
        final_timestamp_column,
    ) = read_ohlcv(
        ohlcv_path
    )

    if (
        list(final_fields)
        != list(fieldnames)
    ):

        raise RuntimeError(
            "Final OHLCV header mismatch after replacement."
        )

    if (
        final_timestamp_column
        != timestamp_column
    ):

        raise RuntimeError(
            "Final OHLCV timestamp column mismatch."
        )

    final_map = build_timestamp_map(
        final_rows,
        timestamp_column,
    )

    final_keys = set(
        final_map.keys()
    )

    # All original timestamps must still exist.
    original_map = build_timestamp_map(
        current_rows,
        timestamp_column,
    )

    original_keys = set(
        original_map.keys()
    )

    if not original_keys.issubset(
        final_keys
    ):

        raise RuntimeError(
            "Final OHLCV lost original timestamps."
        )

    # Existing candles must still be identical.
    for key in original_keys:

        before_row = original_map[
            key
        ]

        after_row = final_map[
            key
        ]

        for field in fieldnames:

            if (
                clean_text(
                    before_row.get(
                        field,
                        "",
                    )
                )
                !=
                clean_text(
                    after_row.get(
                        field,
                        "",
                    )
                )
            ):

                raise RuntimeError(
                    "Existing candle changed after final "
                    "replacement. "
                    f"timestamp={key} "
                    f"field={field}"
                )

    final_missing = current_missing_keys(
        target=target,
        existing_timestamp_keys=final_keys,
    )

    after_sha256 = sha256_file(
        ohlcv_path
    )

    inserted_count = len(
        inserted_keys
    )

    missing_after_count = len(
        final_missing
    )

    if missing_after_count == 0:

        status = STATUS_SUCCESS

        message = (
            "All currently recoverable timestamps in this "
            "target range are present after second-pass "
            "recovery."
        )

    else:

        status = STATUS_PARTIAL

        message = (
            "Second-pass recovery inserted available API "
            "candles, but some timestamps remain missing."
        )

    base["inserted_candles"] = (
        inserted_count
    )

    base["missing_after"] = (
        missing_after_count
    )

    base["rows_after"] = len(
        final_rows
    )

    base["after_sha256"] = (
        after_sha256
    )

    base["file_changed"] = (
        after_sha256
        != before_sha256
    )

    base["status"] = status

    base["message"] = message

    update_checkpoint_target(
        checkpoint=checkpoint,
        target=target,
        status=status,
        missing_before=len(
            missing_before_keys
        ),
        inserted_candles=inserted_count,
        missing_after=missing_after_count,
        ohlcv_path=ohlcv_path,
        before_sha256=before_sha256,
        after_sha256=after_sha256,
        message=message,
    )

    return base


# ============================================================
# 35. BUILD SUMMARY
# ============================================================


def build_summary_rows(
    detail_rows: Sequence[
        Dict[str, Any]
    ],
) -> List[Dict[str, Any]]:

    grouped: Dict[
        str,
        List[Dict[str, Any]],
    ] = {}

    for row in detail_rows:

        timeframe = clean_text(
            row.get(
                "timeframe"
            )
        )

        grouped.setdefault(
            timeframe,
            [],
        ).append(
            row
        )

    result: List[
        Dict[str, Any]
    ] = []

    for timeframe in TIMEFRAMES:

        rows = grouped.get(
            timeframe,
            [],
        )

        counts = Counter(
            clean_text(
                row.get(
                    "status"
                )
            )
            for row
            in rows
        )

        result.append(
            {
                "timeframe":
                    timeframe,

                "targets":
                    len(rows),

                "success":
                    counts[
                        STATUS_SUCCESS
                    ],

                "partial":
                    counts[
                        STATUS_PARTIAL
                    ],

                "already_recovered":
                    counts[
                        STATUS_ALREADY_RECOVERED
                    ],

                "no_data":
                    counts[
                        STATUS_NO_DATA
                    ],

                "failed":
                    counts[
                        STATUS_FAILED
                    ],

                "skipped":
                    counts[
                        STATUS_SKIPPED
                    ],

                "missing_before":
                    sum(
                        safe_int(
                            row.get(
                                "missing_before"
                            )
                        )
                        for row
                        in rows
                    ),

                "inserted_candles":
                    sum(
                        safe_int(
                            row.get(
                                "inserted_candles"
                            )
                        )
                        for row
                        in rows
                    ),

                "missing_after":
                    sum(
                        safe_int(
                            row.get(
                                "missing_after"
                            )
                        )
                        for row
                        in rows
                    ),
            }
        )

    return result


# ============================================================
# 36. RESULT JSON
# ============================================================


def build_result_json(
    *,
    inventory_before: Dict[str, Any],
    inventory_after: Dict[str, Any],
    target_count: int,
    detail_rows: Sequence[Dict[str, Any]],
    before_hashes: Sequence[Dict[str, Any]],
    after_hashes: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:

    counts = Counter(
        clean_text(
            row.get(
                "status"
            )
        )
        for row
        in detail_rows
    )

    before_map = hash_inventory_map(
        before_hashes
    )

    after_map = hash_inventory_map(
        after_hashes
    )

    changed_files = []

    for key in sorted(
        before_map.keys()
    ):

        before = before_map[
            key
        ]

        after = after_map.get(
            key
        )

        if after is None:

            raise RuntimeError(
                "OHLCV file disappeared after recovery: "
                f"{key}"
            )

        if (
            clean_text(
                before.get(
                    "sha256"
                )
            )
            !=
            clean_text(
                after.get(
                    "sha256"
                )
            )
        ):

            changed_files.append(
                {
                    "timeframe":
                        before.get(
                            "timeframe"
                        ),

                    "market":
                        before.get(
                            "market"
                        ),

                    "before_rows":
                        before.get(
                            "rows"
                        ),

                    "after_rows":
                        after.get(
                            "rows"
                        ),

                    "before_sha256":
                        before.get(
                            "sha256"
                        ),

                    "after_sha256":
                        after.get(
                            "sha256"
                        ),
                }
            )

    return {
        "program":
            PROGRAM,

        "version":
            VERSION,

        "execution_mode":
            EXECUTION_MODE,

        "created_utc":
            utc_now_iso(),

        "source": {
            "second_pass_targets":
                str(
                    SECOND_PASS_TARGET_FILE
                ),

            "second_pass_targets_sha256":
                sha256_file(
                    SECOND_PASS_TARGET_FILE
                ),

            "second_pass_plan":
                str(
                    SECOND_PASS_PLAN_FILE
                ),

            "second_pass_plan_sha256":
                sha256_file(
                    SECOND_PASS_PLAN_FILE
                ),
        },

        "inventory_before":
            inventory_before,

        "inventory_after":
            inventory_after,

        "summary": {
            "targets":
                target_count,

            "processed_rows":
                len(
                    detail_rows
                ),

            "success":
                counts[
                    STATUS_SUCCESS
                ],

            "partial":
                counts[
                    STATUS_PARTIAL
                ],

            "already_recovered":
                counts[
                    STATUS_ALREADY_RECOVERED
                ],

            "no_data":
                counts[
                    STATUS_NO_DATA
                ],

            "failed":
                counts[
                    STATUS_FAILED
                ],

            "skipped":
                counts[
                    STATUS_SKIPPED
                ],

            "missing_before":
                sum(
                    safe_int(
                        row.get(
                            "missing_before"
                        )
                    )
                    for row
                    in detail_rows
                ),

            "inserted_candles":
                sum(
                    safe_int(
                        row.get(
                            "inserted_candles"
                        )
                    )
                    for row
                    in detail_rows
                ),

            "remaining_missing":
                sum(
                    safe_int(
                        row.get(
                            "missing_after"
                        )
                    )
                    for row
                    in detail_rows
                ),

            "changed_ohlcv_files":
                len(
                    changed_files
                ),
        },

        "changed_files":
            changed_files,

        "safety":
            SAFETY,
    }


# ============================================================
# 37. FINAL GLOBAL SAFETY VALIDATION
# ============================================================


def validate_global_after_state(
    *,
    before_hashes: Sequence[Dict[str, Any]],
    after_hashes: Sequence[Dict[str, Any]],
    detail_rows: Sequence[Dict[str, Any]],
) -> None:

    before_map = hash_inventory_map(
        before_hashes
    )

    after_map = hash_inventory_map(
        after_hashes
    )

    if (
        set(before_map.keys())
        != set(after_map.keys())
    ):

        raise RuntimeError(
            "OHLCV inventory changed unexpectedly. "
            "File set before/after is different."
        )

    allowed_changed: Set[str] = set()

    for row in detail_rows:

        if not row.get(
            "file_changed"
        ):

            continue

        key = (
            f"{clean_text(row.get('timeframe'))}|"
            f"{clean_text(row.get('market')).upper()}"
        )

        allowed_changed.add(
            key
        )

    actual_changed: Set[str] = set()

    for key in before_map:

        before = before_map[
            key
        ]

        after = after_map[
            key
        ]

        before_hash = clean_text(
            before.get(
                "sha256"
            )
        )

        after_hash = clean_text(
            after.get(
                "sha256"
            )
        )

        before_rows = safe_int(
            before.get(
                "rows"
            ),
            -1,
        )

        after_rows = safe_int(
            after.get(
                "rows"
            ),
            -1,
        )

        if (
            before_hash
            != after_hash
        ):

            actual_changed.add(
                key
            )

            if (
                after_rows
                < before_rows
            ):

                raise RuntimeError(
                    "OHLCV row count decreased. "
                    f"{key}: "
                    f"before={before_rows} "
                    f"after={after_rows}"
                )

    unexpected_changed = (
        actual_changed
        - allowed_changed
    )

    if unexpected_changed:

        raise RuntimeError(
            "Unexpected OHLCV files changed outside "
            "second-pass target processing: "
            f"{sorted(unexpected_changed)}"
        )


# ============================================================
# 38. MAIN RECOVERY
# ============================================================


def run_recovery() -> int:

    print(
        "=" * 70
    )

    print(
        "UPBIT SURGE MONITOR"
    )

    print(
        "HISTORY GAP SECOND-PASS RECOVERY"
    )

    print(
        VERSION
    )

    print(
        "=" * 70
    )

    print(
        f"Execution mode : {EXECUTION_MODE}"
    )

    print(
        f"Expected markets : {EXPECTED_MARKETS}"
    )

    print(
        f"Expected OHLCV files : {EXPECTED_OHLCV_FILES}"
    )

    print()

    # --------------------------------------------------------
    # Prepare directories
    # --------------------------------------------------------

    prepare_directories()

    # --------------------------------------------------------
    # Inventory BEFORE
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "VERIFY OHLCV INVENTORY BEFORE RECOVERY"
    )

    print(
        "=" * 70
    )

    inventory_before = (
        inventory_ohlcv()
    )

    print(
        f"H1 : {inventory_before['h1']}"
    )

    print(
        f"H4 : {inventory_before['h4']}"
    )

    print(
        f"D1 : {inventory_before['d1']}"
    )

    print(
        f"Total : {inventory_before['total']}"
    )

    print(
        f"Markets : {inventory_before['markets']}"
    )

    print()

    # --------------------------------------------------------
    # Validate planner input
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "VERIFY SECOND-PASS PLANNER INPUT"
    )

    print(
        "=" * 70
    )

    plan = validate_target_inputs()

    print(
        "[PASS] Second-pass target CSV"
    )

    print(
        f"       {SECOND_PASS_TARGET_FILE}"
    )

    print(
        "[PASS] Second-pass plan JSON"
    )

    print(
        f"       planner={plan.get('program')}"
    )

    print(
        f"       version={plan.get('version')}"
    )

    print()

    # --------------------------------------------------------
    # Load targets
    # --------------------------------------------------------

    targets = load_targets()

    print(
        f"Second-pass targets : {len(targets)}"
    )

    print()

    # --------------------------------------------------------
    # BEFORE SHA256
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "RECORD BEFORE OHLCV SHA256"
    )

    print(
        "=" * 70
    )

    before_hashes = (
        build_hash_inventory()
    )

    atomic_csv_write(
        BEFORE_SHA256_FILE,
        HASH_FIELDS,
        before_hashes,
    )

    print(
        f"[PASS] BEFORE SHA256 files : "
        f"{len(before_hashes)}"
    )

    print()

    # --------------------------------------------------------
    # Checkpoint
    # --------------------------------------------------------

    checkpoint = load_checkpoint()

    current_target_hash = sha256_file(
        SECOND_PASS_TARGET_FILE
    )

    checkpoint_target_hash = clean_text(
        checkpoint.get(
            "target_file_sha256"
        )
    )

    if (
        checkpoint_target_hash
        and
        checkpoint_target_hash
        != current_target_hash
    ):

        print(
            "[INFO] Target input changed since previous run."
        )

        print(
            "[INFO] Existing checkpoint target states will "
            "not be blindly trusted."
        )

    checkpoint[
        "target_file_sha256"
    ] = current_target_hash

    checkpoint[
        "status"
    ] = "IN_PROGRESS"

    checkpoint[
        "target_total"
    ] = len(
        targets
    )

    save_checkpoint(
        checkpoint
    )

    # --------------------------------------------------------
    # API session
    # --------------------------------------------------------

    session = build_session()

    # --------------------------------------------------------
    # Process targets
    # --------------------------------------------------------

    detail_rows: List[
        Dict[str, Any]
    ] = []

    total = len(
        targets
    )

    print(
        "=" * 70
    )

    print(
        "EXECUTE SECOND-PASS RECOVERY"
    )

    print(
        "=" * 70
    )

    print()

    for index, target in enumerate(
        targets,
        start=1,
    ):

        print(
            "-" * 70
        )

        print(
            f"[TARGET] [{index}/{total}] "
            f"{target.timeframe.upper()} "
            f"{target.market}"
        )

        print(
            f"         {target.gap_start}"
        )

        print(
            f"      -> {target.gap_end}"
        )

        print(
            f"         plan expected_missing="
            f"{target.expected_missing}"
        )

        try:

            row = process_target(
                session=session,
                checkpoint=checkpoint,
                target=target,
                target_index=index,
                target_total=total,
            )

        except KeyboardInterrupt:

            checkpoint[
                "status"
            ] = "INTERRUPTED"

            checkpoint[
                "interrupted_utc"
            ] = utc_now_iso()

            save_checkpoint(
                checkpoint
            )

            print()

            print(
                "[INTERRUPTED] Checkpoint saved."
            )

            print(
                "[SAFE] Completed atomic writes remain valid."
            )

            print(
                "[SAFE] Current unfinished target will be "
                "revalidated on the next run."
            )

            raise

        except Exception as exc:

            message = (
                f"{type(exc).__name__}: {exc}"
            )

            print(
                f"[FAILED] {message}"
            )

            row = {
                "utc_time":
                    utc_now_iso(),

                "version":
                    VERSION,

                "target_index":
                    index,

                "target_total":
                    total,

                "timeframe":
                    target.timeframe,

                "market":
                    target.market,

                "gap_start":
                    target.gap_start,

                "gap_end":
                    target.gap_end,

                "expected_missing_from_plan":
                    target.expected_missing,

                "missing_before":
                    0,

                "downloaded_matching_candles":
                    0,

                "inserted_candles":
                    0,

                "missing_after":
                    0,

                "rows_before":
                    0,

                "rows_after":
                    0,

                "before_sha256":
                    "",

                "after_sha256":
                    "",

                "file_changed":
                    False,

                "status":
                    STATUS_FAILED,

                "message":
                    message,

                "event_key":
                    target.event_key,

                "source_report":
                    target.source_report,

                "source_row":
                    target.source_row,

                "planner_version":
                    target.planner_version,
            }

            ohlcv_path = find_ohlcv_file(
                target.timeframe,
                target.market,
            )

            if ohlcv_path is not None:

                current_hash = ""

                try:

                    current_hash = (
                        sha256_file(
                            ohlcv_path
                        )
                    )

                except Exception:

                    pass

                update_checkpoint_target(
                    checkpoint=checkpoint,
                    target=target,
                    status=STATUS_FAILED,
                    missing_before=0,
                    inserted_candles=0,
                    missing_after=0,
                    ohlcv_path=ohlcv_path,
                    before_sha256=current_hash,
                    after_sha256=current_hash,
                    message=message,
                )

        detail_rows.append(
            row
        )

        append_status(
            {
                "utc_time":
                    utc_now_iso(),

                "version":
                    VERSION,

                "mode":
                    EXECUTION_MODE,

                "target_index":
                    index,

                "target_total":
                    total,

                "timeframe":
                    target.timeframe,

                "market":
                    target.market,

                "event_key":
                    target.event_key,

                "status":
                    row.get(
                        "status",
                        "",
                    ),

                "missing_before":
                    row.get(
                        "missing_before",
                        0,
                    ),

                "inserted_candles":
                    row.get(
                        "inserted_candles",
                        0,
                    ),

                "missing_after":
                    row.get(
                        "missing_after",
                        0,
                    ),

                "message":
                    row.get(
                        "message",
                        "",
                    ),
            }
        )

        print(
            f"[RESULT] status="
            f"{row.get('status')}"
        )

        print(
            f"         missing_before="
            f"{row.get('missing_before')}"
        )

        print(
            f"         inserted="
            f"{row.get('inserted_candles')}"
        )

        print(
            f"         missing_after="
            f"{row.get('missing_after')}"
        )

        print()

        # Persist detail after every target.
        # If the runner stops later, completed progress
        # is still visible.
        atomic_csv_write(
            RECOVERY_DETAIL_FILE,
            DETAIL_FIELDS,
            detail_rows,
        )

    # --------------------------------------------------------
    # Close API session
    # --------------------------------------------------------

    session.close()

    # --------------------------------------------------------
    # Revalidate every target from actual OHLCV
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "FINAL REVALIDATION OF SECOND-PASS TARGETS"
    )

    print(
        "=" * 70
    )

    remaining_total = 0

    remaining_targets = 0

    for index, target in enumerate(
        targets,
        start=1,
    ):

        (
            _,
            _,
            _,
            _,
            remaining,
        ) = verify_target_current_state(
            target
        )

        remaining_count = len(
            remaining
        )

        remaining_total += (
            remaining_count
        )

        if remaining_count > 0:

            remaining_targets += 1

        print(
            f"[VERIFY] [{index}/{total}] "
            f"{target.timeframe.upper()} "
            f"{target.market} "
            f"remaining={remaining_count}"
        )

    print()

    # --------------------------------------------------------
    # Inventory AFTER
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "VERIFY OHLCV INVENTORY AFTER RECOVERY"
    )

    print(
        "=" * 70
    )

    inventory_after = (
        inventory_ohlcv()
    )

    if (
        inventory_before["total"]
        != inventory_after["total"]
    ):

        raise RuntimeError(
            "OHLCV file count changed during recovery."
        )

    if (
        inventory_before["markets"]
        != inventory_after["markets"]
    ):

        raise RuntimeError(
            "OHLCV market inventory changed during recovery."
        )

    print(
        f"H1 : {inventory_after['h1']}"
    )

    print(
        f"H4 : {inventory_after['h4']}"
    )

    print(
        f"D1 : {inventory_after['d1']}"
    )

    print(
        f"Total : {inventory_after['total']}"
    )

    print()

    # --------------------------------------------------------
    # AFTER SHA256
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "RECORD AFTER OHLCV SHA256"
    )

    print(
        "=" * 70
    )

    after_hashes = (
        build_hash_inventory()
    )

    atomic_csv_write(
        AFTER_SHA256_FILE,
        HASH_FIELDS,
        after_hashes,
    )

    print(
        f"[PASS] AFTER SHA256 files : "
        f"{len(after_hashes)}"
    )

    print()

    # --------------------------------------------------------
    # Global safety validation
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "GLOBAL OHLCV SAFETY VALIDATION"
    )

    print(
        "=" * 70
    )

    validate_global_after_state(
        before_hashes=before_hashes,
        after_hashes=after_hashes,
        detail_rows=detail_rows,
    )

    print(
        "[PASS] OHLCV file set preserved."
    )

    print(
        "[PASS] No OHLCV row-count decrease detected."
    )

    print(
        "[PASS] Only authorized target files changed."
    )

    print()

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    summary_rows = (
        build_summary_rows(
            detail_rows
        )
    )

    atomic_csv_write(
        RECOVERY_SUMMARY_FILE,
        SUMMARY_FIELDS,
        summary_rows,
    )

    # --------------------------------------------------------
    # Result JSON
    # --------------------------------------------------------

    result_payload = (
        build_result_json(
            inventory_before=inventory_before,
            inventory_after=inventory_after,
            target_count=len(
                targets
            ),
            detail_rows=detail_rows,
            before_hashes=before_hashes,
            after_hashes=after_hashes,
        )
    )

    result_payload[
        "final_revalidation"
    ] = {
        "remaining_gap_targets":
            remaining_targets,

        "remaining_missing_timestamps":
            remaining_total,
    }

    atomic_json_write(
        RECOVERY_RESULT_FILE,
        result_payload,
    )

    # --------------------------------------------------------
    # Complete checkpoint LAST
    # --------------------------------------------------------

    counts = Counter(
        clean_text(
            row.get(
                "status"
            )
        )
        for row
        in detail_rows
    )

    checkpoint[
        "status"
    ] = "COMPLETED"

    checkpoint[
        "completed"
    ] = True

    checkpoint[
        "completed_utc"
    ] = utc_now_iso()

    checkpoint[
        "summary"
    ] = {
        "targets":
            len(
                targets
            ),

        "success":
            counts[
                STATUS_SUCCESS
            ],

        "partial":
            counts[
                STATUS_PARTIAL
            ],

        "already_recovered":
            counts[
                STATUS_ALREADY_RECOVERED
            ],

        "no_data":
            counts[
                STATUS_NO_DATA
            ],

        "failed":
            counts[
                STATUS_FAILED
            ],

        "skipped":
            counts[
                STATUS_SKIPPED
            ],

        "remaining_gap_targets":
            remaining_targets,

        "remaining_missing_timestamps":
            remaining_total,
    }

    checkpoint[
        "outputs"
    ] = {
        "summary":
            str(
                RECOVERY_SUMMARY_FILE
            ),

        "detail":
            str(
                RECOVERY_DETAIL_FILE
            ),

        "result":
            str(
                RECOVERY_RESULT_FILE
            ),

        "before_sha256":
            str(
                BEFORE_SHA256_FILE
            ),

        "after_sha256":
            str(
                AFTER_SHA256_FILE
            ),
    }

    save_checkpoint(
        checkpoint
    )

    # --------------------------------------------------------
    # Final console report
    # --------------------------------------------------------

    print(
        "=" * 70
    )

    print(
        "SECOND-PASS RECOVERY RESULT"
    )

    print(
        "=" * 70
    )

    print(
        f"Targets              : {len(targets)}"
    )

    print(
        f"SUCCESS              : "
        f"{counts[STATUS_SUCCESS]}"
    )

    print(
        f"PARTIAL              : "
        f"{counts[STATUS_PARTIAL]}"
    )

    print(
        f"ALREADY_RECOVERED    : "
        f"{counts[STATUS_ALREADY_RECOVERED]}"
    )

    print(
        f"NO_DATA              : "
        f"{counts[STATUS_NO_DATA]}"
    )

    print(
        f"FAILED               : "
        f"{counts[STATUS_FAILED]}"
    )

    print(
        f"SKIPPED              : "
        f"{counts[STATUS_SKIPPED]}"
    )

    print()

    print(
        "Inserted candles     : "
        f"{sum(safe_int(row.get('inserted_candles')) for row in detail_rows)}"
    )

    print(
        f"Remaining targets    : {remaining_targets}"
    )

    print(
        f"Remaining timestamps : {remaining_total}"
    )

    print()

    print(
        "Generated:"
    )

    print(
        f"  {RECOVERY_SUMMARY_FILE}"
    )

    print(
        f"  {RECOVERY_DETAIL_FILE}"
    )

    print(
        f"  {RECOVERY_RESULT_FILE}"
    )

    print(
        f"  {BEFORE_SHA256_FILE}"
    )

    print(
        f"  {AFTER_SHA256_FILE}"
    )

    print(
        f"  {CHECKPOINT_FILE}"
    )

    print(
        f"  {STATUS_FILE}"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  Existing candle overwrite : FORBIDDEN"
    )

    print(
        "  OHLCV deletion             : FORBIDDEN"
    )

    print(
        "  Missing candle insertion   : ENABLED"
    )

    print(
        "  Upbit API download         : ENABLED"
    )

    print(
        "  Feature build              : DISABLED"
    )

    print(
        "  256 Detector               : DISABLED"
    )

    print(
        "  Future labels              : DISABLED"
    )

    print(
        "  Prediction                 : DISABLED"
    )

    print(
        "  Trading                    : DISABLED"
    )

    print(
        "  Git reset                  : DISABLED"
    )

    print(
        "  Git clean                  : DISABLED"
    )

    print(
        "  Git commit                 : DISABLED"
    )

    print(
        "  Git push                   : DISABLED"
    )

    print()

    if counts[
        STATUS_FAILED
    ] > 0:

        print(
            "[WARNING] One or more targets failed."
        )

        print(
            "[INFO] Re-running this program is safe."
        )

        print(
            "[INFO] Actual OHLCV state will be revalidated "
            "before resume."
        )

    elif remaining_total > 0:

        print(
            "[PASS] Second-pass recovery execution completed."
        )

        print(
            "[INFO] Some historical timestamps remain "
            "unavailable after the second pass."
        )

    else:

        print(
            "[PASS] Second-pass recovery completed."
        )

        print(
            "[PASS] No target timestamps remain missing."
        )

    print(
        "=" * 70
    )

    return 0


# ============================================================
# 39. MAIN
# ============================================================


def main() -> int:

    try:

        return run_recovery()

    except KeyboardInterrupt:

        print()

        print(
            "=" * 70
        )

        print(
            "SECOND-PASS RECOVERY INTERRUPTED"
        )

        print(
            "=" * 70
        )

        print(
            "[SAFE] Completed target writes were atomic."
        )

        print(
            "[SAFE] Existing candles were never intentionally "
            "overwritten."
        )

        print(
            "[SAFE] The next run will re-read actual OHLCV "
            "before deciding what to skip."
        )

        print(
            "[RESUME] Run the same program again."
        )

        print(
            "=" * 70
        )

        return 130

    except Exception as exc:

        print()

        print(
            "=" * 70
        )

        print(
            "SECOND-PASS RECOVERY FATAL ERROR"
        )

        print(
            "=" * 70
        )

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print()

        traceback.print_exc()

        print()

        print(
            "[SAFETY] Existing OHLCV deletion was not "
            "requested."
        )

        print(
            "[SAFETY] Existing candle overwrite was not "
            "permitted by design."
        )

        print(
            "[SAFETY] Feature build was not executed."
        )

        print(
            "[SAFETY] 256 detector was not executed."
        )

        print(
            "[SAFETY] Future labels were not generated."
        )

        print(
            "[SAFETY] Prediction/trading was not executed."
        )

        print(
            "[SAFETY] Git reset/clean/commit/push was not "
            "executed."
        )

        print()

        print(
            "[RESUME] Fix the reported problem and run the "
            "same program again."
        )

        print(
            "[RESUME] Actual OHLCV state will be checked "
            "again before recovery continues."
        )

        print(
            "=" * 70
        )

        return 1


# ============================================================
# 40. ENTRY POINT
# ============================================================


if __name__ == "__main__":

    sys.exit(
        main()
    )


# ============================================================
# END
# ============================================================
