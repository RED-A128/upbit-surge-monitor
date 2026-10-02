# ============================================================
# Upbit Surge Monitor
# plan_history_gap_retry.py
# Clean V001
# ============================================================
#
# PURPOSE
#
#   Build a SAFE second-pass historical gap recovery plan from
#   the results produced by:
#
#       audit_history_gap_recovery.py
#
#
# THIS PROGRAM DOES NOT:
#
#   - download candles
#   - call the Upbit API
#   - repair OHLCV
#   - modify OHLCV
#   - delete OHLCV
#   - run feature generation
#   - run 256 detector
#   - create future labels
#   - run prediction
#   - run trading
#   - execute Git reset / clean / commit / push
#
#
# EXECUTION MODE
#
#   READ_ONLY_SECOND_PASS_PLANNING
#
#
# PRIMARY INPUT
#
#   data/recovery/
#       history_gap_recovery_retry_candidates.csv
#
#
# SUPPORTING INPUT
#
#   data/reports/history_gap_recovery_audit/
#       history_gap_recovery_audit_summary.csv
#       history_gap_recovery_audit_detail.csv
#       history_gap_recovery_audit_result.json
#
#   data/ohlcv/
#       h1/
#       h4/
#       d1/
#
#
# OUTPUT
#
#   data/recovery/
#       history_gap_second_pass_targets.csv
#       history_gap_second_pass_plan.json
#
#   data/reports/history_gap_retry_plan/
#       history_gap_retry_plan_summary.csv
#       history_gap_retry_plan_detail.csv
#
#   data/validation/
#       plan_history_gap_retry_checkpoint.json
#
#   data/
#       plan_history_gap_retry_status.csv
#
#
# CLASSIFICATIONS
#
#   READY_SECOND_PASS
#       Gap still exists and is suitable for second-pass
#       recovery planning.
#
#   NO_LONGER_GAP
#       Current OHLCV no longer contains the reported gap.
#
#   STRUCTURAL_GAP
#       Gap appears to be outside the observed OHLCV interior
#       or otherwise structural.
#
#   OUTSIDE_API_RANGE
#       Source evidence explicitly identifies the gap as
#       unavailable/outside recoverable API history.
#
#   INVALID_SOURCE
#       Required source information is invalid.
#
#   MANUAL_REVIEW
#       Automatic classification is not safe.
#
#   SKIP_ALREADY_PLANNED
#       Valid completed checkpoint already covers the same
#       immutable input state.
#
#
# IMPORTANT
#
#   Clean V001 does NOT guess that an old gap is outside the
#   Upbit API range merely from age.
#
#   OUTSIDE_API_RANGE requires explicit source evidence.
#
# ============================================================


from __future__ import annotations


import csv
import hashlib
import json
import os
import re
import sys
import traceback

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


# ============================================================
# 1. PROGRAM
# ============================================================


PROGRAM = "plan_history_gap_retry.py"

VERSION = "Clean V001"

EXECUTION_MODE = "READ_ONLY_SECOND_PASS_PLANNING"


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
# 2. ROOT PATHS
# ============================================================


PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"

OHLCV_DIR = DATA_DIR / "ohlcv"

RECOVERY_DIR = DATA_DIR / "recovery"

REPORTS_DIR = DATA_DIR / "reports"

VALIDATION_DIR = DATA_DIR / "validation"


AUDIT_REPORT_DIR = (
    REPORTS_DIR
    / "history_gap_recovery_audit"
)


RETRY_PLAN_REPORT_DIR = (
    REPORTS_DIR
    / "history_gap_retry_plan"
)


# ============================================================
# 3. INPUT FILES
# ============================================================


RETRY_CANDIDATE_FILE = (
    RECOVERY_DIR
    / "history_gap_recovery_retry_candidates.csv"
)


AUDIT_SUMMARY_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_recovery_audit_summary.csv"
)


AUDIT_DETAIL_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_recovery_audit_detail.csv"
)


AUDIT_RESULT_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_recovery_audit_result.json"
)


# ============================================================
# 4. OUTPUT FILES
# ============================================================


SECOND_PASS_TARGET_FILE = (
    RECOVERY_DIR
    / "history_gap_second_pass_targets.csv"
)


SECOND_PASS_PLAN_FILE = (
    RECOVERY_DIR
    / "history_gap_second_pass_plan.json"
)


PLAN_SUMMARY_FILE = (
    RETRY_PLAN_REPORT_DIR
    / "history_gap_retry_plan_summary.csv"
)


PLAN_DETAIL_FILE = (
    RETRY_PLAN_REPORT_DIR
    / "history_gap_retry_plan_detail.csv"
)


CHECKPOINT_FILE = (
    VALIDATION_DIR
    / "plan_history_gap_retry_checkpoint.json"
)


STATUS_FILE = (
    DATA_DIR
    / "plan_history_gap_retry_status.csv"
)


# ============================================================
# 5. SAFETY
# ============================================================


SAFETY = {
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
# 6. CLASSIFICATION
# ============================================================


READY_SECOND_PASS = "READY_SECOND_PASS"

NO_LONGER_GAP = "NO_LONGER_GAP"

STRUCTURAL_GAP = "STRUCTURAL_GAP"

OUTSIDE_API_RANGE = "OUTSIDE_API_RANGE"

INVALID_SOURCE = "INVALID_SOURCE"

MANUAL_REVIEW = "MANUAL_REVIEW"

SKIP_ALREADY_PLANNED = "SKIP_ALREADY_PLANNED"


CLASSIFICATIONS = (
    READY_SECOND_PASS,
    NO_LONGER_GAP,
    STRUCTURAL_GAP,
    OUTSIDE_API_RANGE,
    INVALID_SOURCE,
    MANUAL_REVIEW,
    SKIP_ALREADY_PLANNED,
)


# ============================================================
# 7. COLUMN ALIASES
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


GAP_COUNT_ALIASES = (
    "gap_count",
    "gap_events",
    "gap_report_count",
    "reported_gap_count",
)


MISSING_COUNT_ALIASES = (
    "missing_count",
    "missing",
    "missing_estimate",
    "missing_est",
    "expected_missing",
    "expected_missing_before",
    "remaining",
    "remaining_after",
    "remaining_missing",
)


STATUS_ALIASES = (
    "status",
    "audit_status",
    "recovery_status",
    "original_status",
)


CLASSIFICATION_ALIASES = (
    "classification",
    "gap_classification",
    "audit_classification",
)


REASON_ALIASES = (
    "reason",
    "message",
    "audit_reason",
    "retry_reason",
)


EVENT_KEY_ALIASES = (
    "event_key",
    "gap_key",
    "recovery_key",
    "key",
)


SOURCE_ROW_ALIASES = (
    "source_row",
    "row",
    "row_number",
)


SOURCE_REPORT_ALIASES = (
    "source_report",
    "report",
    "source_file",
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
# 8. OUTPUT SCHEMAS
# ============================================================


DETAIL_FIELDS = (
    "utc_time",
    "version",
    "candidate_index",
    "candidate_total",
    "timeframe",
    "market",
    "gap_start",
    "gap_end",
    "reported_gap_count",
    "reported_missing_count",
    "current_missing_count",
    "ohlcv_file",
    "ohlcv_rows",
    "ohlcv_first_utc",
    "ohlcv_last_utc",
    "source_status",
    "source_classification",
    "classification",
    "reason",
    "event_key",
    "source_report",
    "source_row",
)


TARGET_FIELDS = (
    "timeframe",
    "market",
    "gap_start",
    "gap_end",
    "expected_missing",
    "classification",
    "reason",
    "event_key",
    "source_report",
    "source_row",
    "planner_version",
)


SUMMARY_FIELDS = (
    "timeframe",
    "market",
    "candidate_events",
    "ready_second_pass",
    "no_longer_gap",
    "structural_gap",
    "outside_api_range",
    "invalid_source",
    "manual_review",
    "skip_already_planned",
    "current_missing_total",
)


STATUS_FIELDS = (
    "utc_time",
    "version",
    "mode",
    "candidate_index",
    "candidate_total",
    "timeframe",
    "market",
    "classification",
    "current_missing_count",
    "status",
    "message",
)


# ============================================================
# 9. DATA CLASS
# ============================================================


@dataclass
class Candidate:

    market: str

    timeframe: str

    gap_start: str

    gap_end: str

    gap_count: int

    missing_count: int

    source_status: str

    source_classification: str

    source_reason: str

    event_key: str

    source_report: str

    source_row: str

    raw: Dict[str, str]


# ============================================================
# 10. BASIC UTILITIES
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

    text = re.sub(
        r"[^a-z0-9]+",
        "_",
        text,
    )

    return text.strip("_")


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
# 11. DATETIME
# ============================================================


def parse_datetime(
    value: Any,
) -> Optional[datetime]:

    text = clean_text(value)

    if not text:
        return None

    text = text.replace(
        "Z",
        "+00:00",
    )

    candidates = [
        text,
        text.replace(
            " ",
            "T",
        ),
    ]

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


# ============================================================
# 12. COLUMN DETECTION
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
# 13. DIRECTORY PREPARATION
# ============================================================


def prepare_output_directories() -> None:

    directories = (
        RECOVERY_DIR,
        RETRY_PLAN_REPORT_DIR,
        VALIDATION_DIR,
    )

    for directory in directories:

        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


# ============================================================
# 14. INVENTORY
# ============================================================


def inventory_ohlcv() -> Dict[str, Any]:

    result: Dict[str, Any] = {
        "h1": 0,
        "h4": 0,
        "d1": 0,
        "total": 0,
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
            file.stem.upper()
            for file
            in files
        }

    for timeframe in TIMEFRAMES:

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
# 15. AUDIT INPUT VALIDATION
# ============================================================


def validate_audit_inputs() -> Dict[str, Any]:

    required = (
        RETRY_CANDIDATE_FILE,
        AUDIT_SUMMARY_FILE,
        AUDIT_DETAIL_FILE,
        AUDIT_RESULT_FILE,
    )

    for path in required:

        if not path.is_file():

            raise FileNotFoundError(
                f"Required audit input missing: {path}"
            )

        if path.stat().st_size <= 0:

            raise RuntimeError(
                f"Required audit input is empty: {path}"
            )

    try:

        with AUDIT_RESULT_FILE.open(
            "r",
            encoding="utf-8-sig",
        ) as handle:

            result = json.load(
                handle
            )

    except Exception as exc:

        raise RuntimeError(
            "Unable to read audit result JSON: "
            f"{exc}"
        ) from exc

    if not isinstance(
        result,
        dict,
    ):

        raise RuntimeError(
            "Audit result JSON root must be an object."
        )

    return result


# ============================================================
# 16. LOAD RETRY CANDIDATES
# ============================================================


def load_retry_candidates() -> Tuple[
    List[Candidate],
    List[str],
]:

    fieldnames, rows = read_csv(
        RETRY_CANDIDATE_FILE
    )

    candidates: List[
        Candidate
    ] = []

    for index, row in enumerate(
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

        gap_count = safe_int(
            value_from_aliases(
                row,
                fieldnames,
                GAP_COUNT_ALIASES,
            )
        )

        missing_count = safe_int(
            value_from_aliases(
                row,
                fieldnames,
                MISSING_COUNT_ALIASES,
            )
        )

        source_status = value_from_aliases(
            row,
            fieldnames,
            STATUS_ALIASES,
        )

        source_classification = (
            value_from_aliases(
                row,
                fieldnames,
                CLASSIFICATION_ALIASES,
            )
        )

        source_reason = value_from_aliases(
            row,
            fieldnames,
            REASON_ALIASES,
        )

        event_key = value_from_aliases(
            row,
            fieldnames,
            EVENT_KEY_ALIASES,
        )

        source_report = (
            value_from_aliases(
                row,
                fieldnames,
                SOURCE_REPORT_ALIASES,
            )
        )

        source_row = value_from_aliases(
            row,
            fieldnames,
            SOURCE_ROW_ALIASES,
        )

        if not source_row:

            source_row = str(
                index
            )

        if not event_key:

            event_key = (
                f"{timeframe}|"
                f"{market}|"
                f"{gap_start}|"
                f"{gap_end}"
            )

        candidates.append(
            Candidate(
                market=market,
                timeframe=timeframe,
                gap_start=gap_start,
                gap_end=gap_end,
                gap_count=gap_count,
                missing_count=missing_count,
                source_status=source_status,
                source_classification=(
                    source_classification
                ),
                source_reason=source_reason,
                event_key=event_key,
                source_report=source_report,
                source_row=source_row,
                raw=row,
            )
        )

    return (
        candidates,
        fieldnames,
    )


# ============================================================
# 17. OHLCV FILE DISCOVERY
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
# 18. READ OHLCV TIMESTAMPS
# ============================================================


def read_ohlcv_timestamps(
    path: Path,
) -> Tuple[
    List[datetime],
    int,
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

    timestamps: List[
        datetime
    ] = []

    for row in rows:

        value = row.get(
            timestamp_column,
            "",
        )

        parsed = parse_datetime(
            value
        )

        if parsed is not None:

            timestamps.append(
                parsed
            )

    if not timestamps:

        raise RuntimeError(
            f"No valid timestamps found: {path}"
        )

    timestamps = sorted(
        set(timestamps)
    )

    return (
        timestamps,
        len(rows),
    )


# ============================================================
# 19. SOURCE EVIDENCE
# ============================================================


def explicit_outside_api_evidence(
    candidate: Candidate,
) -> bool:

    combined = " ".join(
        (
            candidate.source_status,
            candidate.source_classification,
            candidate.source_reason,
        )
    ).lower()

    keywords = (
        "outside_api_range",
        "outside api range",
        "api_range_exceeded",
        "api range exceeded",
        "not_available_from_api",
        "not available from api",
        "api_unavailable",
        "api unavailable",
        "before_listing",
        "before listing",
        "pre_listing",
        "pre-listing",
    )

    return any(
        keyword in combined
        for keyword
        in keywords
    )


def explicit_structural_evidence(
    candidate: Candidate,
) -> bool:

    combined = " ".join(
        (
            candidate.source_status,
            candidate.source_classification,
            candidate.source_reason,
        )
    ).lower()

    keywords = (
        "structural",
        "listing boundary",
        "delisting boundary",
        "market boundary",
        "boundary gap",
        "expected boundary",
    )

    return any(
        keyword in combined
        for keyword
        in keywords
    )


# ============================================================
# 20. GAP ANALYSIS
# ============================================================


def expected_missing_timestamps(
    *,
    start: datetime,
    end: datetime,
    timeframe: str,
) -> List[int]:

    seconds = TIMEFRAME_SECONDS[
        timeframe
    ]

    start_key = datetime_key(
        start
    )

    end_key = datetime_key(
        end
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


def analyze_candidate(
    candidate: Candidate,
) -> Dict[str, Any]:

    now = utc_now_iso()

    base: Dict[str, Any] = {
        "utc_time": now,
        "version": VERSION,
        "timeframe": candidate.timeframe,
        "market": candidate.market,
        "gap_start": candidate.gap_start,
        "gap_end": candidate.gap_end,
        "reported_gap_count": (
            candidate.gap_count
        ),
        "reported_missing_count": (
            candidate.missing_count
        ),
        "current_missing_count": 0,
        "ohlcv_file": "",
        "ohlcv_rows": 0,
        "ohlcv_first_utc": "",
        "ohlcv_last_utc": "",
        "source_status": (
            candidate.source_status
        ),
        "source_classification": (
            candidate.source_classification
        ),
        "classification": "",
        "reason": "",
        "event_key": candidate.event_key,
        "source_report": (
            candidate.source_report
        ),
        "source_row": candidate.source_row,
    }

    # --------------------------------------------------------
    # Validate market
    # --------------------------------------------------------

    if not candidate.market:

        base["classification"] = (
            INVALID_SOURCE
        )

        base["reason"] = (
            "Retry candidate has no market."
        )

        return base

    # --------------------------------------------------------
    # Validate timeframe
    # --------------------------------------------------------

    if (
        candidate.timeframe
        not in TIMEFRAMES
    ):

        base["classification"] = (
            INVALID_SOURCE
        )

        base["reason"] = (
            "Retry candidate has invalid timeframe: "
            f"{candidate.timeframe}"
        )

        return base

    # --------------------------------------------------------
    # Explicit source evidence
    # --------------------------------------------------------

    if explicit_outside_api_evidence(
        candidate
    ):

        base["classification"] = (
            OUTSIDE_API_RANGE
        )

        base["reason"] = (
            "Source audit explicitly identifies this "
            "candidate as outside recoverable API range."
        )

        return base

    # --------------------------------------------------------
    # OHLCV file
    # --------------------------------------------------------

    ohlcv_file = find_ohlcv_file(
        candidate.timeframe,
        candidate.market,
    )

    if ohlcv_file is None:

        base["classification"] = (
            INVALID_SOURCE
        )

        base["reason"] = (
            "Matching OHLCV source file was not found."
        )

        return base

    base["ohlcv_file"] = str(
        ohlcv_file
    )

    try:

        timestamps, row_count = (
            read_ohlcv_timestamps(
                ohlcv_file
            )
        )

    except Exception as exc:

        base["classification"] = (
            INVALID_SOURCE
        )

        base["reason"] = (
            "Unable to read OHLCV timestamps: "
            f"{exc}"
        )

        return base

    base["ohlcv_rows"] = (
        row_count
    )

    first = timestamps[0]

    last = timestamps[-1]

    base["ohlcv_first_utc"] = (
        first.isoformat()
    )

    base["ohlcv_last_utc"] = (
        last.isoformat()
    )

    # --------------------------------------------------------
    # Parse gap boundaries
    # --------------------------------------------------------

    gap_start = parse_datetime(
        candidate.gap_start
    )

    gap_end = parse_datetime(
        candidate.gap_end
    )

    if (
        gap_start is None
        or
        gap_end is None
    ):

        base["classification"] = (
            MANUAL_REVIEW
        )

        base["reason"] = (
            "Gap start/end timestamps cannot be "
            "safely parsed."
        )

        return base

    if gap_end < gap_start:

        base["classification"] = (
            INVALID_SOURCE
        )

        base["reason"] = (
            "Gap end is earlier than gap start."
        )

        return base

    # --------------------------------------------------------
    # Structural boundary
    # --------------------------------------------------------

    if explicit_structural_evidence(
        candidate
    ):

        base["classification"] = (
            STRUCTURAL_GAP
        )

        base["reason"] = (
            "Source audit explicitly identifies "
            "the gap as structural."
        )

        return base

    if (
        gap_start < first
        or
        gap_end > last
    ):

        base["classification"] = (
            STRUCTURAL_GAP
        )

        base["reason"] = (
            "Reported gap is outside the interior "
            "range of the currently stored OHLCV file."
        )

        return base

    # --------------------------------------------------------
    # Determine current missing timestamps
    # --------------------------------------------------------

    expected = (
        expected_missing_timestamps(
            start=gap_start,
            end=gap_end,
            timeframe=candidate.timeframe,
        )
    )

    actual = {
        datetime_key(timestamp)
        for timestamp
        in timestamps
    }

    missing = [
        timestamp
        for timestamp
        in expected
        if timestamp not in actual
    ]

    base[
        "current_missing_count"
    ] = len(
        missing
    )

    # --------------------------------------------------------
    # Gap no longer exists
    # --------------------------------------------------------

    if not missing:

        base["classification"] = (
            NO_LONGER_GAP
        )

        base["reason"] = (
            "All expected timestamps in the reported "
            "gap range are now present in OHLCV."
        )

        return base

    # --------------------------------------------------------
    # Alignment validation
    # --------------------------------------------------------

    seconds = TIMEFRAME_SECONDS[
        candidate.timeframe
    ]

    first_key = datetime_key(
        first
    )

    alignment_invalid = any(
        (
            timestamp - first_key
        )
        % seconds
        != 0
        for timestamp
        in expected
    )

    if alignment_invalid:

        base["classification"] = (
            MANUAL_REVIEW
        )

        base["reason"] = (
            "Gap timestamps do not align safely with "
            "the current OHLCV timeframe grid."
        )

        return base

    # --------------------------------------------------------
    # READY
    # --------------------------------------------------------

    base["classification"] = (
        READY_SECOND_PASS
    )

    base["reason"] = (
        "Gap still exists inside the current OHLCV "
        "history and is eligible for second-pass "
        "recovery planning."
    )

    return base


# ============================================================
# 21. CHECKPOINT INPUT FINGERPRINT
# ============================================================


def build_input_fingerprint() -> Dict[str, Any]:

    files = (
        RETRY_CANDIDATE_FILE,
        AUDIT_SUMMARY_FILE,
        AUDIT_DETAIL_FILE,
        AUDIT_RESULT_FILE,
    )

    records: Dict[
        str,
        Dict[str, Any],
    ] = {}

    digest = hashlib.sha256()

    for path in files:

        file_hash = sha256_file(
            path
        )

        size = path.stat().st_size

        records[str(path)] = {
            "sha256": file_hash,
            "size": size,
        }

        digest.update(
            str(path).encode(
                "utf-8"
            )
        )

        digest.update(
            file_hash.encode(
                "ascii"
            )
        )

        digest.update(
            str(size).encode(
                "ascii"
            )
        )

    # --------------------------------------------------------
    # OHLCV state fingerprint
    #
    # We intentionally include all 870 file metadata/hashes.
    # This makes resume safe: a previous completed plan cannot
    # be reused after OHLCV changes.
    # --------------------------------------------------------

    ohlcv_digest = hashlib.sha256()

    ohlcv_count = 0

    for timeframe in TIMEFRAMES:

        directory = (
            OHLCV_DIR
            / timeframe
        )

        for path in sorted(
            directory.glob("*.csv")
        ):

            relative = path.relative_to(
                DATA_DIR
            )

            file_hash = sha256_file(
                path
            )

            ohlcv_digest.update(
                str(relative).encode(
                    "utf-8"
                )
            )

            ohlcv_digest.update(
                file_hash.encode(
                    "ascii"
                )
            )

            ohlcv_count += 1

    if (
        ohlcv_count
        != EXPECTED_OHLCV_FILES
    ):

        raise RuntimeError(
            "Unable to build fingerprint because OHLCV "
            f"count is {ohlcv_count}, expected "
            f"{EXPECTED_OHLCV_FILES}."
        )

    return {
        "source_files": records,
        "source_fingerprint": (
            digest.hexdigest()
        ),
        "ohlcv_file_count": (
            ohlcv_count
        ),
        "ohlcv_fingerprint": (
            ohlcv_digest.hexdigest()
        ),
    }


# ============================================================
# 22. OUTPUT VALIDATION
# ============================================================


def csv_has_fields(
    path: Path,
    required: Sequence[str],
) -> bool:

    if not path.is_file():

        return False

    try:

        fieldnames, _ = read_csv(
            path
        )

    except Exception:

        return False

    normalized = {
        normalize_column_name(
            field
        )
        for field
        in fieldnames
    }

    for field in required:

        if (
            normalize_column_name(
                field
            )
            not in normalized
        ):

            return False

    return True


def outputs_are_valid() -> bool:

    required_files = (
        SECOND_PASS_TARGET_FILE,
        SECOND_PASS_PLAN_FILE,
        PLAN_SUMMARY_FILE,
        PLAN_DETAIL_FILE,
    )

    for path in required_files:

        if not path.is_file():

            return False

        if path.stat().st_size <= 0:

            return False

    if not csv_has_fields(
        SECOND_PASS_TARGET_FILE,
        (
            "timeframe",
            "market",
            "classification",
            "event_key",
        ),
    ):

        return False

    if not csv_has_fields(
        PLAN_DETAIL_FILE,
        (
            "timeframe",
            "market",
            "classification",
            "reason",
        ),
    ):

        return False

    try:

        with SECOND_PASS_PLAN_FILE.open(
            "r",
            encoding="utf-8-sig",
        ) as handle:

            payload = json.load(
                handle
            )

    except Exception:

        return False

    if not isinstance(
        payload,
        dict,
    ):

        return False

    if (
        payload.get("program")
        != PROGRAM
    ):

        return False

    if (
        payload.get("version")
        != VERSION
    ):

        return False

    if (
        payload.get(
            "execution_mode"
        )
        != EXECUTION_MODE
    ):

        return False

    return True


# ============================================================
# 23. CHECKPOINT
# ============================================================


def load_checkpoint() -> Dict[str, Any]:

    if not CHECKPOINT_FILE.is_file():

        return {}

    try:

        with CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8-sig",
        ) as handle:

            data = json.load(
                handle
            )

        if isinstance(
            data,
            dict,
        ):

            return data

    except Exception:

        pass

    return {}


def checkpoint_is_reusable(
    checkpoint: Dict[str, Any],
    fingerprint: Dict[str, Any],
) -> bool:

    if not checkpoint:

        return False

    if (
        checkpoint.get("program")
        != PROGRAM
    ):

        return False

    if (
        checkpoint.get("version")
        != VERSION
    ):

        return False

    if (
        checkpoint.get("status")
        != "COMPLETED"
    ):

        return False

    if (
        checkpoint.get(
            "source_fingerprint"
        )
        != fingerprint.get(
            "source_fingerprint"
        )
    ):

        return False

    if (
        checkpoint.get(
            "ohlcv_fingerprint"
        )
        != fingerprint.get(
            "ohlcv_fingerprint"
        )
    ):

        return False

    if not outputs_are_valid():

        return False

    return True


def save_checkpoint(
    *,
    fingerprint: Dict[str, Any],
    candidate_count: int,
    classification_counts: Dict[str, int],
    target_count: int,
) -> None:

    payload = {
        "program": PROGRAM,
        "version": VERSION,
        "execution_mode": (
            EXECUTION_MODE
        ),
        "status": "COMPLETED",
        "completed": True,
        "updated_utc": utc_now_iso(),
        "source_fingerprint": (
            fingerprint[
                "source_fingerprint"
            ]
        ),
        "ohlcv_fingerprint": (
            fingerprint[
                "ohlcv_fingerprint"
            ]
        ),
        "ohlcv_file_count": (
            fingerprint[
                "ohlcv_file_count"
            ]
        ),
        "candidate_count": (
            candidate_count
        ),
        "second_pass_target_count": (
            target_count
        ),
        "classification_counts": (
            classification_counts
        ),
        "outputs": {
            "second_pass_targets":
                str(
                    SECOND_PASS_TARGET_FILE
                ),
            "second_pass_plan":
                str(
                    SECOND_PASS_PLAN_FILE
                ),
            "summary":
                str(
                    PLAN_SUMMARY_FILE
                ),
            "detail":
                str(
                    PLAN_DETAIL_FILE
                ),
        },
    }

    atomic_json_write(
        CHECKPOINT_FILE,
        payload,
    )


# ============================================================
# 24. STATUS
# ============================================================


def append_status(
    row: Dict[str, Any],
) -> None:

    STATUS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    exists = (
        STATUS_FILE.is_file()
    )

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


# ============================================================
# 25. BUILD SUMMARY
# ============================================================


def build_summary_rows(
    detail_rows: Sequence[
        Dict[str, Any]
    ],
) -> List[
    Dict[str, Any]
]:

    groups: Dict[
        Tuple[str, str],
        List[Dict[str, Any]],
    ] = {}

    for row in detail_rows:

        key = (
            clean_text(
                row.get(
                    "timeframe"
                )
            ),
            clean_text(
                row.get(
                    "market"
                )
            ),
        )

        groups.setdefault(
            key,
            [],
        ).append(
            row
        )

    result: List[
        Dict[str, Any]
    ] = []

    for key in sorted(
        groups.keys()
    ):

        timeframe, market = key

        rows = groups[key]

        counter = Counter(
            clean_text(
                row.get(
                    "classification"
                )
            )
            for row
            in rows
        )

        current_missing_total = sum(
            safe_int(
                row.get(
                    "current_missing_count"
                )
            )
            for row
            in rows
        )

        result.append(
            {
                "timeframe":
                    timeframe,

                "market":
                    market,

                "candidate_events":
                    len(rows),

                "ready_second_pass":
                    counter[
                        READY_SECOND_PASS
                    ],

                "no_longer_gap":
                    counter[
                        NO_LONGER_GAP
                    ],

                "structural_gap":
                    counter[
                        STRUCTURAL_GAP
                    ],

                "outside_api_range":
                    counter[
                        OUTSIDE_API_RANGE
                    ],

                "invalid_source":
                    counter[
                        INVALID_SOURCE
                    ],

                "manual_review":
                    counter[
                        MANUAL_REVIEW
                    ],

                "skip_already_planned":
                    counter[
                        SKIP_ALREADY_PLANNED
                    ],

                "current_missing_total":
                    current_missing_total,
            }
        )

    return result


# ============================================================
# 26. BUILD SECOND PASS TARGETS
# ============================================================


def build_target_rows(
    detail_rows: Sequence[
        Dict[str, Any]
    ],
) -> List[
    Dict[str, Any]
]:

    result: List[
        Dict[str, Any]
    ] = []

    seen: Set[str] = set()

    for row in detail_rows:

        if (
            row.get(
                "classification"
            )
            != READY_SECOND_PASS
        ):

            continue

        event_key = clean_text(
            row.get(
                "event_key"
            )
        )

        if event_key in seen:

            continue

        seen.add(
            event_key
        )

        result.append(
            {
                "timeframe":
                    row.get(
                        "timeframe",
                        "",
                    ),

                "market":
                    row.get(
                        "market",
                        "",
                    ),

                "gap_start":
                    row.get(
                        "gap_start",
                        "",
                    ),

                "gap_end":
                    row.get(
                        "gap_end",
                        "",
                    ),

                "expected_missing":
                    row.get(
                        "current_missing_count",
                        0,
                    ),

                "classification":
                    READY_SECOND_PASS,

                "reason":
                    row.get(
                        "reason",
                        "",
                    ),

                "event_key":
                    event_key,

                "source_report":
                    row.get(
                        "source_report",
                        "",
                    ),

                "source_row":
                    row.get(
                        "source_row",
                        "",
                    ),

                "planner_version":
                    VERSION,
            }
        )

    result.sort(
        key=lambda row: (
            clean_text(
                row.get(
                    "timeframe"
                )
            ),
            clean_text(
                row.get(
                    "market"
                )
            ),
            clean_text(
                row.get(
                    "gap_start"
                )
            ),
        )
    )

    return result


# ============================================================
# 27. PLAN JSON
# ============================================================


def build_plan_json(
    *,
    fingerprint: Dict[str, Any],
    inventory: Dict[str, Any],
    candidates: Sequence[Candidate],
    detail_rows: Sequence[
        Dict[str, Any]
    ],
    target_rows: Sequence[
        Dict[str, Any]
    ],
) -> Dict[str, Any]:

    counts = Counter(
        clean_text(
            row.get(
                "classification"
            )
        )
        for row
        in detail_rows
    )

    missing_total = sum(
        safe_int(
            row.get(
                "current_missing_count"
            )
        )
        for row
        in detail_rows
        if (
            row.get(
                "classification"
            )
            == READY_SECOND_PASS
        )
    )

    return {
        "program": PROGRAM,
        "version": VERSION,
        "execution_mode": (
            EXECUTION_MODE
        ),
        "created_utc": (
            utc_now_iso()
        ),

        "source": {
            "retry_candidate_file":
                str(
                    RETRY_CANDIDATE_FILE
                ),

            "retry_candidate_sha256":
                sha256_file(
                    RETRY_CANDIDATE_FILE
                ),

            "audit_summary_file":
                str(
                    AUDIT_SUMMARY_FILE
                ),

            "audit_detail_file":
                str(
                    AUDIT_DETAIL_FILE
                ),

            "audit_result_file":
                str(
                    AUDIT_RESULT_FILE
                ),

            "source_fingerprint":
                fingerprint[
                    "source_fingerprint"
                ],

            "ohlcv_fingerprint":
                fingerprint[
                    "ohlcv_fingerprint"
                ],
        },

        "ohlcv_inventory": {
            "h1":
                inventory["h1"],

            "h4":
                inventory["h4"],

            "d1":
                inventory["d1"],

            "total":
                inventory["total"],

            "markets":
                inventory["markets"],
        },

        "summary": {
            "retry_candidates":
                len(candidates),

            "ready_second_pass":
                counts[
                    READY_SECOND_PASS
                ],

            "no_longer_gap":
                counts[
                    NO_LONGER_GAP
                ],

            "structural_gap":
                counts[
                    STRUCTURAL_GAP
                ],

            "outside_api_range":
                counts[
                    OUTSIDE_API_RANGE
                ],

            "invalid_source":
                counts[
                    INVALID_SOURCE
                ],

            "manual_review":
                counts[
                    MANUAL_REVIEW
                ],

            "second_pass_targets":
                len(target_rows),

            "second_pass_missing_estimate":
                missing_total,
        },

        "classifications": list(
            CLASSIFICATIONS
        ),

        "next_stage": {
            "allowed_input":
                str(
                    SECOND_PASS_TARGET_FILE
                ),

            "allowed_classification":
                READY_SECOND_PASS,

            "automatic_repair_executed":
                False,
        },

        "safety": SAFETY,
    }


# ============================================================
# 28. VERIFY OUTPUTS
# ============================================================


def verify_generated_outputs(
    *,
    expected_targets: int,
) -> None:

    if not outputs_are_valid():

        raise RuntimeError(
            "Generated planner outputs failed validation."
        )

    _, targets = read_csv(
        SECOND_PASS_TARGET_FILE
    )

    if (
        len(targets)
        != expected_targets
    ):

        raise RuntimeError(
            "Second-pass target count mismatch. "
            f"expected={expected_targets} "
            f"found={len(targets)}"
        )

    for row in targets:

        if (
            clean_text(
                row.get(
                    "classification"
                )
            )
            != READY_SECOND_PASS
        ):

            raise RuntimeError(
                "Second-pass target file contains "
                "a non-READY_SECOND_PASS row."
            )


# ============================================================
# 29. MAIN PLANNER
# ============================================================


def run_planner() -> int:

    print(
        "=" * 60
    )

    print(
        "UPBIT SURGE MONITOR"
    )

    print(
        "HISTORY GAP RETRY PLANNER"
    )

    print(
        VERSION
    )

    print(
        "=" * 60
    )

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

    # --------------------------------------------------------
    # Prepare
    # --------------------------------------------------------

    prepare_output_directories()

    # --------------------------------------------------------
    # Inventory
    # --------------------------------------------------------

    print(
        "=" * 60
    )

    print(
        "OHLCV INVENTORY"
    )

    print(
        "=" * 60
    )

    inventory = inventory_ohlcv()

    print(
        f"H1 files : {inventory['h1']}"
    )

    print(
        f"H4 files : {inventory['h4']}"
    )

    print(
        f"D1 files : {inventory['d1']}"
    )

    print(
        f"Total    : {inventory['total']}"
    )

    print(
        f"Markets  : {inventory['markets']}"
    )

    print()

    # --------------------------------------------------------
    # Audit inputs
    # --------------------------------------------------------

    print(
        "=" * 60
    )

    print(
        "VERIFY RECOVERY AUDIT INPUTS"
    )

    print(
        "=" * 60
    )

    audit_result = (
        validate_audit_inputs()
    )

    print(
        "[PASS] Retry candidate file"
    )

    print(
        f"       {RETRY_CANDIDATE_FILE}"
    )

    print(
        "[PASS] Audit summary"
    )

    print(
        "[PASS] Audit detail"
    )

    print(
        "[PASS] Audit result JSON"
    )

    audit_program = clean_text(
        audit_result.get(
            "program"
        )
    )

    audit_version = clean_text(
        audit_result.get(
            "version"
        )
    )

    print(
        f"Audit program : {audit_program}"
    )

    print(
        f"Audit version : {audit_version}"
    )

    print()

    # --------------------------------------------------------
    # Load candidates
    # --------------------------------------------------------

    print(
        "=" * 60
    )

    print(
        "LOAD RETRY CANDIDATES"
    )

    print(
        "=" * 60
    )

    candidates, source_fields = (
        load_retry_candidates()
    )

    print(
        f"Candidate rows : {len(candidates)}"
    )

    print(
        f"Source columns : {len(source_fields)}"
    )

    print()

    if not candidates:

        print(
            "[INFO] Retry candidate file contains no rows."
        )

    # --------------------------------------------------------
    # Fingerprint
    # --------------------------------------------------------

    print(
        "=" * 60
    )

    print(
        "BUILD IMMUTABLE INPUT FINGERPRINT"
    )

    print(
        "=" * 60
    )

    fingerprint = (
        build_input_fingerprint()
    )

    print(
        "Source fingerprint:"
    )

    print(
        f"  {fingerprint['source_fingerprint']}"
    )

    print(
        "OHLCV fingerprint:"
    )

    print(
        f"  {fingerprint['ohlcv_fingerprint']}"
    )

    print(
        "OHLCV fingerprint files:"
    )

    print(
        f"  {fingerprint['ohlcv_file_count']}"
    )

    print()

    # --------------------------------------------------------
    # Resume
    # --------------------------------------------------------

    checkpoint = load_checkpoint()

    if checkpoint_is_reusable(
        checkpoint,
        fingerprint,
    ):

        print(
            "=" * 60
        )

        print(
            "RESUME / CHECKPOINT"
        )

        print(
            "=" * 60
        )

        print(
            "[SKIP] Previous completed planner result is valid."
        )

        print(
            "[SKIP] Input fingerprints are unchanged."
        )

        print(
            "[SKIP] Output integrity is valid."
        )

        print()

        print(
            f"Second-pass targets : "
            f"{checkpoint.get('second_pass_target_count', 0)}"
        )

        print()

        print(
            "[PASS] No OHLCV modification executed."
        )

        return 0

    # --------------------------------------------------------
    # Analyze
    # --------------------------------------------------------

    print(
        "=" * 60
    )

    print(
        "ANALYZE RETRY CANDIDATES"
    )

    print(
        "=" * 60
    )

    detail_rows: List[
        Dict[str, Any]
    ] = []

    total = len(
        candidates
    )

    for index, candidate in enumerate(
        candidates,
        start=1,
    ):

        row = analyze_candidate(
            candidate
        )

        row[
            "candidate_index"
        ] = index

        row[
            "candidate_total"
        ] = total

        detail_rows.append(
            row
        )

        classification = clean_text(
            row.get(
                "classification"
            )
        )

        current_missing = safe_int(
            row.get(
                "current_missing_count"
            )
        )

        print(
            f"[PLAN] [{index}/{total}] "
            f"{candidate.timeframe.upper()} "
            f"{candidate.market} "
            f"{classification} "
            f"missing={current_missing}"
        )

        append_status(
            {
                "utc_time":
                    utc_now_iso(),

                "version":
                    VERSION,

                "mode":
                    EXECUTION_MODE,

                "candidate_index":
                    index,

                "candidate_total":
                    total,

                "timeframe":
                    candidate.timeframe,

                "market":
                    candidate.market,

                "classification":
                    classification,

                "current_missing_count":
                    current_missing,

                "status":
                    "PLANNED",

                "message":
                    row.get(
                        "reason",
                        "",
                    ),
            }
        )

    print()

    # --------------------------------------------------------
    # Build outputs
    # --------------------------------------------------------

    target_rows = (
        build_target_rows(
            detail_rows
        )
    )

    summary_rows = (
        build_summary_rows(
            detail_rows
        )
    )

    counts = Counter(
        clean_text(
            row.get(
                "classification"
            )
        )
        for row
        in detail_rows
    )

    # --------------------------------------------------------
    # Detail
    # --------------------------------------------------------

    atomic_csv_write(
        PLAN_DETAIL_FILE,
        DETAIL_FIELDS,
        detail_rows,
    )

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    atomic_csv_write(
        PLAN_SUMMARY_FILE,
        SUMMARY_FIELDS,
        summary_rows,
    )

    # --------------------------------------------------------
    # Second-pass target
    # --------------------------------------------------------

    atomic_csv_write(
        SECOND_PASS_TARGET_FILE,
        TARGET_FIELDS,
        target_rows,
    )

    # --------------------------------------------------------
    # Plan JSON
    # --------------------------------------------------------

    plan_payload = (
        build_plan_json(
            fingerprint=fingerprint,
            inventory=inventory,
            candidates=candidates,
            detail_rows=detail_rows,
            target_rows=target_rows,
        )
    )

    atomic_json_write(
        SECOND_PASS_PLAN_FILE,
        plan_payload,
    )

    # --------------------------------------------------------
    # Verify
    # --------------------------------------------------------

    verify_generated_outputs(
        expected_targets=len(
            target_rows
        )
    )

    # --------------------------------------------------------
    # Checkpoint LAST
    #
    # A completed checkpoint is written only after every
    # required output passes validation.
    # --------------------------------------------------------

    save_checkpoint(
        fingerprint=fingerprint,
        candidate_count=len(
            candidates
        ),
        classification_counts={
            classification:
            counts[classification]
            for classification
            in CLASSIFICATIONS
        },
        target_count=len(
            target_rows
        ),
    )

    # --------------------------------------------------------
    # Final report
    # --------------------------------------------------------

    print(
        "=" * 60
    )

    print(
        "HISTORY GAP RETRY PLAN RESULT"
    )

    print(
        "=" * 60
    )

    print(
        f"Input candidates      : {len(candidates)}"
    )

    print(
        f"READY_SECOND_PASS     : "
        f"{counts[READY_SECOND_PASS]}"
    )

    print(
        f"NO_LONGER_GAP         : "
        f"{counts[NO_LONGER_GAP]}"
    )

    print(
        f"STRUCTURAL_GAP        : "
        f"{counts[STRUCTURAL_GAP]}"
    )

    print(
        f"OUTSIDE_API_RANGE     : "
        f"{counts[OUTSIDE_API_RANGE]}"
    )

    print(
        f"INVALID_SOURCE        : "
        f"{counts[INVALID_SOURCE]}"
    )

    print(
        f"MANUAL_REVIEW         : "
        f"{counts[MANUAL_REVIEW]}"
    )

    print()

    print(
        f"Second-pass targets   : {len(target_rows)}"
    )

    print()

    print(
        "Generated:"
    )

    print(
        f"  {SECOND_PASS_TARGET_FILE}"
    )

    print(
        f"  {SECOND_PASS_PLAN_FILE}"
    )

    print(
        f"  {PLAN_SUMMARY_FILE}"
    )

    print(
        f"  {PLAN_DETAIL_FILE}"
    )

    print(
        f"  {CHECKPOINT_FILE}"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  OHLCV write       : FORBIDDEN"
    )

    print(
        "  OHLCV delete      : FORBIDDEN"
    )

    print(
        "  API download      : FORBIDDEN"
    )

    print(
        "  Gap repair        : FORBIDDEN"
    )

    print(
        "  Feature build     : DISABLED"
    )

    print(
        "  256 Detector      : DISABLED"
    )

    print(
        "  Future labels     : DISABLED"
    )

    print(
        "  Prediction        : DISABLED"
    )

    print(
        "  Trading           : DISABLED"
    )

    print(
        "  Git reset         : DISABLED"
    )

    print(
        "  Git clean         : DISABLED"
    )

    print(
        "  Git commit        : DISABLED"
    )

    print(
        "  Git push          : DISABLED"
    )

    print()

    print(
        "[PASS] Second-pass recovery plan completed."
    )

    print(
        "[NEXT] Validate plan with "
        "plan_history_gap_retry_test.yml."
    )

    print(
        "=" * 60
    )

    return 0


# ============================================================
# 30. MAIN
# ============================================================


def main() -> int:

    try:

        return run_planner()

    except KeyboardInterrupt:

        print()

        print(
            "=" * 60
        )

        print(
            "HISTORY GAP RETRY PLANNER INTERRUPTED"
        )

        print(
            "=" * 60
        )

        print(
            "[SAFE] No OHLCV repair was executed."
        )

        print(
            "[SAFE] No API download was executed."
        )

        print(
            "[SAFE] Existing OHLCV remains untouched."
        )

        return 130

    except Exception as exc:

        print()

        print(
            "=" * 60
        )

        print(
            "HISTORY GAP RETRY PLANNER FATAL ERROR"
        )

        print(
            "=" * 60
        )

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
            "[SAFETY] No OHLCV write was executed."
        )

        print(
            "[SAFETY] No historical gap repair was executed."
        )

        print(
            "[SAFETY] No API candle download was executed."
        )

        print(
            "[SAFETY] No feature build was executed."
        )

        print(
            "[SAFETY] No detector was executed."
        )

        print(
            "[SAFETY] No prediction/trading was executed."
        )

        print(
            "[SAFETY] No Git reset/clean/commit/push was executed."
        )

        return 1


# ============================================================
# 31. ENTRY POINT
# ============================================================


if __name__ == "__main__":

    sys.exit(
        main()
    )


# ============================================================
# END
# ============================================================
