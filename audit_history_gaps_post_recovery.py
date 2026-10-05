# ============================================================
# Upbit Surge Monitor
# audit_history_gaps_post_recovery.py
# Clean V001
# ============================================================
#
# PURPOSE
#
#   Perform a strict READ-ONLY post-recovery audit after:
#
#       plan_history_gap_retry.py
#       recover_history_gaps_second_pass.py
#
#   This program independently re-reads the CURRENT production
#   OHLCV files and verifies the actual post-recovery state.
#
#
# PRIMARY GOALS
#
#   1. Verify production OHLCV inventory:
#
#          290 markets x 3 timeframes = 870 CSV files
#
#   2. Verify every second-pass recovery target against the
#      CURRENT OHLCV state.
#
#   3. Recalculate remaining missing timestamps independently.
#
#   4. Cross-check the recovery program result/report against
#      the actual OHLCV state.
#
#   5. Verify current OHLCV structural integrity:
#
#      - timestamp column exists
#      - timestamps are parseable
#      - no duplicate timestamps
#      - timestamps are aligned to the timeframe grid
#      - timestamps are strictly ordered after normalization
#      - OHLCV rows are readable
#
#   6. Verify recovery BEFORE / AFTER SHA256 evidence.
#
#   7. Produce an explicit gate for the next research stage.
#
#
# THIS PROGRAM DOES NOT:
#
#   - call the Upbit API
#   - download candles
#   - modify OHLCV
#   - repair OHLCV
#   - delete OHLCV
#   - overwrite OHLCV
#   - build features
#   - run 256 detector
#   - create future labels
#   - run prediction
#   - run trading
#   - execute Git reset / clean / commit / push
#
#
# EXECUTION MODE
#
#   READ_ONLY_POST_RECOVERY_AUDIT
#
#
# INPUT
#
#   data/recovery/
#
#       history_gap_second_pass_targets.csv
#       history_gap_second_pass_plan.json
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
#   data/ohlcv/
#
#       h1/
#       h4/
#       d1/
#
#
# OUTPUT
#
#   data/reports/history_gap_post_recovery_audit/
#
#       history_gap_post_recovery_audit_summary.csv
#       history_gap_post_recovery_audit_detail.csv
#       history_gap_post_recovery_audit_inventory.csv
#       history_gap_post_recovery_audit_result.json
#
#   data/validation/
#
#       audit_history_gaps_post_recovery_checkpoint.json
#
#   data/
#
#       audit_history_gaps_post_recovery_status.csv
#
#
# FINAL AUDIT STATUS
#
#   PASS
#
#       All second-pass target timestamps are now present and
#       no critical production integrity problem was found.
#
#   PASS_WITH_UNAVAILABLE_HISTORY
#
#       Production integrity is valid, but one or more target
#       timestamps remain missing after second-pass recovery.
#       These are NOT automatically declared recoverable or
#       unrecoverable by this audit.
#
#   FAIL
#
#       A critical integrity, contract, evidence, inventory,
#       duplicate, alignment, or cross-validation problem exists.
#
#
# IMPORTANT
#
#   Remaining historical gaps are evidence only.
#
#   This program NEVER guesses that a remaining gap is outside
#   the Upbit API range merely because it is old.
#
#   No remaining gap is silently repaired.
#
# ============================================================


from __future__ import annotations


import csv
import hashlib
import json
import os
import sys
import traceback

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)


# ============================================================
# 1. PROGRAM
# ============================================================


PROGRAM = "audit_history_gaps_post_recovery.py"

VERSION = "Clean V001"

EXECUTION_MODE = "READ_ONLY_POST_RECOVERY_AUDIT"


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
# 3. ROOT PATHS
# ============================================================


SCRIPT_ROOT = Path(__file__).resolve().parent

def _resolve_project_root() -> Path:
    """Resolve the real production project root, never silently auditing an empty Actions workspace."""
    env_names = (
        "UPBIT_SURGE_PROJECT_ROOT",
        "UPBIT_PROJECT_ROOT",
        "PRODUCTION_PROJECT_ROOT",
        "PRODUCTION_ROOT",
    )
    candidates: List[Tuple[str, Path]] = []
    for name in env_names:
        # IMPORTANT: root resolution runs during module import, before
        # clean_text() is defined later in this file. Keep this bootstrap
        # logic dependency-free.
        raw_value = os.environ.get(name, "")
        raw = str(raw_value).strip() if raw_value is not None else ""
        if raw:
            candidates.append((f"env:{name}", Path(raw).expanduser()))

    # Self-hosted Windows production location used by this project.
    candidates.append(("user-documents", Path.home() / "Documents" / "upbit-surge-monitor"))
    # Local/manual execution remains supported when the script itself is in production.
    candidates.append(("script-root", SCRIPT_ROOT))

    required_relatives = (
        Path("data") / "recovery" / "history_gap_second_pass_targets.csv",
        Path("data") / "recovery" / "history_gap_second_pass_plan.json",
        Path("data") / "reports" / "history_gap_second_pass_recovery" / "history_gap_second_pass_recovery_result.json",
        Path("data") / "validation" / "recover_history_gaps_second_pass_checkpoint.json",
        Path("data") / "ohlcv" / "h1",
        Path("data") / "ohlcv" / "h4",
        Path("data") / "ohlcv" / "d1",
    )

    checked: List[str] = []
    for source, candidate in candidates:
        candidate = candidate.resolve()
        missing = [str(rel) for rel in required_relatives if not (candidate / rel).exists()]
        checked.append(f"{source}={candidate} missing={len(missing)}")
        if not missing:
            print(f"[PATH] Production project root: {candidate}")
            print(f"[PATH] Root source: {source}")
            return candidate

    raise FileNotFoundError(
        "Unable to resolve production project root containing the required "
        "post-recovery evidence and OHLCV directories. Checked: "
        + " | ".join(checked)
    )

PROJECT_ROOT = _resolve_project_root()
DATA_DIR = PROJECT_ROOT / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"
RECOVERY_DIR = DATA_DIR / "recovery"
REPORTS_DIR = DATA_DIR / "reports"
VALIDATION_DIR = DATA_DIR / "validation"


RECOVERY_REPORT_DIR = (
    REPORTS_DIR
    / "history_gap_second_pass_recovery"
)


AUDIT_REPORT_DIR = (
    REPORTS_DIR
    / "history_gap_post_recovery_audit"
)


# ============================================================
# 4. INPUT FILES
# ============================================================


SECOND_PASS_TARGET_FILE = (
    RECOVERY_DIR
    / "history_gap_second_pass_targets.csv"
)


SECOND_PASS_PLAN_FILE = (
    RECOVERY_DIR
    / "history_gap_second_pass_plan.json"
)


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


RECOVERY_CHECKPOINT_FILE = (
    VALIDATION_DIR
    / "recover_history_gaps_second_pass_checkpoint.json"
)


# ============================================================
# 5. OUTPUT FILES
# ============================================================


AUDIT_SUMMARY_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_post_recovery_audit_summary.csv"
)


AUDIT_DETAIL_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_post_recovery_audit_detail.csv"
)


AUDIT_INVENTORY_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_post_recovery_audit_inventory.csv"
)


AUDIT_RESULT_FILE = (
    AUDIT_REPORT_DIR
    / "history_gap_post_recovery_audit_result.json"
)


AUDIT_CHECKPOINT_FILE = (
    VALIDATION_DIR
    / "audit_history_gaps_post_recovery_checkpoint.json"
)


STATUS_FILE = (
    DATA_DIR
    / "audit_history_gaps_post_recovery_status.csv"
)


# ============================================================
# 6. SAFETY
# ============================================================


SAFETY = {
    "ohlcv_write": False,
    "ohlcv_delete": False,
    "existing_candle_overwrite": False,
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
# 7. CONSTANTS
# ============================================================


READY_SECOND_PASS = "READY_SECOND_PASS"


RECOVERY_STATUS_SUCCESS = "SUCCESS"

RECOVERY_STATUS_PARTIAL = "PARTIAL"

RECOVERY_STATUS_ALREADY_RECOVERED = "ALREADY_RECOVERED"

RECOVERY_STATUS_NO_DATA = "NO_DATA"

RECOVERY_STATUS_FAILED = "FAILED"

RECOVERY_STATUS_SKIPPED = "SKIPPED"


AUDIT_RECOVERED = "RECOVERED"

AUDIT_REMAINING_GAP = "REMAINING_GAP"

AUDIT_INVALID_TARGET = "INVALID_TARGET"

AUDIT_OHLCV_ERROR = "OHLCV_ERROR"


FINAL_PASS = "PASS"

FINAL_PASS_WITH_UNAVAILABLE_HISTORY = (
    "PASS_WITH_UNAVAILABLE_HISTORY"
)

FINAL_FAIL = "FAIL"


# ============================================================
# 8. COLUMN ALIASES
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
# 9. OUTPUT SCHEMAS
# ============================================================


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
    "expected_timestamp_count",
    "current_missing_count",
    "current_present_count",
    "recovery_report_status",
    "recovery_report_missing_before",
    "recovery_report_inserted_candles",
    "recovery_report_missing_after",
    "ohlcv_file",
    "ohlcv_rows",
    "ohlcv_unique_timestamps",
    "ohlcv_first_utc",
    "ohlcv_last_utc",
    "audit_status",
    "reason",
    "event_key",
    "source_report",
    "source_row",
    "planner_version",
)


SUMMARY_FIELDS = (
    "timeframe",
    "targets",
    "recovered",
    "remaining_gap",
    "invalid_target",
    "ohlcv_error",
    "expected_missing_from_plan",
    "current_missing",
)


INVENTORY_FIELDS = (
    "timeframe",
    "market",
    "path",
    "rows",
    "valid_timestamps",
    "unique_timestamps",
    "duplicate_timestamps",
    "invalid_timestamps",
    "misaligned_timestamps",
    "first_utc",
    "last_utc",
    "sha256",
    "recovery_before_sha256",
    "recovery_after_sha256",
    "matches_recovery_after_sha256",
    "status",
    "message",
)


STATUS_FIELDS = (
    "utc_time",
    "version",
    "mode",
    "stage",
    "target_index",
    "target_total",
    "timeframe",
    "market",
    "status",
    "message",
)


# ============================================================
# 10. DATA CLASSES
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
# 11. BASIC UTILITIES
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

    result: List[str] = []

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


def read_json(
    path: Path,
) -> Dict[str, Any]:

    if not path.is_file():

        raise FileNotFoundError(
            f"JSON file not found: {path}"
        )

    if path.stat().st_size <= 0:

        raise RuntimeError(
            f"JSON file is empty: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:

        payload = json.load(
            handle
        )

    if not isinstance(
        payload,
        dict,
    ):

        raise RuntimeError(
            f"JSON root must be an object: {path}"
        )

    return payload


# ============================================================
# 12. DATETIME
# ============================================================


def parse_datetime(
    value: Any,
) -> Optional[datetime]:

    text = clean_text(value)

    if not text:
        return None

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


def datetime_iso(
    value: Optional[datetime],
) -> str:

    if value is None:
        return ""

    return (
        value.astimezone(
            timezone.utc
        )
        .replace(microsecond=0)
        .isoformat()
    )


# ============================================================
# 13. COLUMN DETECTION
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
# 14. OUTPUT DIRECTORY PREPARATION
# ============================================================


def prepare_output_directories() -> None:

    directories = (
        AUDIT_REPORT_DIR,
        VALIDATION_DIR,
    )

    for directory in directories:

        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


# ============================================================
# 15. STATUS
# ============================================================


def write_status(
    *,
    stage: str,
    status: str,
    message: str,
    target_index: int = 0,
    target_total: int = 0,
    timeframe: str = "",
    market: str = "",
) -> None:

    row = {
        "utc_time":
            utc_now_iso(),

        "version":
            VERSION,

        "mode":
            EXECUTION_MODE,

        "stage":
            stage,

        "target_index":
            target_index,

        "target_total":
            target_total,

        "timeframe":
            timeframe,

        "market":
            market,

        "status":
            status,

        "message":
            message,
    }

    atomic_csv_write(
        STATUS_FILE,
        STATUS_FIELDS,
        [row],
    )


# ============================================================
# 16. INPUT CONTRACT VALIDATION
# ============================================================


def validate_required_inputs() -> Tuple[
    Dict[str, Any],
    Dict[str, Any],
    Dict[str, Any],
]:

    required_files = (
        SECOND_PASS_TARGET_FILE,
        SECOND_PASS_PLAN_FILE,
        RECOVERY_SUMMARY_FILE,
        RECOVERY_DETAIL_FILE,
        RECOVERY_RESULT_FILE,
        BEFORE_SHA256_FILE,
        AFTER_SHA256_FILE,
        RECOVERY_CHECKPOINT_FILE,
    )

    for path in required_files:

        if not path.is_file():

            raise FileNotFoundError(
                f"Required post-recovery input missing: {path}"
            )

        if path.stat().st_size <= 0:

            raise RuntimeError(
                f"Required post-recovery input is empty: {path}"
            )

    plan = read_json(
        SECOND_PASS_PLAN_FILE
    )

    recovery_result = read_json(
        RECOVERY_RESULT_FILE
    )

    recovery_checkpoint = read_json(
        RECOVERY_CHECKPOINT_FILE
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
            "history_gap_second_pass_plan.json: "
            f"{plan.get('program')}"
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

    if not isinstance(
        next_stage,
        dict,
    ):

        raise RuntimeError(
            "Planner next_stage must be an object."
        )

    allowed_classification = clean_text(
        next_stage.get(
            "allowed_classification"
        )
    )

    if (
        allowed_classification
        and
        allowed_classification
        != READY_SECOND_PASS
    ):

        raise RuntimeError(
            "Planner allowed classification mismatch. "
            f"expected={READY_SECOND_PASS} "
            f"found={allowed_classification}"
        )

    recovery_program = clean_text(
        recovery_result.get(
            "program"
        )
    )

    if (
        recovery_program
        and
        recovery_program
        != "recover_history_gaps_second_pass.py"
    ):

        raise RuntimeError(
            "Unexpected recovery result program: "
            f"{recovery_program}"
        )

    recovery_version = clean_text(
        recovery_result.get(
            "version"
        )
    )

    if (
        recovery_version
        and
        recovery_version
        != "Clean V001"
    ):

        raise RuntimeError(
            "Unexpected recovery result version: "
            f"{recovery_version}"
        )

    checkpoint_completed = (
        recovery_checkpoint.get(
            "completed"
        )
    )

    checkpoint_status = clean_text(
        recovery_checkpoint.get(
            "status"
        )
    )

    if (
        checkpoint_completed is not True
        or checkpoint_status != "COMPLETED"
    ):

        raise RuntimeError(
            "Second-pass recovery checkpoint is not "
            "COMPLETED."
        )

    return (
        plan,
        recovery_result,
        recovery_checkpoint,
    )


# ============================================================
# 17. LOAD SECOND-PASS TARGETS
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
# 18. OHLCV FILE DISCOVERY
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
# 19. HASH REPORT LOADING
# ============================================================


def load_hash_report(
    path: Path,
) -> Dict[
    Tuple[str, str],
    Dict[str, str],
]:

    fieldnames, rows = read_csv(
        path
    )

    timeframe_column = detect_column(
        fieldnames,
        TIMEFRAME_ALIASES,
    )

    market_column = detect_column(
        fieldnames,
        MARKET_ALIASES,
    )

    hash_column = detect_column(
        fieldnames,
        (
            "sha256",
            "hash",
            "file_sha256",
        ),
    )

    if (
        timeframe_column is None
        or market_column is None
        or hash_column is None
    ):

        raise RuntimeError(
            f"Invalid SHA256 report schema: {path}"
        )

    result: Dict[
        Tuple[str, str],
        Dict[str, str],
    ] = {}

    for row in rows:

        timeframe = normalize_timeframe(
            row.get(
                timeframe_column,
                "",
            )
        )

        market = normalize_market(
            row.get(
                market_column,
                "",
            )
        )

        if (
            timeframe not in TIMEFRAMES
            or not market
        ):

            raise RuntimeError(
                f"Invalid SHA256 report row: {path}"
            )

        key = (
            timeframe,
            market,
        )

        if key in result:

            raise RuntimeError(
                "Duplicate SHA256 report key: "
                f"{timeframe}/{market}"
            )

        result[key] = row

    return result


# ============================================================
# 20. RECOVERY DETAIL LOADING
# ============================================================


def load_recovery_detail() -> Dict[
    str,
    Dict[str, str],
]:

    fieldnames, rows = read_csv(
        RECOVERY_DETAIL_FILE
    )

    event_column = detect_column(
        fieldnames,
        EVENT_KEY_ALIASES,
    )

    if event_column is None:

        raise RuntimeError(
            "Recovery detail report has no event_key column."
        )

    result: Dict[
        str,
        Dict[str, str],
    ] = {}

    for row in rows:

        event_key = clean_text(
            row.get(
                event_column,
                "",
            )
        )

        if not event_key:

            raise RuntimeError(
                "Recovery detail contains empty event_key."
            )

        if event_key in result:

            raise RuntimeError(
                "Duplicate event_key in recovery detail: "
                f"{event_key}"
            )

        result[event_key] = row

    return result


# ============================================================
# 21. OHLCV TIMESTAMP ANALYSIS
# ============================================================


def analyze_ohlcv_file(
    path: Path,
    timeframe: str,
) -> Dict[str, Any]:

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

    parsed_timestamps: List[
        datetime
    ] = []

    invalid_timestamps = 0

    for row in rows:

        raw_value = row.get(
            timestamp_column,
            "",
        )

        parsed = parse_datetime(
            raw_value
        )

        if parsed is None:

            invalid_timestamps += 1

            continue

        parsed_timestamps.append(
            parsed
        )

    timestamp_keys = [
        datetime_key(value)
        for value
        in parsed_timestamps
    ]

    unique_keys = set(
        timestamp_keys
    )

    duplicate_timestamps = (
        len(timestamp_keys)
        - len(unique_keys)
    )

    step_seconds = TIMEFRAME_SECONDS[
        timeframe
    ]

    misaligned_timestamps = 0

    for key in unique_keys:

        if (
            key
            % step_seconds
            != 0
        ):

            misaligned_timestamps += 1

    sorted_unique = sorted(
        unique_keys
    )

    first_dt: Optional[datetime] = None

    last_dt: Optional[datetime] = None

    if sorted_unique:

        first_dt = datetime.fromtimestamp(
            sorted_unique[0],
            tz=timezone.utc,
        )

        last_dt = datetime.fromtimestamp(
            sorted_unique[-1],
            tz=timezone.utc,
        )

    status = "PASS"

    messages: List[str] = []

    if invalid_timestamps > 0:

        status = "FAIL"

        messages.append(
            f"invalid_timestamps={invalid_timestamps}"
        )

    if duplicate_timestamps > 0:

        status = "FAIL"

        messages.append(
            f"duplicate_timestamps={duplicate_timestamps}"
        )

    if misaligned_timestamps > 0:

        status = "FAIL"

        messages.append(
            f"misaligned_timestamps={misaligned_timestamps}"
        )

    if not sorted_unique:

        status = "FAIL"

        messages.append(
            "no_valid_timestamps"
        )

    if not messages:

        messages.append(
            "OHLCV timestamp integrity valid."
        )

    return {
        "rows":
            len(rows),

        "valid_timestamps":
            len(parsed_timestamps),

        "unique_timestamps":
            len(unique_keys),

        "duplicate_timestamps":
            duplicate_timestamps,

        "invalid_timestamps":
            invalid_timestamps,

        "misaligned_timestamps":
            misaligned_timestamps,

        "first_utc":
            datetime_iso(
                first_dt
            ),

        "last_utc":
            datetime_iso(
                last_dt
            ),

        "timestamp_keys":
            unique_keys,

        "sha256":
            sha256_file(
                path
            ),

        "status":
            status,

        "message":
            "; ".join(
                messages
            ),
    }


# ============================================================
# 22. INVENTORY AUDIT
# ============================================================


def audit_full_inventory(
    before_hashes: Dict[
        Tuple[str, str],
        Dict[str, str],
    ],
    after_hashes: Dict[
        Tuple[str, str],
        Dict[str, str],
    ],
) -> Tuple[
    Dict[str, Any],
    List[Dict[str, Any]],
    Dict[
        Tuple[str, str],
        Dict[str, Any],
    ],
]:

    counts: Dict[str, int] = {
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

    inventory_rows: List[
        Dict[str, Any]
    ] = []

    analyses: Dict[
        Tuple[str, str],
        Dict[str, Any],
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
            directory.glob(
                "*.csv"
            )
        )

        counts[timeframe] = len(
            files
        )

        counts["total"] += len(
            files
        )

        market_sets[timeframe] = {
            path.stem.upper()
            for path
            in files
        }

        if (
            len(files)
            != EXPECTED_MARKETS
        ):

            raise RuntimeError(
                f"{timeframe.upper()} inventory mismatch. "
                f"expected={EXPECTED_MARKETS} "
                f"found={len(files)}"
            )

        for index, path in enumerate(
            files,
            start=1,
        ):

            market = path.stem.upper()

            analysis = analyze_ohlcv_file(
                path,
                timeframe,
            )

            key = (
                timeframe,
                market,
            )

            analyses[key] = analysis

            before_row = before_hashes.get(
                key,
                {},
            )

            after_row = after_hashes.get(
                key,
                {},
            )

            before_sha = clean_text(
                before_row.get(
                    "sha256",
                    "",
                )
            )

            recovery_after_sha = clean_text(
                after_row.get(
                    "sha256",
                    "",
                )
            )

            current_sha = clean_text(
                analysis.get(
                    "sha256",
                    "",
                )
            )

            hash_match = (
                bool(recovery_after_sha)
                and
                current_sha
                == recovery_after_sha
            )

            status = clean_text(
                analysis.get(
                    "status"
                )
            )

            message = clean_text(
                analysis.get(
                    "message"
                )
            )

            if not recovery_after_sha:

                status = "FAIL"

                message = (
                    message
                    + "; missing recovery AFTER SHA256 evidence"
                ).strip("; ")

            elif not hash_match:

                status = "FAIL"

                message = (
                    message
                    + "; current SHA256 differs from recovery "
                    "AFTER SHA256"
                ).strip("; ")

            inventory_rows.append(
                {
                    "timeframe":
                        timeframe,

                    "market":
                        market,

                    "path":
                        str(path),

                    "rows":
                        analysis[
                            "rows"
                        ],

                    "valid_timestamps":
                        analysis[
                            "valid_timestamps"
                        ],

                    "unique_timestamps":
                        analysis[
                            "unique_timestamps"
                        ],

                    "duplicate_timestamps":
                        analysis[
                            "duplicate_timestamps"
                        ],

                    "invalid_timestamps":
                        analysis[
                            "invalid_timestamps"
                        ],

                    "misaligned_timestamps":
                        analysis[
                            "misaligned_timestamps"
                        ],

                    "first_utc":
                        analysis[
                            "first_utc"
                        ],

                    "last_utc":
                        analysis[
                            "last_utc"
                        ],

                    "sha256":
                        current_sha,

                    "recovery_before_sha256":
                        before_sha,

                    "recovery_after_sha256":
                        recovery_after_sha,

                    "matches_recovery_after_sha256":
                        hash_match,

                    "status":
                        status,

                    "message":
                        message,
                }
            )

            if (
                index % 25 == 0
                or index == len(files)
            ):

                print(
                    f"  {timeframe.upper()} "
                    f"{index}/{len(files)}"
                )

    if (
        counts["total"]
        != EXPECTED_OHLCV_FILES
    ):

        raise RuntimeError(
            "Total OHLCV inventory mismatch. "
            f"expected={EXPECTED_OHLCV_FILES} "
            f"found={counts['total']}"
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

    counts["markets"] = len(
        reference
    )

    if (
        counts["markets"]
        != EXPECTED_MARKETS
    ):

        raise RuntimeError(
            "Market inventory mismatch. "
            f"expected={EXPECTED_MARKETS} "
            f"found={counts['markets']}"
        )

    return (
        counts,
        inventory_rows,
        analyses,
    )


# ============================================================
# 23. EXPECTED TARGET TIMESTAMPS
# ============================================================


def build_expected_timestamp_keys(
    target: RecoveryTarget,
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
            "Invalid target datetime: "
            f"{target.event_key}"
        )

    if end_dt < start_dt:

        raise RuntimeError(
            "Target gap_end earlier than gap_start: "
            f"{target.event_key}"
        )

    step_seconds = TIMEFRAME_SECONDS[
        target.timeframe
    ]

    start_key = datetime_key(
        start_dt
    )

    end_key = datetime_key(
        end_dt
    )

    if (
        start_key
        % step_seconds
        != 0
    ):

        raise RuntimeError(
            "Target gap_start is not aligned to timeframe: "
            f"{target.event_key}"
        )

    if (
        end_key
        % step_seconds
        != 0
    ):

        raise RuntimeError(
            "Target gap_end is not aligned to timeframe: "
            f"{target.event_key}"
        )

    result: List[int] = []

    current = start_key

    while current <= end_key:

        result.append(
            current
        )

        current += step_seconds

    return result


# ============================================================
# 24. RECOVERY DETAIL HELPERS
# ============================================================


def recovery_value(
    row: Dict[str, str],
    name: str,
) -> str:

    return clean_text(
        row.get(
            name,
            "",
        )
    )


# ============================================================
# 25. TARGET POST-RECOVERY AUDIT
# ============================================================


def audit_targets(
    targets: Sequence[RecoveryTarget],
    analyses: Dict[
        Tuple[str, str],
        Dict[str, Any],
    ],
    recovery_detail: Dict[
        str,
        Dict[str, str],
    ],
) -> List[Dict[str, Any]]:

    detail_rows: List[
        Dict[str, Any]
    ] = []

    total = len(
        targets
    )

    for index, target in enumerate(
        targets,
        start=1,
    ):

        print(
            f"[{index}/{total}] "
            f"{target.timeframe.upper()} "
            f"{target.market}"
        )

        key = (
            target.timeframe,
            target.market,
        )

        analysis = analyses.get(
            key
        )

        recovery_row = recovery_detail.get(
            target.event_key,
            {},
        )

        audit_status = AUDIT_RECOVERED

        reasons: List[str] = []

        expected_keys: List[int] = []

        current_missing: List[int] = []

        current_present = 0

        if analysis is None:

            audit_status = AUDIT_OHLCV_ERROR

            reasons.append(
                "OHLCV analysis missing for target."
            )

        else:

            if (
                clean_text(
                    analysis.get(
                        "status"
                    )
                )
                != "PASS"
            ):

                audit_status = AUDIT_OHLCV_ERROR

                reasons.append(
                    "Target OHLCV failed timestamp integrity."
                )

            try:

                expected_keys = (
                    build_expected_timestamp_keys(
                        target
                    )
                )

            except Exception as exc:

                audit_status = AUDIT_INVALID_TARGET

                reasons.append(
                    f"Invalid target range: {exc}"
                )

            if expected_keys:

                timestamp_keys = analysis[
                    "timestamp_keys"
                ]

                current_missing = [
                    timestamp
                    for timestamp
                    in expected_keys
                    if timestamp
                    not in timestamp_keys
                ]

                current_present = (
                    len(expected_keys)
                    - len(current_missing)
                )

                if (
                    current_missing
                    and
                    audit_status
                    == AUDIT_RECOVERED
                ):

                    audit_status = (
                        AUDIT_REMAINING_GAP
                    )

                    reasons.append(
                        "One or more target timestamps remain "
                        "missing in current production OHLCV."
                    )

                elif (
                    not current_missing
                    and
                    audit_status
                    == AUDIT_RECOVERED
                ):

                    reasons.append(
                        "All target timestamps exist in current "
                        "production OHLCV."
                    )

        recovery_status = recovery_value(
            recovery_row,
            "status",
        )

        recovery_missing_before = safe_int(
            recovery_value(
                recovery_row,
                "missing_before",
            )
        )

        recovery_inserted = safe_int(
            recovery_value(
                recovery_row,
                "inserted_candles",
            )
        )

        recovery_missing_after = safe_int(
            recovery_value(
                recovery_row,
                "missing_after",
            )
        )

        if not recovery_row:

            if (
                audit_status
                not in {
                    AUDIT_INVALID_TARGET,
                    AUDIT_OHLCV_ERROR,
                }
            ):

                audit_status = AUDIT_OHLCV_ERROR

            reasons.append(
                "Matching event_key missing from recovery "
                "detail report."
            )

        else:

            if (
                recovery_missing_after
                != len(current_missing)
            ):

                if (
                    audit_status
                    not in {
                        AUDIT_INVALID_TARGET,
                        AUDIT_OHLCV_ERROR,
                    }
                ):

                    audit_status = AUDIT_OHLCV_ERROR

                reasons.append(
                    "Recovery report missing_after does not "
                    "match current OHLCV revalidation. "
                    f"report={recovery_missing_after} "
                    f"actual={len(current_missing)}"
                )

            if (
                recovery_status
                in {
                    RECOVERY_STATUS_SUCCESS,
                    RECOVERY_STATUS_ALREADY_RECOVERED,
                }
                and
                len(current_missing) > 0
            ):

                if (
                    audit_status
                    not in {
                        AUDIT_INVALID_TARGET,
                        AUDIT_OHLCV_ERROR,
                    }
                ):

                    audit_status = AUDIT_OHLCV_ERROR

                reasons.append(
                    "Recovery status claims completed target "
                    "but current OHLCV still has missing "
                    "timestamps."
                )

        if (
            target.expected_missing > 0
            and
            len(expected_keys) > 0
            and
            target.expected_missing
            > len(expected_keys)
        ):

            audit_status = AUDIT_INVALID_TARGET

            reasons.append(
                "Planner expected_missing exceeds target "
                "timestamp range size."
            )

        ohlcv_file = find_ohlcv_file(
            target.timeframe,
            target.market,
        )

        detail_rows.append(
            {
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

                "expected_timestamp_count":
                    len(
                        expected_keys
                    ),

                "current_missing_count":
                    len(
                        current_missing
                    ),

                "current_present_count":
                    current_present,

                "recovery_report_status":
                    recovery_status,

                "recovery_report_missing_before":
                    recovery_missing_before,

                "recovery_report_inserted_candles":
                    recovery_inserted,

                "recovery_report_missing_after":
                    recovery_missing_after,

                "ohlcv_file":
                    (
                        str(ohlcv_file)
                        if ohlcv_file
                        else ""
                    ),

                "ohlcv_rows":
                    (
                        analysis.get(
                            "rows",
                            "",
                        )
                        if analysis
                        else ""
                    ),

                "ohlcv_unique_timestamps":
                    (
                        analysis.get(
                            "unique_timestamps",
                            "",
                        )
                        if analysis
                        else ""
                    ),

                "ohlcv_first_utc":
                    (
                        analysis.get(
                            "first_utc",
                            "",
                        )
                        if analysis
                        else ""
                    ),

                "ohlcv_last_utc":
                    (
                        analysis.get(
                            "last_utc",
                            "",
                        )
                        if analysis
                        else ""
                    ),

                "audit_status":
                    audit_status,

                "reason":
                    "; ".join(
                        reasons
                    ),

                "event_key":
                    target.event_key,

                "source_report":
                    target.source_report,

                "source_row":
                    target.source_row,

                "planner_version":
                    target.planner_version,
            }
        )

        write_status(
            stage="TARGET_AUDIT",
            status=audit_status,
            message="; ".join(
                reasons
            ),
            target_index=index,
            target_total=total,
            timeframe=target.timeframe,
            market=target.market,
        )

    return detail_rows


# ============================================================
# 26. VERIFY TARGET / RECOVERY EVENT SET
# ============================================================


def verify_event_contract(
    targets: Sequence[RecoveryTarget],
    recovery_detail: Dict[
        str,
        Dict[str, str],
    ],
) -> Dict[str, Any]:

    target_keys = {
        target.event_key
        for target
        in targets
    }

    recovery_keys = set(
        recovery_detail.keys()
    )

    missing_from_recovery = sorted(
        target_keys
        - recovery_keys
    )

    unexpected_recovery = sorted(
        recovery_keys
        - target_keys
    )

    return {
        "target_event_count":
            len(target_keys),

        "recovery_event_count":
            len(recovery_keys),

        "missing_from_recovery_detail":
            missing_from_recovery,

        "unexpected_recovery_detail":
            unexpected_recovery,

        "event_sets_match":
            (
                not missing_from_recovery
                and
                not unexpected_recovery
            ),
    }


# ============================================================
# 27. VERIFY HASH EVIDENCE CONTRACT
# ============================================================


def verify_hash_contract(
    before_hashes: Dict[
        Tuple[str, str],
        Dict[str, str],
    ],
    after_hashes: Dict[
        Tuple[str, str],
        Dict[str, str],
    ],
) -> Dict[str, Any]:

    before_keys = set(
        before_hashes.keys()
    )

    after_keys = set(
        after_hashes.keys()
    )

    missing_after = sorted(
        before_keys
        - after_keys
    )

    unexpected_after = sorted(
        after_keys
        - before_keys
    )

    expected_keys = EXPECTED_OHLCV_FILES

    valid_count = (
        len(before_keys)
        == expected_keys
        and
        len(after_keys)
        == expected_keys
    )

    return {
        "before_hash_files":
            len(before_keys),

        "after_hash_files":
            len(after_keys),

        "expected_hash_files":
            expected_keys,

        "missing_after_keys":
            [
                f"{tf}|{market}"
                for tf, market
                in missing_after
            ],

        "unexpected_after_keys":
            [
                f"{tf}|{market}"
                for tf, market
                in unexpected_after
            ],

        "hash_key_sets_match":
            (
                not missing_after
                and
                not unexpected_after
            ),

        "hash_inventory_count_valid":
            valid_count,
    }


# ============================================================
# 28. SUMMARY
# ============================================================


def build_summary_rows(
    detail_rows: Sequence[
        Dict[str, Any]
    ],
) -> List[Dict[str, Any]]:

    buckets: Dict[
        str,
        Dict[str, int],
    ] = {}

    for timeframe in TIMEFRAMES:

        buckets[timeframe] = {
            "targets": 0,
            "recovered": 0,
            "remaining_gap": 0,
            "invalid_target": 0,
            "ohlcv_error": 0,
            "expected_missing_from_plan": 0,
            "current_missing": 0,
        }

    for row in detail_rows:

        timeframe = clean_text(
            row.get(
                "timeframe"
            )
        )

        if timeframe not in buckets:
            continue

        bucket = buckets[
            timeframe
        ]

        bucket["targets"] += 1

        bucket[
            "expected_missing_from_plan"
        ] += safe_int(
            row.get(
                "expected_missing_from_plan"
            )
        )

        bucket[
            "current_missing"
        ] += safe_int(
            row.get(
                "current_missing_count"
            )
        )

        status = clean_text(
            row.get(
                "audit_status"
            )
        )

        if status == AUDIT_RECOVERED:

            bucket["recovered"] += 1

        elif status == AUDIT_REMAINING_GAP:

            bucket["remaining_gap"] += 1

        elif status == AUDIT_INVALID_TARGET:

            bucket["invalid_target"] += 1

        elif status == AUDIT_OHLCV_ERROR:

            bucket["ohlcv_error"] += 1

    rows: List[
        Dict[str, Any]
    ] = []

    for timeframe in TIMEFRAMES:

        bucket = buckets[
            timeframe
        ]

        rows.append(
            {
                "timeframe":
                    timeframe,

                "targets":
                    bucket[
                        "targets"
                    ],

                "recovered":
                    bucket[
                        "recovered"
                    ],

                "remaining_gap":
                    bucket[
                        "remaining_gap"
                    ],

                "invalid_target":
                    bucket[
                        "invalid_target"
                    ],

                "ohlcv_error":
                    bucket[
                        "ohlcv_error"
                    ],

                "expected_missing_from_plan":
                    bucket[
                        "expected_missing_from_plan"
                    ],

                "current_missing":
                    bucket[
                        "current_missing"
                    ],
            }
        )

    return rows


# ============================================================
# 29. RECOVERY RESULT CROSS-CHECK
# ============================================================


def recovery_result_remaining_values(
    recovery_result: Dict[str, Any],
) -> Tuple[
    Optional[int],
    Optional[int],
]:

    final_revalidation = recovery_result.get(
        "final_revalidation"
    )

    if not isinstance(
        final_revalidation,
        dict,
    ):

        return (
            None,
            None,
        )

    targets_raw = final_revalidation.get(
        "remaining_gap_targets"
    )

    missing_raw = final_revalidation.get(
        "remaining_missing_timestamps"
    )

    try:

        targets = int(
            targets_raw
        )

    except Exception:

        targets = None

    try:

        missing = int(
            missing_raw
        )

    except Exception:

        missing = None

    return (
        targets,
        missing,
    )


# ============================================================
# 30. FINAL RESULT
# ============================================================


def determine_final_status(
    *,
    detail_rows: Sequence[
        Dict[str, Any]
    ],
    inventory_rows: Sequence[
        Dict[str, Any]
    ],
    event_contract: Dict[str, Any],
    hash_contract: Dict[str, Any],
    recovery_result: Dict[str, Any],
) -> Tuple[
    str,
    List[str],
    Dict[str, int],
]:

    reasons: List[str] = []

    target_counts = Counter(
        clean_text(
            row.get(
                "audit_status"
            )
        )
        for row
        in detail_rows
    )

    inventory_failures = sum(
        1
        for row
        in inventory_rows
        if clean_text(
            row.get(
                "status"
            )
        )
        != "PASS"
    )

    remaining_targets = sum(
        1
        for row
        in detail_rows
        if safe_int(
            row.get(
                "current_missing_count"
            )
        )
        > 0
    )

    remaining_missing = sum(
        safe_int(
            row.get(
                "current_missing_count"
            )
        )
        for row
        in detail_rows
    )

    critical = False

    if inventory_failures > 0:

        critical = True

        reasons.append(
            "One or more production OHLCV files failed "
            "inventory/hash/timestamp integrity validation."
        )

    if (
        target_counts[
            AUDIT_INVALID_TARGET
        ]
        > 0
    ):

        critical = True

        reasons.append(
            "One or more second-pass targets are invalid."
        )

    if (
        target_counts[
            AUDIT_OHLCV_ERROR
        ]
        > 0
    ):

        critical = True

        reasons.append(
            "One or more target/recovery/current-OHLCV "
            "cross-checks failed."
        )

    if not event_contract.get(
        "event_sets_match"
    ):

        critical = True

        reasons.append(
            "Second-pass target event set does not match "
            "recovery detail event set."
        )

    if not hash_contract.get(
        "hash_key_sets_match"
    ):

        critical = True

        reasons.append(
            "Recovery BEFORE/AFTER SHA256 key sets differ."
        )

    if not hash_contract.get(
        "hash_inventory_count_valid"
    ):

        critical = True

        reasons.append(
            "Recovery SHA256 inventory does not contain "
            f"exactly {EXPECTED_OHLCV_FILES} files."
        )

    (
        reported_remaining_targets,
        reported_remaining_missing,
    ) = recovery_result_remaining_values(
        recovery_result
    )

    if (
        reported_remaining_targets
        is None
        or reported_remaining_missing
        is None
    ):

        critical = True

        reasons.append(
            "Recovery result final_revalidation evidence "
            "is missing or invalid."
        )

    else:

        if (
            reported_remaining_targets
            != remaining_targets
        ):

            critical = True

            reasons.append(
                "Recovery result remaining_gap_targets "
                "does not match independent audit. "
                f"report={reported_remaining_targets} "
                f"audit={remaining_targets}"
            )

        if (
            reported_remaining_missing
            != remaining_missing
        ):

            critical = True

            reasons.append(
                "Recovery result remaining_missing_timestamps "
                "does not match independent audit. "
                f"report={reported_remaining_missing} "
                f"audit={remaining_missing}"
            )

    if critical:

        final_status = FINAL_FAIL

    elif remaining_missing > 0:

        final_status = (
            FINAL_PASS_WITH_UNAVAILABLE_HISTORY
        )

        reasons.append(
            "Production integrity is valid, but historical "
            "target timestamps still remain missing."
        )

        reasons.append(
            "This audit does not guess whether those timestamps "
            "are permanently unavailable from the API."
        )

    else:

        final_status = FINAL_PASS

        reasons.append(
            "All second-pass target timestamps are present."
        )

        reasons.append(
            "Production OHLCV integrity and recovery evidence "
            "passed post-recovery revalidation."
        )

    metrics = {
        "targets":
            len(
                detail_rows
            ),

        "recovered_targets":
            target_counts[
                AUDIT_RECOVERED
            ],

        "remaining_gap_targets":
            remaining_targets,

        "remaining_missing_timestamps":
            remaining_missing,

        "invalid_targets":
            target_counts[
                AUDIT_INVALID_TARGET
            ],

        "ohlcv_errors":
            target_counts[
                AUDIT_OHLCV_ERROR
            ],

        "inventory_failures":
            inventory_failures,
    }

    return (
        final_status,
        reasons,
        metrics,
    )


# ============================================================
# 31. BUILD RESULT JSON
# ============================================================


def build_result_json(
    *,
    final_status: str,
    reasons: Sequence[str],
    metrics: Dict[str, int],
    inventory: Dict[str, Any],
    event_contract: Dict[str, Any],
    hash_contract: Dict[str, Any],
    recovery_result: Dict[str, Any],
) -> Dict[str, Any]:

    (
        recovery_remaining_targets,
        recovery_remaining_missing,
    ) = recovery_result_remaining_values(
        recovery_result
    )

    next_stage_allowed = (
        final_status
        in {
            FINAL_PASS,
            FINAL_PASS_WITH_UNAVAILABLE_HISTORY,
        }
    )

    return {
        "program":
            PROGRAM,

        "version":
            VERSION,

        "mode":
            EXECUTION_MODE,

        "generated_utc":
            utc_now_iso(),

        "final_status":
            final_status,

        "reasons":
            list(
                reasons
            ),

        "production_inventory": {
            "expected_markets":
                EXPECTED_MARKETS,

            "expected_timeframes":
                EXPECTED_TIMEFRAMES,

            "expected_ohlcv_files":
                EXPECTED_OHLCV_FILES,

            "markets":
                inventory[
                    "markets"
                ],

            "h1":
                inventory[
                    "h1"
                ],

            "h4":
                inventory[
                    "h4"
                ],

            "d1":
                inventory[
                    "d1"
                ],

            "total":
                inventory[
                    "total"
                ],
        },

        "post_recovery_audit": {
            "targets":
                metrics[
                    "targets"
                ],

            "recovered_targets":
                metrics[
                    "recovered_targets"
                ],

            "remaining_gap_targets":
                metrics[
                    "remaining_gap_targets"
                ],

            "remaining_missing_timestamps":
                metrics[
                    "remaining_missing_timestamps"
                ],

            "invalid_targets":
                metrics[
                    "invalid_targets"
                ],

            "ohlcv_errors":
                metrics[
                    "ohlcv_errors"
                ],

            "inventory_failures":
                metrics[
                    "inventory_failures"
                ],
        },

        "recovery_report_claim": {
            "remaining_gap_targets":
                recovery_remaining_targets,

            "remaining_missing_timestamps":
                recovery_remaining_missing,
        },

        "event_contract":
            event_contract,

        "hash_contract":
            hash_contract,

        "input_evidence": {
            "second_pass_target_file":
                str(
                    SECOND_PASS_TARGET_FILE
                ),

            "second_pass_target_sha256":
                sha256_file(
                    SECOND_PASS_TARGET_FILE
                ),

            "second_pass_plan_file":
                str(
                    SECOND_PASS_PLAN_FILE
                ),

            "second_pass_plan_sha256":
                sha256_file(
                    SECOND_PASS_PLAN_FILE
                ),

            "recovery_summary_file":
                str(
                    RECOVERY_SUMMARY_FILE
                ),

            "recovery_summary_sha256":
                sha256_file(
                    RECOVERY_SUMMARY_FILE
                ),

            "recovery_detail_file":
                str(
                    RECOVERY_DETAIL_FILE
                ),

            "recovery_detail_sha256":
                sha256_file(
                    RECOVERY_DETAIL_FILE
                ),

            "recovery_result_file":
                str(
                    RECOVERY_RESULT_FILE
                ),

            "recovery_result_sha256":
                sha256_file(
                    RECOVERY_RESULT_FILE
                ),

            "recovery_before_sha256_file":
                str(
                    BEFORE_SHA256_FILE
                ),

            "recovery_after_sha256_file":
                str(
                    AFTER_SHA256_FILE
                ),

            "recovery_checkpoint_file":
                str(
                    RECOVERY_CHECKPOINT_FILE
                ),
        },

        "outputs": {
            "summary":
                str(
                    AUDIT_SUMMARY_FILE
                ),

            "detail":
                str(
                    AUDIT_DETAIL_FILE
                ),

            "inventory":
                str(
                    AUDIT_INVENTORY_FILE
                ),

            "result":
                str(
                    AUDIT_RESULT_FILE
                ),

            "checkpoint":
                str(
                    AUDIT_CHECKPOINT_FILE
                ),

            "status":
                str(
                    STATUS_FILE
                ),
        },

        "next_stage": {
            "allowed":
                next_stage_allowed,

            "gate":
                final_status,

            "feature_build_executed":
                False,

            "detector_256_executed":
                False,

            "future_labels_generated":
                False,

            "prediction_executed":
                False,

            "trading_executed":
                False,
        },

        "safety":
            SAFETY,
    }


# ============================================================
# 32. CHECKPOINT
# ============================================================


def build_checkpoint(
    *,
    final_status: str,
    metrics: Dict[str, int],
) -> Dict[str, Any]:

    return {
        "program":
            PROGRAM,

        "version":
            VERSION,

        "mode":
            EXECUTION_MODE,

        "status":
            "COMPLETED",

        "completed":
            True,

        "completed_utc":
            utc_now_iso(),

        "final_status":
            final_status,

        "source_fingerprint": {
            "second_pass_targets_sha256":
                sha256_file(
                    SECOND_PASS_TARGET_FILE
                ),

            "second_pass_plan_sha256":
                sha256_file(
                    SECOND_PASS_PLAN_FILE
                ),

            "recovery_detail_sha256":
                sha256_file(
                    RECOVERY_DETAIL_FILE
                ),

            "recovery_result_sha256":
                sha256_file(
                    RECOVERY_RESULT_FILE
                ),

            "recovery_after_sha256_sha256":
                sha256_file(
                    AFTER_SHA256_FILE
                ),
        },

        "summary":
            metrics,

        "safety":
            SAFETY,
    }


# ============================================================
# 33. VERIFY GENERATED OUTPUTS
# ============================================================


def verify_generated_outputs(
    *,
    expected_targets: int,
    expected_inventory: int,
) -> None:

    required = (
        AUDIT_SUMMARY_FILE,
        AUDIT_DETAIL_FILE,
        AUDIT_INVENTORY_FILE,
        AUDIT_RESULT_FILE,
        AUDIT_CHECKPOINT_FILE,
        STATUS_FILE,
    )

    for path in required:

        if not path.is_file():

            raise RuntimeError(
                f"Generated output missing: {path}"
            )

        if path.stat().st_size <= 0:

            raise RuntimeError(
                f"Generated output empty: {path}"
            )

    _, detail_rows = read_csv(
        AUDIT_DETAIL_FILE
    )

    if (
        len(detail_rows)
        != expected_targets
    ):

        raise RuntimeError(
            "Generated audit detail target count mismatch. "
            f"expected={expected_targets} "
            f"found={len(detail_rows)}"
        )

    _, inventory_rows = read_csv(
        AUDIT_INVENTORY_FILE
    )

    if (
        len(inventory_rows)
        != expected_inventory
    ):

        raise RuntimeError(
            "Generated audit inventory count mismatch. "
            f"expected={expected_inventory} "
            f"found={len(inventory_rows)}"
        )

    result = read_json(
        AUDIT_RESULT_FILE
    )

    if (
        clean_text(
            result.get(
                "program"
            )
        )
        != PROGRAM
    ):

        raise RuntimeError(
            "Generated audit result program mismatch."
        )

    if (
        clean_text(
            result.get(
                "version"
            )
        )
        != VERSION
    ):

        raise RuntimeError(
            "Generated audit result version mismatch."
        )

    checkpoint = read_json(
        AUDIT_CHECKPOINT_FILE
    )

    if (
        checkpoint.get(
            "completed"
        )
        is not True
    ):

        raise RuntimeError(
            "Generated audit checkpoint is not completed."
        )


# ============================================================
# 34. RUN AUDIT
# ============================================================


def run_audit() -> int:

    print()
    print(
        "=" * 72
    )
    print(
        "UPBIT SURGE MONITOR"
    )
    print(
        "POST-RECOVERY AUDIT REVALIDATION"
    )
    print(
        VERSION
    )
    print(
        "=" * 72
    )
    print()

    prepare_output_directories()

    write_status(
        stage="START",
        status="RUNNING",
        message=(
            "Post-recovery read-only audit started."
        ),
    )

    # --------------------------------------------------------
    # Input validation
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "1. VALIDATE POST-RECOVERY INPUT CONTRACT"
    )
    print(
        "=" * 72
    )

    (
        plan,
        recovery_result,
        recovery_checkpoint,
    ) = validate_required_inputs()

    print(
        "[PASS] Required recovery evidence exists."
    )

    print(
        "[PASS] Planner contract valid."
    )

    print(
        "[PASS] Recovery checkpoint COMPLETED."
    )

    print()

    # --------------------------------------------------------
    # Load targets
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "2. LOAD SECOND-PASS TARGETS"
    )
    print(
        "=" * 72
    )

    targets = load_targets()

    print(
        f"[PASS] Second-pass targets : {len(targets)}"
    )

    print()

    # --------------------------------------------------------
    # Recovery detail
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "3. LOAD SECOND-PASS RECOVERY DETAIL"
    )
    print(
        "=" * 72
    )

    recovery_detail = (
        load_recovery_detail()
    )

    print(
        f"[PASS] Recovery detail rows : "
        f"{len(recovery_detail)}"
    )

    print()

    # --------------------------------------------------------
    # Event contract
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "4. VERIFY TARGET / RECOVERY EVENT CONTRACT"
    )
    print(
        "=" * 72
    )

    event_contract = (
        verify_event_contract(
            targets,
            recovery_detail,
        )
    )

    print(
        f"Target events   : "
        f"{event_contract['target_event_count']}"
    )

    print(
        f"Recovery events : "
        f"{event_contract['recovery_event_count']}"
    )

    print(
        f"Event sets match: "
        f"{event_contract['event_sets_match']}"
    )

    print()

    # --------------------------------------------------------
    # Hash evidence
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "5. LOAD AND VERIFY RECOVERY SHA256 EVIDENCE"
    )
    print(
        "=" * 72
    )

    before_hashes = load_hash_report(
        BEFORE_SHA256_FILE
    )

    after_hashes = load_hash_report(
        AFTER_SHA256_FILE
    )

    hash_contract = verify_hash_contract(
        before_hashes,
        after_hashes,
    )

    print(
        f"BEFORE SHA256 : "
        f"{hash_contract['before_hash_files']}"
    )

    print(
        f"AFTER SHA256  : "
        f"{hash_contract['after_hash_files']}"
    )

    print(
        f"Hash sets match: "
        f"{hash_contract['hash_key_sets_match']}"
    )

    print()

    # --------------------------------------------------------
    # Full production inventory
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "6. REVALIDATE CURRENT PRODUCTION OHLCV"
    )
    print(
        "=" * 72
    )

    (
        inventory,
        inventory_rows,
        analyses,
    ) = audit_full_inventory(
        before_hashes,
        after_hashes,
    )

    atomic_csv_write(
        AUDIT_INVENTORY_FILE,
        INVENTORY_FIELDS,
        inventory_rows,
    )

    print()

    print(
        f"H1     : {inventory['h1']}"
    )

    print(
        f"H4     : {inventory['h4']}"
    )

    print(
        f"D1     : {inventory['d1']}"
    )

    print(
        f"Markets: {inventory['markets']}"
    )

    print(
        f"Total  : {inventory['total']}"
    )

    inventory_failures = sum(
        1
        for row
        in inventory_rows
        if clean_text(
            row.get(
                "status"
            )
        )
        != "PASS"
    )

    print(
        f"Integrity failures: {inventory_failures}"
    )

    print()

    # --------------------------------------------------------
    # Target audit
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "7. INDEPENDENT TARGET REVALIDATION"
    )
    print(
        "=" * 72
    )

    detail_rows = audit_targets(
        targets,
        analyses,
        recovery_detail,
    )

    atomic_csv_write(
        AUDIT_DETAIL_FILE,
        DETAIL_FIELDS,
        detail_rows,
    )

    print()

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------

    summary_rows = build_summary_rows(
        detail_rows
    )

    atomic_csv_write(
        AUDIT_SUMMARY_FILE,
        SUMMARY_FIELDS,
        summary_rows,
    )

    # --------------------------------------------------------
    # Final decision
    # --------------------------------------------------------

    print(
        "=" * 72
    )
    print(
        "8. FINAL POST-RECOVERY DECISION"
    )
    print(
        "=" * 72
    )

    (
        final_status,
        reasons,
        metrics,
    ) = determine_final_status(
        detail_rows=detail_rows,
        inventory_rows=inventory_rows,
        event_contract=event_contract,
        hash_contract=hash_contract,
        recovery_result=recovery_result,
    )

    result_payload = build_result_json(
        final_status=final_status,
        reasons=reasons,
        metrics=metrics,
        inventory=inventory,
        event_contract=event_contract,
        hash_contract=hash_contract,
        recovery_result=recovery_result,
    )

    atomic_json_write(
        AUDIT_RESULT_FILE,
        result_payload,
    )

    checkpoint = build_checkpoint(
        final_status=final_status,
        metrics=metrics,
    )

    atomic_json_write(
        AUDIT_CHECKPOINT_FILE,
        checkpoint,
    )

    write_status(
        stage="COMPLETE",
        status=final_status,
        message="; ".join(
            reasons
        ),
        target_index=len(
            targets
        ),
        target_total=len(
            targets
        ),
    )

    verify_generated_outputs(
        expected_targets=len(
            targets
        ),
        expected_inventory=EXPECTED_OHLCV_FILES,
    )

    # --------------------------------------------------------
    # Console report
    # --------------------------------------------------------

    print(
        f"FINAL STATUS                 : "
        f"{final_status}"
    )

    print(
        f"Targets                      : "
        f"{metrics['targets']}"
    )

    print(
        f"Recovered targets            : "
        f"{metrics['recovered_targets']}"
    )

    print(
        f"Remaining gap targets        : "
        f"{metrics['remaining_gap_targets']}"
    )

    print(
        f"Remaining missing timestamps : "
        f"{metrics['remaining_missing_timestamps']}"
    )

    print(
        f"Invalid targets              : "
        f"{metrics['invalid_targets']}"
    )

    print(
        f"OHLCV errors                 : "
        f"{metrics['ohlcv_errors']}"
    )

    print(
        f"Inventory failures           : "
        f"{metrics['inventory_failures']}"
    )

    print()

    print(
        "Generated:"
    )

    print(
        f"  {AUDIT_SUMMARY_FILE}"
    )

    print(
        f"  {AUDIT_DETAIL_FILE}"
    )

    print(
        f"  {AUDIT_INVENTORY_FILE}"
    )

    print(
        f"  {AUDIT_RESULT_FILE}"
    )

    print(
        f"  {AUDIT_CHECKPOINT_FILE}"
    )

    print(
        f"  {STATUS_FILE}"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  OHLCV write               : FORBIDDEN"
    )

    print(
        "  OHLCV delete              : FORBIDDEN"
    )

    print(
        "  Existing candle overwrite : FORBIDDEN"
    )

    print(
        "  API download              : FORBIDDEN"
    )

    print(
        "  Gap repair                : FORBIDDEN"
    )

    print(
        "  Feature build             : DISABLED"
    )

    print(
        "  256 Detector              : DISABLED"
    )

    print(
        "  Future labels             : DISABLED"
    )

    print(
        "  Prediction                : DISABLED"
    )

    print(
        "  Trading                   : DISABLED"
    )

    print(
        "  Git reset                 : DISABLED"
    )

    print(
        "  Git clean                 : DISABLED"
    )

    print(
        "  Git commit                : DISABLED"
    )

    print(
        "  Git push                  : DISABLED"
    )

    print()

    for reason in reasons:

        print(
            f"- {reason}"
        )

    print()

    if final_status == FINAL_PASS:

        print(
            "[PASS] POST-RECOVERY AUDIT REVALIDATION PASSED."
        )

        print(
            "[PASS] No second-pass target timestamps remain "
            "missing."
        )

        print(
            "[GATE] Production OHLCV passed the post-recovery "
            "audit gate."
        )

    elif (
        final_status
        == FINAL_PASS_WITH_UNAVAILABLE_HISTORY
    ):

        print(
            "[PASS] Production OHLCV integrity validation "
            "passed."
        )

        print(
            "[INFO] Some historical target timestamps remain "
            "missing."
        )

        print(
            "[IMPORTANT] Remaining timestamps were NOT "
            "automatically classified as API-unavailable."
        )

        print(
            "[GATE] Data integrity passed, but remaining "
            "historical gaps must stay explicitly documented."
        )

    else:

        print(
            "[FAIL] POST-RECOVERY AUDIT REVALIDATION FAILED."
        )

        print(
            "[BLOCK] Do NOT proceed to feature generation, "
            "pattern quantification, backtest, ML, scanner, "
            "or prediction."
        )

    print(
        "=" * 72
    )

    if final_status == FINAL_FAIL:

        return 1

    return 0


# ============================================================
# 35. MAIN
# ============================================================


def main() -> int:

    try:

        return run_audit()

    except KeyboardInterrupt:

        print()
        print(
            "=" * 72
        )
        print(
            "POST-RECOVERY AUDIT INTERRUPTED"
        )
        print(
            "=" * 72
        )

        print(
            "[SAFE] No OHLCV write was executed."
        )

        print(
            "[SAFE] No OHLCV delete was executed."
        )

        print(
            "[SAFE] No API download was executed."
        )

        print(
            "[SAFE] No historical gap repair was executed."
        )

        print(
            "[SAFE] Production OHLCV remains untouched."
        )

        print(
            "[RESUME] Run the same audit again."
        )

        print(
            "=" * 72
        )

        return 130

    except Exception as exc:

        print()
        print(
            "=" * 72
        )
        print(
            "POST-RECOVERY AUDIT FATAL ERROR"
        )
        print(
            "=" * 72
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
            "[SAFETY] No existing candle overwrite was "
            "executed."
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
            "[SAFETY] No 256 detector was executed."
        )

        print(
            "[SAFETY] No future labels were generated."
        )

        print(
            "[SAFETY] No prediction/trading was executed."
        )

        print(
            "[SAFETY] No Git reset/clean/commit/push was "
            "executed."
        )

        print()

        print(
            "[BLOCK] Do not proceed to the next research stage "
            "until this audit passes."
        )

        print(
            "=" * 72
        )

        return 1


# ============================================================
# 36. ENTRY POINT
# ============================================================


if __name__ == "__main__":

    sys.exit(
        main()
    )


# ============================================================
# END
# ============================================================
