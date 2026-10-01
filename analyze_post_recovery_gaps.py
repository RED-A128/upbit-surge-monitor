# ============================================================
# Upbit Surge Monitor
# analyze_post_recovery_gaps.py
# Clean V001
# ============================================================
#
# PURPOSE
# ------------------------------------------------------------
# Analyze historical OHLCV gaps AFTER the first real recovery.
#
# This program is READ ONLY for:
#
#   data/ohlcv/h1/
#   data/ohlcv/h4/
#   data/ohlcv/d1/
#
# It does NOT:
#
#   - modify OHLCV
#   - delete OHLCV
#   - download candles
#   - repair gaps
#   - run feature build
#   - run detector
#   - run prediction
#   - trade
#   - git reset
#   - git clean
#   - git commit
#   - git push
#
# It DOES:
#
#   1. Locate pre-recovery gap report
#   2. Locate post-recovery gap report
#   3. Detect report schema
#   4. Compare pre/post gap state
#   5. Calculate recovery performance
#   6. Identify remaining gaps
#   7. Classify second-pass recovery candidates
#   8. Write analysis reports
#   9. Maintain persistent checkpoint
#
# ============================================================


from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import traceback

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ============================================================
# 1. VERSION
# ============================================================

VERSION = "Clean V001"
PROGRAM_NAME = "analyze_post_recovery_gaps.py"


# ============================================================
# 2. PROJECT PATHS
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent

DATA_DIR = ROOT_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_DIR / "h1"
H4_DIR = OHLCV_DIR / "h4"
D1_DIR = OHLCV_DIR / "d1"

REPORTS_DIR = DATA_DIR / "reports"
VALIDATION_DIR = DATA_DIR / "validation"
RECOVERY_DIR = DATA_DIR / "recovery"

ANALYSIS_DIR = REPORTS_DIR / "post_recovery_analysis"

CHECKPOINT_PATH = (
    VALIDATION_DIR
    / "analyze_post_recovery_gaps_checkpoint.json"
)

STATUS_PATH = (
    DATA_DIR
    / "analyze_post_recovery_gaps_status.csv"
)

SUMMARY_PATH = (
    ANALYSIS_DIR
    / "post_recovery_analysis_summary.csv"
)

DETAIL_PATH = (
    ANALYSIS_DIR
    / "post_recovery_analysis_detail.csv"
)

SECOND_PASS_PATH = (
    RECOVERY_DIR
    / "history_gap_second_pass_targets.csv"
)

RESULT_JSON_PATH = (
    ANALYSIS_DIR
    / "post_recovery_analysis_result.json"
)


# ============================================================
# 3. EXPECTED PROJECT SIZE
# ============================================================

EXPECTED_MARKETS = 290
EXPECTED_TIMEFRAMES = 3
EXPECTED_OHLCV_FILES = (
    EXPECTED_MARKETS * EXPECTED_TIMEFRAMES
)


# ============================================================
# 4. SAFETY FLAGS
# ============================================================

ALLOW_OHLCV_WRITE = False
ALLOW_OHLCV_DELETE = False
ALLOW_GAP_REPAIR = False
ALLOW_API_DOWNLOAD = False
ALLOW_FEATURE_BUILD = False
ALLOW_256_DETECTOR = False
ALLOW_FUTURE_LABELS = False
ALLOW_PREDICTION = False
ALLOW_TRADING = False

ALLOW_GIT_RESET = False
ALLOW_GIT_CLEAN = False
ALLOW_GIT_COMMIT = False
ALLOW_GIT_PUSH = False


# ============================================================
# 5. REPORT SEARCH LOCATIONS
# ============================================================

PRE_REPORT_CANDIDATES = [
    REPORTS_DIR
    / "history_gap"
    / "data_history_gap_report.csv",

    REPORTS_DIR
    / "history_gap"
    / "history_gap_report.csv",

    REPORTS_DIR
    / "history_gap"
    / "history_gap_detail.csv",

    DATA_DIR
    / "reports"
    / "history_gap"
    / "data_history_gap_report.csv",
]

POST_REPORT_CANDIDATES = [
    REPORTS_DIR
    / "post_recovery_gap_audit"
    / "data_history_gap_report.csv",

    REPORTS_DIR
    / "post_recovery_gap_audit"
    / "history_gap_report.csv",

    REPORTS_DIR
    / "post_recovery_gap_audit"
    / "post_recovery_gap_report.csv",

    REPORTS_DIR
    / "history_gap"
    / "post_recovery_gap_report.csv",

    DATA_DIR
    / "post_recovery_gap_report.csv",
]


# ============================================================
# 6. COLUMN ALIASES
# ============================================================

MARKET_ALIASES = [
    "market",
    "ticker",
    "symbol",
    "code",
]

TIMEFRAME_ALIASES = [
    "timeframe",
    "interval",
    "tf",
    "unit",
]

GAP_COUNT_ALIASES = [
    "gap_count",
    "gaps",
    "gap_events",
    "gap_event_count",
    "total_gaps",
]

MISSING_COUNT_ALIASES = [
    "missing_count",
    "missing",
    "missing_est",
    "missing_estimate",
    "estimated_missing",
    "missing_candles",
]

GAP_START_ALIASES = [
    "gap_start",
    "start",
    "start_time",
    "gap_start_time",
    "from_time",
]

GAP_END_ALIASES = [
    "gap_end",
    "end",
    "end_time",
    "gap_end_time",
    "to_time",
]

CLASS_ALIASES = [
    "classification",
    "class",
    "gap_class",
    "category",
]

RECOVERABLE_ALIASES = [
    "recoverable",
    "is_recoverable",
    "recovery_possible",
]


# ============================================================
# 7. UTILITY FUNCTIONS
# ============================================================

def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def ensure_runtime_directories() -> None:
    ANALYSIS_DIR.mkdir(
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


def normalize_text(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def normalize_column_name(value: str) -> str:
    value = normalize_text(value).lower()

    value = value.replace("-", "_")
    value = value.replace(" ", "_")
    value = value.replace(".", "_")

    while "__" in value:
        value = value.replace("__", "_")

    return value.strip("_")


def safe_int(value: Any) -> int:
    text = normalize_text(value)

    if not text:
        return 0

    try:
        return int(float(text))
    except Exception:
        return 0


def safe_bool(value: Any) -> Optional[bool]:
    text = normalize_text(value).lower()

    if text in {
        "1",
        "true",
        "yes",
        "y",
        "recoverable",
        "possible",
    }:
        return True

    if text in {
        "0",
        "false",
        "no",
        "n",
        "unrecoverable",
        "impossible",
    }:
        return False

    return None


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def count_csv_files(directory: Path) -> int:
    if not directory.exists():
        return 0

    return len(
        list(
            directory.glob("KRW-*.csv")
        )
    )


# ============================================================
# 8. CSV HELPERS
# ============================================================

def read_csv_rows(
    path: Path,
) -> Tuple[List[str], List[Dict[str, str]]]:

    encodings = [
        "utf-8-sig",
        "utf-8",
        "cp949",
    ]

    last_error: Optional[Exception] = None

    for encoding in encodings:
        try:
            with path.open(
                "r",
                encoding=encoding,
                newline="",
            ) as handle:

                reader = csv.DictReader(handle)

                if reader.fieldnames is None:
                    raise RuntimeError(
                        f"No CSV header: {path}"
                    )

                fieldnames = [
                    normalize_text(name)
                    for name in reader.fieldnames
                ]

                rows = []

                for row in reader:
                    clean_row = {}

                    for key, value in row.items():
                        if key is None:
                            continue

                        clean_row[
                            normalize_text(key)
                        ] = normalize_text(value)

                    rows.append(clean_row)

                return fieldnames, rows

        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error is not None:
        raise last_error

    raise RuntimeError(
        f"Unable to read CSV: {path}"
    )


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Dict[str, Any]],
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


# ============================================================
# 9. REPORT DISCOVERY
# ============================================================

def find_existing_candidate(
    candidates: Sequence[Path],
) -> Optional[Path]:

    for path in candidates:
        if path.exists() and path.is_file():
            return path

    return None


def find_csv_by_keywords(
    root: Path,
    required_keywords: Sequence[str],
) -> Optional[Path]:

    if not root.exists():
        return None

    matches: List[Path] = []

    for path in root.rglob("*.csv"):
        lower_name = path.name.lower()
        lower_full = str(path).lower()

        matched = True

        for keyword in required_keywords:
            keyword = keyword.lower()

            if (
                keyword not in lower_name
                and keyword not in lower_full
            ):
                matched = False
                break

        if matched:
            matches.append(path)

    if not matches:
        return None

    matches.sort(
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    return matches[0]


def discover_pre_report() -> Path:

    direct = find_existing_candidate(
        PRE_REPORT_CANDIDATES
    )

    if direct is not None:
        return direct

    candidates = [
        ("history", "gap"),
        ("gap", "report"),
    ]

    for keywords in candidates:
        result = find_csv_by_keywords(
            REPORTS_DIR,
            keywords,
        )

        if result is not None:
            lower = str(result).lower()

            if "post_recovery" not in lower:
                return result

    raise FileNotFoundError(
        "Unable to locate pre-recovery gap report."
    )


def discover_post_report(
    pre_report: Path,
) -> Path:

    direct = find_existing_candidate(
        POST_REPORT_CANDIDATES
    )

    if (
        direct is not None
        and direct.resolve()
        != pre_report.resolve()
    ):
        return direct

    keyword_sets = [
        ("post", "recovery", "gap"),
        ("post_recovery", "gap"),
        ("recovery", "gap", "report"),
    ]

    for keywords in keyword_sets:
        result = find_csv_by_keywords(
            REPORTS_DIR,
            keywords,
        )

        if (
            result is not None
            and result.resolve()
            != pre_report.resolve()
        ):
            return result

    raise FileNotFoundError(
        "Unable to locate post-recovery gap report."
    )


# ============================================================
# 10. COLUMN DETECTION
# ============================================================

def detect_column(
    fieldnames: Sequence[str],
    aliases: Sequence[str],
) -> Optional[str]:

    normalized_map = {
        normalize_column_name(name): name
        for name in fieldnames
    }

    for alias in aliases:
        normalized_alias = normalize_column_name(
            alias
        )

        if normalized_alias in normalized_map:
            return normalized_map[
                normalized_alias
            ]

    return None


def detect_schema(
    fieldnames: Sequence[str],
) -> Dict[str, Optional[str]]:

    schema = {
        "market": detect_column(
            fieldnames,
            MARKET_ALIASES,
        ),
        "timeframe": detect_column(
            fieldnames,
            TIMEFRAME_ALIASES,
        ),
        "gap_count": detect_column(
            fieldnames,
            GAP_COUNT_ALIASES,
        ),
        "missing_count": detect_column(
            fieldnames,
            MISSING_COUNT_ALIASES,
        ),
        "gap_start": detect_column(
            fieldnames,
            GAP_START_ALIASES,
        ),
        "gap_end": detect_column(
            fieldnames,
            GAP_END_ALIASES,
        ),
        "classification": detect_column(
            fieldnames,
            CLASS_ALIASES,
        ),
        "recoverable": detect_column(
            fieldnames,
            RECOVERABLE_ALIASES,
        ),
    }

    if schema["market"] is None:
        raise RuntimeError(
            "Unable to detect market column."
        )

    if schema["timeframe"] is None:
        raise RuntimeError(
            "Unable to detect timeframe column."
        )

    if (
        schema["gap_count"] is None
        and schema["missing_count"] is None
    ):
        raise RuntimeError(
            "Unable to detect gap/missing column."
        )

    return schema


# ============================================================
# 11. NORMALIZED REPORT RECORD
# ============================================================

def normalize_timeframe(value: str) -> str:
    text = normalize_text(value).lower()

    mapping = {
        "1h": "h1",
        "h1": "h1",
        "60": "h1",
        "60m": "h1",
        "4h": "h4",
        "h4": "h4",
        "240": "h4",
        "240m": "h4",
        "1d": "d1",
        "d1": "d1",
        "day": "d1",
        "daily": "d1",
    }

    return mapping.get(text, text)


def normalize_market(value: str) -> str:
    return normalize_text(value).upper()


def normalized_record(
    row: Dict[str, str],
    schema: Dict[str, Optional[str]],
) -> Dict[str, Any]:

    market_column = schema["market"]
    timeframe_column = schema["timeframe"]

    market = normalize_market(
        row.get(market_column or "", "")
    )

    timeframe = normalize_timeframe(
        row.get(timeframe_column or "", "")
    )

    gap_count = 0
    missing_count = 0

    if schema["gap_count"]:
        gap_count = safe_int(
            row.get(
                schema["gap_count"] or "",
                "",
            )
        )

    if schema["missing_count"]:
        missing_count = safe_int(
            row.get(
                schema["missing_count"] or "",
                "",
            )
        )

    classification = ""

    if schema["classification"]:
        classification = normalize_text(
            row.get(
                schema["classification"] or "",
                "",
            )
        )

    recoverable: Optional[bool] = None

    if schema["recoverable"]:
        recoverable = safe_bool(
            row.get(
                schema["recoverable"] or "",
                "",
            )
        )

    gap_start = ""

    if schema["gap_start"]:
        gap_start = normalize_text(
            row.get(
                schema["gap_start"] or "",
                "",
            )
        )

    gap_end = ""

    if schema["gap_end"]:
        gap_end = normalize_text(
            row.get(
                schema["gap_end"] or "",
                "",
            )
        )

    return {
        "market": market,
        "timeframe": timeframe,
        "gap_count": gap_count,
        "missing_count": missing_count,
        "classification": classification,
        "recoverable": recoverable,
        "gap_start": gap_start,
        "gap_end": gap_end,
    }


# ============================================================
# 12. AGGREGATION
# ============================================================

def aggregate_report(
    rows: Sequence[Dict[str, str]],
    schema: Dict[str, Optional[str]],
) -> Dict[Tuple[str, str], Dict[str, Any]]:

    result: Dict[
        Tuple[str, str],
        Dict[str, Any],
    ] = {}

    for raw_row in rows:
        row = normalized_record(
            raw_row,
            schema,
        )

        market = row["market"]
        timeframe = row["timeframe"]

        if not market or not timeframe:
            continue

        key = (
            market,
            timeframe,
        )

        if key not in result:
            result[key] = {
                "market": market,
                "timeframe": timeframe,
                "gap_count": 0,
                "missing_count": 0,
                "rows": 0,
            }

        result[key]["gap_count"] += int(
            row["gap_count"]
        )

        result[key]["missing_count"] += int(
            row["missing_count"]
        )

        result[key]["rows"] += 1

    return result


# ============================================================
# 13. OHLCV INVENTORY VALIDATION
# ============================================================

def validate_ohlcv_inventory() -> Dict[str, int]:

    counts = {
        "h1": count_csv_files(H1_DIR),
        "h4": count_csv_files(H4_DIR),
        "d1": count_csv_files(D1_DIR),
    }

    total = sum(counts.values())

    print("")
    print(
        "============================================================"
    )
    print(
        "OHLCV INVENTORY"
    )
    print(
        "============================================================"
    )

    print(
        f"H1 files : {counts['h1']}"
    )
    print(
        f"H4 files : {counts['h4']}"
    )
    print(
        f"D1 files : {counts['d1']}"
    )
    print(
        f"Total    : {total}"
    )

    if counts["h1"] != EXPECTED_MARKETS:
        raise RuntimeError(
            "H1 market count mismatch. "
            f"expected={EXPECTED_MARKETS}, "
            f"found={counts['h1']}"
        )

    if counts["h4"] != EXPECTED_MARKETS:
        raise RuntimeError(
            "H4 market count mismatch. "
            f"expected={EXPECTED_MARKETS}, "
            f"found={counts['h4']}"
        )

    if counts["d1"] != EXPECTED_MARKETS:
        raise RuntimeError(
            "D1 market count mismatch. "
            f"expected={EXPECTED_MARKETS}, "
            f"found={counts['d1']}"
        )

    if total != EXPECTED_OHLCV_FILES:
        raise RuntimeError(
            "Total OHLCV file count mismatch. "
            f"expected={EXPECTED_OHLCV_FILES}, "
            f"found={total}"
        )

    return counts


# ============================================================
# 14. ANALYSIS CLASSIFICATION
# ============================================================

def determine_result_class(
    pre_gaps: int,
    pre_missing: int,
    post_gaps: int,
    post_missing: int,
) -> str:

    if (
        post_gaps == 0
        and post_missing == 0
    ):
        if (
            pre_gaps > 0
            or pre_missing > 0
        ):
            return "FULLY_RECOVERED"

        return "NO_GAP"

    if (
        post_gaps < pre_gaps
        or post_missing < pre_missing
    ):
        return "PARTIALLY_RECOVERED"

    if (
        post_gaps == pre_gaps
        and post_missing == pre_missing
    ):
        return "UNCHANGED"

    if (
        post_gaps > pre_gaps
        or post_missing > pre_missing
    ):
        return "INCREASED"

    return "REVIEW_REQUIRED"


def determine_second_pass(
    result_class: str,
    post_gaps: int,
    post_missing: int,
) -> Tuple[bool, str]:

    if (
        post_gaps <= 0
        and post_missing <= 0
    ):
        return (
            False,
            "NO_REMAINING_GAP",
        )

    if result_class == "PARTIALLY_RECOVERED":
        return (
            True,
            "REMAINING_AFTER_PARTIAL_RECOVERY",
        )

    if result_class == "UNCHANGED":
        return (
            True,
            "UNCHANGED_GAP_REQUIRES_REVIEW",
        )

    if result_class == "INCREASED":
        return (
            False,
            "INCREASED_GAP_MANUAL_REVIEW_FIRST",
        )

    return (
        True,
        "REMAINING_GAP_REQUIRES_CLASSIFICATION",
    )


# ============================================================
# 15. COMPARE PRE / POST
# ============================================================

def compare_reports(
    pre_data: Dict[
        Tuple[str, str],
        Dict[str, Any],
    ],
    post_data: Dict[
        Tuple[str, str],
        Dict[str, Any],
    ],
) -> List[Dict[str, Any]]:

    all_keys = sorted(
        set(pre_data.keys())
        | set(post_data.keys())
    )

    output: List[Dict[str, Any]] = []

    for market, timeframe in all_keys:

        pre = pre_data.get(
            (market, timeframe),
            {},
        )

        post = post_data.get(
            (market, timeframe),
            {},
        )

        pre_gaps = safe_int(
            pre.get("gap_count", 0)
        )

        pre_missing = safe_int(
            pre.get("missing_count", 0)
        )

        post_gaps = safe_int(
            post.get("gap_count", 0)
        )

        post_missing = safe_int(
            post.get("missing_count", 0)
        )

        recovered_gap_events = max(
            0,
            pre_gaps - post_gaps,
        )

        recovered_missing = max(
            0,
            pre_missing - post_missing,
        )

        result_class = determine_result_class(
            pre_gaps=pre_gaps,
            pre_missing=pre_missing,
            post_gaps=post_gaps,
            post_missing=post_missing,
        )

        second_pass, second_pass_reason = (
            determine_second_pass(
                result_class=result_class,
                post_gaps=post_gaps,
                post_missing=post_missing,
            )
        )

        output.append(
            {
                "market": market,
                "timeframe": timeframe,

                "pre_gap_events": pre_gaps,
                "post_gap_events": post_gaps,

                "recovered_gap_events":
                    recovered_gap_events,

                "pre_missing_est": pre_missing,
                "post_missing_est": post_missing,

                "recovered_missing_est":
                    recovered_missing,

                "result_class": result_class,

                "second_pass_candidate":
                    "YES"
                    if second_pass
                    else "NO",

                "second_pass_reason":
                    second_pass_reason,
            }
        )

    return output


# ============================================================
# 16. SUMMARY CALCULATION
# ============================================================

def build_summary(
    detail_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:

    pre_gap_events = sum(
        safe_int(row["pre_gap_events"])
        for row in detail_rows
    )

    post_gap_events = sum(
        safe_int(row["post_gap_events"])
        for row in detail_rows
    )

    pre_missing = sum(
        safe_int(row["pre_missing_est"])
        for row in detail_rows
    )

    post_missing = sum(
        safe_int(row["post_missing_est"])
        for row in detail_rows
    )

    recovered_gap_events = max(
        0,
        pre_gap_events - post_gap_events,
    )

    recovered_missing = max(
        0,
        pre_missing - post_missing,
    )

    gap_recovery_rate = (
        (
            recovered_gap_events
            / pre_gap_events
        )
        * 100.0
        if pre_gap_events > 0
        else 0.0
    )

    missing_recovery_rate = (
        (
            recovered_missing
            / pre_missing
        )
        * 100.0
        if pre_missing > 0
        else 0.0
    )

    class_counter = Counter(
        normalize_text(
            row["result_class"]
        )
        for row in detail_rows
    )

    second_pass_count = sum(
        1
        for row in detail_rows
        if row["second_pass_candidate"]
        == "YES"
    )

    markets_with_remaining_gap = {
        row["market"]
        for row in detail_rows
        if (
            safe_int(
                row["post_gap_events"]
            ) > 0
            or safe_int(
                row["post_missing_est"]
            ) > 0
        )
    }

    jobs_with_remaining_gap = sum(
        1
        for row in detail_rows
        if (
            safe_int(
                row["post_gap_events"]
            ) > 0
            or safe_int(
                row["post_missing_est"]
            ) > 0
        )
    )

    return {
        "pre_gap_events":
            pre_gap_events,

        "post_gap_events":
            post_gap_events,

        "recovered_gap_events":
            recovered_gap_events,

        "gap_recovery_rate_pct":
            round(
                gap_recovery_rate,
                4,
            ),

        "pre_missing_est":
            pre_missing,

        "post_missing_est":
            post_missing,

        "recovered_missing_est":
            recovered_missing,

        "missing_recovery_rate_pct":
            round(
                missing_recovery_rate,
                4,
            ),

        "fully_recovered_jobs":
            class_counter[
                "FULLY_RECOVERED"
            ],

        "partially_recovered_jobs":
            class_counter[
                "PARTIALLY_RECOVERED"
            ],

        "unchanged_jobs":
            class_counter[
                "UNCHANGED"
            ],

        "increased_jobs":
            class_counter[
                "INCREASED"
            ],

        "no_gap_jobs":
            class_counter[
                "NO_GAP"
            ],

        "review_required_jobs":
            class_counter[
                "REVIEW_REQUIRED"
            ],

        "remaining_gap_markets":
            len(
                markets_with_remaining_gap
            ),

        "remaining_gap_jobs":
            jobs_with_remaining_gap,

        "second_pass_candidates":
            second_pass_count,
    }


# ============================================================
# 17. OUTPUT BUILDERS
# ============================================================

def write_detail_report(
    rows: Sequence[Dict[str, Any]],
) -> None:

    fields = [
        "market",
        "timeframe",

        "pre_gap_events",
        "post_gap_events",
        "recovered_gap_events",

        "pre_missing_est",
        "post_missing_est",
        "recovered_missing_est",

        "result_class",

        "second_pass_candidate",
        "second_pass_reason",
    ]

    write_csv(
        DETAIL_PATH,
        fields,
        rows,
    )


def write_second_pass_targets(
    rows: Sequence[Dict[str, Any]],
) -> None:

    targets = [
        row
        for row in rows
        if row["second_pass_candidate"]
        == "YES"
    ]

    targets.sort(
        key=lambda row: (
            -safe_int(
                row["post_missing_est"]
            ),
            -safe_int(
                row["post_gap_events"]
            ),
            row["market"],
            row["timeframe"],
        )
    )

    fields = [
        "market",
        "timeframe",
        "post_gap_events",
        "post_missing_est",
        "result_class",
        "second_pass_reason",
    ]

    write_csv(
        SECOND_PASS_PATH,
        fields,
        targets,
    )


def write_summary_report(
    summary: Dict[str, Any],
) -> None:

    rows = [
        {
            "metric": key,
            "value": value,
        }
        for key, value
        in summary.items()
    ]

    write_csv(
        SUMMARY_PATH,
        [
            "metric",
            "value",
        ],
        rows,
    )


def write_result_json(
    summary: Dict[str, Any],
    pre_report: Path,
    post_report: Path,
    inventory: Dict[str, int],
) -> None:

    payload = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "execution_mode":
            "READ_ONLY_ANALYSIS",

        "created_utc":
            utc_now_iso(),

        "source": {
            "pre_recovery_gap_report":
                str(pre_report),

            "post_recovery_gap_report":
                str(post_report),

            "pre_report_sha256":
                file_sha256(pre_report),

            "post_report_sha256":
                file_sha256(post_report),
        },

        "ohlcv_inventory": {
            "h1": inventory["h1"],
            "h4": inventory["h4"],
            "d1": inventory["d1"],
            "total": sum(
                inventory.values()
            ),
        },

        "summary": summary,

        "safety": {
            "ohlcv_write":
                ALLOW_OHLCV_WRITE,

            "ohlcv_delete":
                ALLOW_OHLCV_DELETE,

            "gap_repair":
                ALLOW_GAP_REPAIR,

            "api_download":
                ALLOW_API_DOWNLOAD,

            "feature_build":
                ALLOW_FEATURE_BUILD,

            "detector_256":
                ALLOW_256_DETECTOR,

            "future_labels":
                ALLOW_FUTURE_LABELS,

            "prediction":
                ALLOW_PREDICTION,

            "trading":
                ALLOW_TRADING,

            "git_reset":
                ALLOW_GIT_RESET,

            "git_clean":
                ALLOW_GIT_CLEAN,

            "git_commit":
                ALLOW_GIT_COMMIT,

            "git_push":
                ALLOW_GIT_PUSH,
        },
    }

    RESULT_JSON_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with RESULT_JSON_PATH.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
        )


# ============================================================
# 18. CHECKPOINT
# ============================================================

def write_checkpoint(
    status: str,
    pre_report: Optional[Path] = None,
    post_report: Optional[Path] = None,
    summary: Optional[
        Dict[str, Any]
    ] = None,
) -> None:

    payload = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "status": status,
        "updated_utc": utc_now_iso(),
    }

    if pre_report is not None:
        payload[
            "pre_recovery_gap_report"
        ] = str(pre_report)

    if post_report is not None:
        payload[
            "post_recovery_gap_report"
        ] = str(post_report)

    if summary is not None:
        payload["summary"] = summary

    CHECKPOINT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with CHECKPOINT_PATH.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
        )


# ============================================================
# 19. STATUS
# ============================================================

def write_status(
    status: str,
    message: str,
) -> None:

    exists = STATUS_PATH.exists()

    STATUS_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with STATUS_PATH.open(
        "a",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "utc_time",
                "status",
                "message",
            ],
        )

        if not exists:
            writer.writeheader()

        writer.writerow(
            {
                "utc_time":
                    utc_now_iso(),

                "status":
                    status,

                "message":
                    message,
            }
        )


# ============================================================
# 20. CONSOLE SUMMARY
# ============================================================

def print_summary(
    summary: Dict[str, Any],
    pre_report: Path,
    post_report: Path,
) -> None:

    print("")
    print(
        "============================================================"
    )
    print(
        "POST-RECOVERY GAP ANALYSIS SUMMARY"
    )
    print(
        "============================================================"
    )

    print(
        f"Version              : {VERSION}"
    )

    print(
        "Execution mode       : READ ONLY ANALYSIS"
    )

    print("")
    print("Source:")

    print(
        f"  Pre report         : {pre_report}"
    )

    print(
        f"  Post report        : {post_report}"
    )

    print("")
    print("Gap events:")

    print(
        "  Before             : "
        f"{summary['pre_gap_events']}"
    )

    print(
        "  After              : "
        f"{summary['post_gap_events']}"
    )

    print(
        "  Recovered          : "
        f"{summary['recovered_gap_events']}"
    )

    print(
        "  Recovery rate      : "
        f"{summary['gap_recovery_rate_pct']}%"
    )

    print("")
    print("Missing estimate:")

    print(
        "  Before             : "
        f"{summary['pre_missing_est']}"
    )

    print(
        "  After              : "
        f"{summary['post_missing_est']}"
    )

    print(
        "  Recovered          : "
        f"{summary['recovered_missing_est']}"
    )

    print(
        "  Recovery rate      : "
        f"{summary['missing_recovery_rate_pct']}%"
    )

    print("")
    print("Job classification:")

    print(
        "  Fully recovered    : "
        f"{summary['fully_recovered_jobs']}"
    )

    print(
        "  Partially recovered: "
        f"{summary['partially_recovered_jobs']}"
    )

    print(
        "  Unchanged          : "
        f"{summary['unchanged_jobs']}"
    )

    print(
        "  Increased          : "
        f"{summary['increased_jobs']}"
    )

    print(
        "  No gap             : "
        f"{summary['no_gap_jobs']}"
    )

    print("")
    print("Remaining:")

    print(
        "  Markets w/gaps     : "
        f"{summary['remaining_gap_markets']}"
    )

    print(
        "  Jobs w/gaps        : "
        f"{summary['remaining_gap_jobs']}"
    )

    print(
        "  Second-pass jobs   : "
        f"{summary['second_pass_candidates']}"
    )

    print("")
    print("Safety:")

    print(
        "  OHLCV write        : DISABLED"
    )

    print(
        "  OHLCV delete       : DISABLED"
    )

    print(
        "  Gap repair         : DISABLED"
    )

    print(
        "  API download       : DISABLED"
    )

    print(
        "  Feature build      : DISABLED"
    )

    print(
        "  256 Detector       : DISABLED"
    )

    print(
        "  Future labels      : DISABLED"
    )

    print(
        "  Prediction         : DISABLED"
    )

    print(
        "  Trading            : DISABLED"
    )

    print(
        "  Git reset          : DISABLED"
    )

    print(
        "  Git clean          : DISABLED"
    )

    print(
        "  Git commit         : DISABLED"
    )

    print(
        "  Git push           : DISABLED"
    )

    print(
        "============================================================"
    )


# ============================================================
# 21. MAIN
# ============================================================

def main() -> int:

    print(
        "============================================================"
    )
    print(
        "UPBIT SURGE MONITOR"
    )
    print(
        "POST-RECOVERY GAP ANALYZER"
    )
    print(
        VERSION
    )
    print(
        "============================================================"
    )

    print(
        f"Program        : {PROGRAM_NAME}"
    )

    print(
        "Execution mode : READ ONLY ANALYSIS"
    )

    print(
        f"Root           : {ROOT_DIR}"
    )

    print(
        f"UTC time       : {utc_now_iso()}"
    )

    print(
        "============================================================"
    )

    ensure_runtime_directories()

    write_status(
        "STARTED",
        "Post-recovery gap analysis started.",
    )

    write_checkpoint(
        status="STARTED",
    )

    try:
        # ----------------------------------------------------
        # STEP 1
        # Validate OHLCV inventory.
        # ----------------------------------------------------

        inventory = validate_ohlcv_inventory()

        # ----------------------------------------------------
        # STEP 2
        # Discover source reports.
        # ----------------------------------------------------

        print("")
        print(
            "============================================================"
        )
        print(
            "DISCOVER SOURCE REPORTS"
        )
        print(
            "============================================================"
        )

        pre_report = discover_pre_report()

        post_report = discover_post_report(
            pre_report
        )

        print(
            f"Pre-recovery report  : {pre_report}"
        )

        print(
            f"Post-recovery report : {post_report}"
        )

        if (
            pre_report.resolve()
            == post_report.resolve()
        ):
            raise RuntimeError(
                "Pre/post report resolved to "
                "the same file."
            )

        # ----------------------------------------------------
        # STEP 3
        # Read reports.
        # ----------------------------------------------------

        pre_fields, pre_rows = (
            read_csv_rows(
                pre_report
            )
        )

        post_fields, post_rows = (
            read_csv_rows(
                post_report
            )
        )

        print("")
        print(
            "Pre rows             : "
            f"{len(pre_rows)}"
        )

        print(
            "Post rows            : "
            f"{len(post_rows)}"
        )

        # ----------------------------------------------------
        # STEP 4
        # Detect schemas.
        # ----------------------------------------------------

        pre_schema = detect_schema(
            pre_fields
        )

        post_schema = detect_schema(
            post_fields
        )

        print("")
        print(
            "Pre schema           : "
            f"{pre_schema}"
        )

        print(
            "Post schema          : "
            f"{post_schema}"
        )

        # ----------------------------------------------------
        # STEP 5
        # Aggregate reports.
        # ----------------------------------------------------

        pre_data = aggregate_report(
            pre_rows,
            pre_schema,
        )

        post_data = aggregate_report(
            post_rows,
            post_schema,
        )

        print("")
        print(
            "Pre jobs             : "
            f"{len(pre_data)}"
        )

        print(
            "Post jobs            : "
            f"{len(post_data)}"
        )

        # ----------------------------------------------------
        # STEP 6
        # Compare.
        # ----------------------------------------------------

        detail_rows = compare_reports(
            pre_data,
            post_data,
        )

        summary = build_summary(
            detail_rows
        )

        # ----------------------------------------------------
        # STEP 7
        # Write outputs.
        # ----------------------------------------------------

        write_detail_report(
            detail_rows
        )

        write_second_pass_targets(
            detail_rows
        )

        write_summary_report(
            summary
        )

        write_result_json(
            summary=summary,
            pre_report=pre_report,
            post_report=post_report,
            inventory=inventory,
        )

        # ----------------------------------------------------
        # STEP 8
        # Final checkpoint.
        # ----------------------------------------------------

        write_checkpoint(
            status="COMPLETED",
            pre_report=pre_report,
            post_report=post_report,
            summary=summary,
        )

        write_status(
            "COMPLETED",
            (
                "Post-recovery gap analysis "
                "completed successfully."
            ),
        )

        # ----------------------------------------------------
        # STEP 9
        # Console summary.
        # ----------------------------------------------------

        print_summary(
            summary=summary,
            pre_report=pre_report,
            post_report=post_report,
        )

        print("")
        print(
            "[RESULT] POST-RECOVERY GAP ANALYSIS PASSED"
        )

        print(
            "[PASS] 870 OHLCV source files verified."
        )

        print(
            "[PASS] Pre/post recovery reports compared."
        )

        print(
            "[PASS] Recovery performance calculated."
        )

        print(
            "[PASS] Remaining gap jobs identified."
        )

        print(
            "[PASS] Second-pass candidates generated."
        )

        print(
            "[PASS] OHLCV source data was not modified."
        )

        print(
            "[NEXT] Validate analysis outputs before "
            "second-pass recovery planning."
        )

        return 0

    except Exception as exc:

        print("")
        print(
            "============================================================"
        )
        print(
            "POST-RECOVERY GAP ANALYSIS FATAL ERROR"
        )
        print(
            "============================================================"
        )

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print("")
        traceback.print_exc()

        print("")
        print(
            "[SAFETY] No OHLCV deletion was executed."
        )

        print(
            "[SAFETY] No historical gap repair was executed."
        )

        print(
            "[SAFETY] No Git reset/clean was executed."
        )

        write_checkpoint(
            status="FAILED",
        )

        write_status(
            "FAILED",
            (
                f"{type(exc).__name__}: "
                f"{exc}"
            ),
        )

        return 1


# ============================================================
# 22. ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())


# ============================================================
# END
# ============================================================
