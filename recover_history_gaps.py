"""
Upbit Surge Monitor - Historical Gap Recovery Clean V001
=========================================================

File:
    recover_history_gaps.py

Purpose:
    Historical OHLCV gap recovery를 수행하기 전에
    현재 Gap Report 결과를 READ ONLY로 분석한다.

Clean V001은 DRY-RUN 전용이다.

IMPORTANT:
    - OHLCV 수정 금지
    - OHLCV 삭제 금지
    - Feature 수정 금지
    - Signal 수정 금지
    - 실제 Upbit API 복구 금지
    - Prediction 금지
    - Trading 금지
    - git reset 금지
    - git clean 금지
    - git commit 금지
    - git push 금지

Clean V001 responsibilities:
    1. 전체 KRW OHLCV 시장 파일 확인
    2. Gap Report 결과 자동 탐색
    3. Gap Report CSV 구조 확인
    4. 시장 / 타임프레임별 Gap 통계 생성
    5. 복구 후보 목록 생성
    6. 비정상/과도한 Gap 별도 표시
    7. Resume / Checkpoint 지원
    8. Source OHLCV SHA256 변경 여부 검증
    9. DRY-RUN 결과만 runtime 파일로 저장

This version NEVER repairs OHLCV.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd


# ============================================================
# 1. VERSION / PROJECT
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Historical Gap Recovery Clean V001"

TIMEFRAMES: Tuple[str, ...] = ("h1", "h4", "d1")

EXPECTED_MARKET_COUNT = 290
EXPECTED_JOB_COUNT = EXPECTED_MARKET_COUNT * len(TIMEFRAMES)

DRY_RUN_ONLY = True

# Gap이 너무 큰 경우 실제 복구 단계 전에 사람이 확인할 수 있도록
# HIGH / EXTREME으로 분류한다.
HIGH_GAP_THRESHOLD = 100
EXTREME_GAP_THRESHOLD = 1000


# ============================================================
# 2. PATHS
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent

DATA_DIR = ROOT_DIR / "data"

OHLCV_DIR = DATA_DIR / "ohlcv"

RUNTIME_DIR = DATA_DIR / "recovery"
CHECKPOINT_DIR = RUNTIME_DIR / "checkpoint"
REPORT_DIR = RUNTIME_DIR / "reports"

CHECKPOINT_FILE = CHECKPOINT_DIR / "recover_history_gaps_checkpoint.json"
STATUS_FILE = DATA_DIR / "recover_history_gaps_status.csv"

RECOVERY_PLAN_FILE = REPORT_DIR / "historical_gap_recovery_plan.csv"
SUMMARY_FILE = REPORT_DIR / "historical_gap_recovery_summary.csv"
META_FILE = REPORT_DIR / "historical_gap_recovery_meta.json"

# 이전 단계에서 생성될 가능성이 있는 Gap Report 위치를
# 하나로 강제하지 않고 안전하게 탐색한다.
GAP_REPORT_SEARCH_DIRS: Tuple[Path, ...] = (
    DATA_DIR / "gap_report",
    DATA_DIR / "gap_reports",
    DATA_DIR / "history_gap_report",
    DATA_DIR / "history_gap_reports",
    DATA_DIR / "audit",
    DATA_DIR / "reports",
    DATA_DIR,
)


# ============================================================
# 3. POSSIBLE GAP REPORT COLUMN NAMES
# ============================================================

MARKET_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "market",
    "ticker",
    "symbol",
    "code",
)

TIMEFRAME_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "timeframe",
    "interval",
    "tf",
)

GAP_COUNT_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "missing_count",
    "gap_count",
    "missing_est",
    "missing_estimate",
    "missing",
    "count",
)

GAP_START_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "gap_start",
    "start",
    "start_time",
    "start_timestamp",
    "missing_start",
    "from_time",
)

GAP_END_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "gap_end",
    "end",
    "end_time",
    "end_timestamp",
    "missing_end",
    "to_time",
)

STATUS_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "status",
    "gap_status",
    "classification",
    "result",
)

FILE_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "file",
    "file_path",
    "path",
    "source_file",
)


# ============================================================
# 4. DATA CLASSES
# ============================================================

@dataclass(frozen=True)
class JobKey:
    timeframe: str
    market: str

    @property
    def key(self) -> str:
        return f"{self.timeframe}|{self.market}"


@dataclass
class GapRecord:
    market: str
    timeframe: str
    gap_count: int
    gap_start: str
    gap_end: str
    source_report: str
    source_row: int
    original_status: str = ""
    source_file: str = ""


# ============================================================
# 5. BASIC HELPERS
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_print(text: str = "") -> None:
    """
    Windows cp949 콘솔에서도 UnicodeEncodeError 때문에
    전체 프로그램이 죽지 않도록 안전하게 출력한다.
    """

    value = str(text)

    try:
        print(value, flush=True)
        return
    except UnicodeEncodeError:
        pass

    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"

    try:
        safe_value = value.encode(
            encoding,
            errors="replace",
        ).decode(
            encoding,
            errors="replace",
        )
        print(safe_value, flush=True)
    except Exception:
        fallback = value.encode(
            "ascii",
            errors="replace",
        ).decode("ascii")
        print(fallback, flush=True)


def separator(char: str = "=", length: int = 68) -> str:
    return char * length


def normalize_column_name(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[\s\-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text


def normalize_market(value: Any) -> str:
    text = str(value).strip().upper()

    if not text:
        return ""

    text = text.replace("_", "-")

    if text.startswith("KRW-"):
        return text

    if text.startswith("KRW") and "-" not in text:
        remainder = text[3:].lstrip("-")
        if remainder:
            return f"KRW-{remainder}"

    return text


def normalize_timeframe(value: Any) -> str:
    text = str(value).strip().lower()

    mapping = {
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
    }

    return mapping.get(text, text)


def parse_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if pd.isna(value):
            return default
        return int(value)

    text = str(value).strip()

    if not text:
        return default

    text = text.replace(",", "")

    try:
        return int(float(text))
    except Exception:
        return default


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    return str(value).strip()


def ensure_runtime_directories() -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 6. SHA256
# ============================================================

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def collect_ohlcv_hashes(
    jobs: Sequence[JobKey],
) -> Dict[str, str]:

    result: Dict[str, str] = {}

    total = len(jobs)

    safe_print()
    safe_print(separator())
    safe_print("RECORD OHLCV SHA256")
    safe_print(separator())

    for index, job in enumerate(jobs, start=1):
        path = OHLCV_DIR / job.timeframe / f"{job.market}.csv"

        if not path.is_file():
            raise FileNotFoundError(
                f"OHLCV file missing: {path}"
            )

        result[job.key] = sha256_file(path)

        if (
            index == 1
            or index % 100 == 0
            or index == total
        ):
            safe_print(
                f"SHA256 progress : {index:,} / {total:,}"
            )

    return result


# ============================================================
# 7. MARKET / JOB DISCOVERY
# ============================================================

def discover_markets() -> List[str]:
    market_sets: Dict[str, Set[str]] = {}

    for timeframe in TIMEFRAMES:
        directory = OHLCV_DIR / timeframe

        if not directory.is_dir():
            raise FileNotFoundError(
                f"OHLCV directory missing: {directory}"
            )

        markets: Set[str] = set()

        for path in directory.glob("KRW-*.csv"):
            if path.is_file():
                markets.add(path.stem.upper())

        market_sets[timeframe] = markets

    if not market_sets:
        raise RuntimeError("No OHLCV markets discovered.")

    base = market_sets[TIMEFRAMES[0]]

    for timeframe in TIMEFRAMES[1:]:
        current = market_sets[timeframe]

        if current != base:
            missing = sorted(base - current)
            extra = sorted(current - base)

            message = [
                f"Market file set mismatch: {timeframe}",
                f"Missing count: {len(missing)}",
                f"Extra count  : {len(extra)}",
            ]

            if missing:
                message.append(
                    "Missing sample: "
                    + ", ".join(missing[:10])
                )

            if extra:
                message.append(
                    "Extra sample: "
                    + ", ".join(extra[:10])
                )

            raise RuntimeError("\n".join(message))

    markets = sorted(base)

    if not markets:
        raise RuntimeError("No KRW market CSV files found.")

    return markets


def build_jobs(markets: Sequence[str]) -> List[JobKey]:
    jobs: List[JobKey] = []

    for timeframe in TIMEFRAMES:
        for market in markets:
            jobs.append(
                JobKey(
                    timeframe=timeframe,
                    market=market,
                )
            )

    return jobs


# ============================================================
# 8. GAP REPORT DISCOVERY
# ============================================================

def is_generated_recovery_file(path: Path) -> bool:
    generated_names = {
        STATUS_FILE.name.lower(),
        RECOVERY_PLAN_FILE.name.lower(),
        SUMMARY_FILE.name.lower(),
    }

    if path.name.lower() in generated_names:
        return True

    try:
        path.resolve().relative_to(RUNTIME_DIR.resolve())
        return True
    except Exception:
        return False


def score_gap_report_candidate(path: Path) -> int:
    """
    파일명만 보고 우선순위를 정한다.

    history + gap + report 조합을 가장 우선한다.
    """

    name = path.name.lower()

    score = 0

    if "gap" in name:
        score += 100

    if "history" in name:
        score += 40

    if "report" in name:
        score += 30

    if "audit" in name:
        score += 10

    if "summary" in name:
        score -= 30

    if "status" in name:
        score -= 40

    if "checkpoint" in name:
        score -= 100

    return score


def discover_gap_report_files() -> List[Path]:
    candidates: Dict[str, Path] = {}

    for directory in GAP_REPORT_SEARCH_DIRS:
        if not directory.exists():
            continue

        if directory == DATA_DIR:
            iterator: Iterable[Path] = directory.glob("*.csv")
        else:
            iterator = directory.rglob("*.csv")

        for path in iterator:
            if not path.is_file():
                continue

            if is_generated_recovery_file(path):
                continue

            name = path.name.lower()

            # 너무 넓게 아무 CSV나 읽지 않는다.
            if not (
                "gap" in name
                or "history" in name
                or "audit" in name
            ):
                continue

            candidates[str(path.resolve())] = path

    ordered = sorted(
        candidates.values(),
        key=lambda item: (
            -score_gap_report_candidate(item),
            str(item).lower(),
        ),
    )

    return ordered


# ============================================================
# 9. CSV READING
# ============================================================

def read_csv_robust(path: Path) -> pd.DataFrame:
    attempts = (
        {"encoding": "utf-8-sig"},
        {"encoding": "utf-8"},
        {"encoding": "cp949"},
    )

    last_error: Optional[Exception] = None

    for kwargs in attempts:
        try:
            return pd.read_csv(
                path,
                low_memory=False,
                **kwargs,
            )
        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        f"Unable to read CSV: {path}\n"
        f"Last error: {last_error}"
    )


def find_column(
    columns: Sequence[str],
    candidates: Sequence[str],
) -> Optional[str]:

    normalized_map = {
        normalize_column_name(column): column
        for column in columns
    }

    for candidate in candidates:
        normalized_candidate = normalize_column_name(candidate)

        if normalized_candidate in normalized_map:
            return normalized_map[normalized_candidate]

    return None


def infer_market_timeframe_from_path(
    path: Path,
) -> Tuple[str, str]:

    market = ""
    timeframe = ""

    stem_upper = path.stem.upper()

    match = re.search(r"(KRW-[A-Z0-9]+)", stem_upper)

    if match:
        market = match.group(1)

    parts = [
        part.lower()
        for part in path.parts
    ]

    for part in reversed(parts):
        normalized = normalize_timeframe(part)

        if normalized in TIMEFRAMES:
            timeframe = normalized
            break

    return market, timeframe


# ============================================================
# 10. GAP REPORT PARSER
# ============================================================

def parse_gap_report(
    path: Path,
) -> List[GapRecord]:

    df = read_csv_robust(path)

    if df.empty:
        return []

    columns = list(df.columns)

    market_col = find_column(
        columns,
        MARKET_COLUMN_CANDIDATES,
    )

    timeframe_col = find_column(
        columns,
        TIMEFRAME_COLUMN_CANDIDATES,
    )

    gap_count_col = find_column(
        columns,
        GAP_COUNT_COLUMN_CANDIDATES,
    )

    gap_start_col = find_column(
        columns,
        GAP_START_COLUMN_CANDIDATES,
    )

    gap_end_col = find_column(
        columns,
        GAP_END_COLUMN_CANDIDATES,
    )

    status_col = find_column(
        columns,
        STATUS_COLUMN_CANDIDATES,
    )

    file_col = find_column(
        columns,
        FILE_COLUMN_CANDIDATES,
    )

    inferred_market, inferred_timeframe = (
        infer_market_timeframe_from_path(path)
    )

    # 최소한 market / timeframe을 얻을 수 없는 CSV는
    # Gap detail source로 사용하지 않는다.
    if market_col is None and not inferred_market:
        return []

    if timeframe_col is None and not inferred_timeframe:
        return []

    # gap count 또는 gap 시작/종료 정보 중 하나는 있어야 한다.
    if (
        gap_count_col is None
        and gap_start_col is None
        and gap_end_col is None
    ):
        return []

    records: List[GapRecord] = []

    for row_number, (_, row) in enumerate(
        df.iterrows(),
        start=2,
    ):
        if market_col is not None:
            market = normalize_market(row.get(market_col))
        else:
            market = inferred_market

        if timeframe_col is not None:
            timeframe = normalize_timeframe(
                row.get(timeframe_col)
            )
        else:
            timeframe = inferred_timeframe

        if not market.startswith("KRW-"):
            continue

        if timeframe not in TIMEFRAMES:
            continue

        gap_count = 0

        if gap_count_col is not None:
            gap_count = parse_int(
                row.get(gap_count_col),
                default=0,
            )

        gap_start = (
            clean_text(row.get(gap_start_col))
            if gap_start_col is not None
            else ""
        )

        gap_end = (
            clean_text(row.get(gap_end_col))
            if gap_end_col is not None
            else ""
        )

        original_status = (
            clean_text(row.get(status_col))
            if status_col is not None
            else ""
        )

        source_file = (
            clean_text(row.get(file_col))
            if file_col is not None
            else ""
        )

        # count가 0이더라도 start/end가 있다면
        # 실제 gap event일 수 있으므로 1개 이상의 후보로 취급한다.
        if gap_count <= 0 and (gap_start or gap_end):
            gap_count = 1

        if gap_count <= 0:
            continue

        records.append(
            GapRecord(
                market=market,
                timeframe=timeframe,
                gap_count=gap_count,
                gap_start=gap_start,
                gap_end=gap_end,
                source_report=str(path),
                source_row=row_number,
                original_status=original_status,
                source_file=source_file,
            )
        )

    return records


def load_all_gap_records(
    report_files: Sequence[Path],
) -> Tuple[List[GapRecord], List[Path]]:

    all_records: List[GapRecord] = []
    accepted_files: List[Path] = []

    safe_print()
    safe_print(separator())
    safe_print("DISCOVER GAP REPORT DATA")
    safe_print(separator())

    if not report_files:
        safe_print("[WARN] No candidate Gap Report CSV files found.")
        return [], []

    for path in report_files:
        try:
            records = parse_gap_report(path)
        except Exception as exc:
            safe_print(
                f"[SKIP] {path} : {exc}"
            )
            continue

        if not records:
            continue

        accepted_files.append(path)
        all_records.extend(records)

        safe_print(
            f"[READ] {path}"
        )
        safe_print(
            f"       Gap rows : {len(records):,}"
        )

    return all_records, accepted_files


# ============================================================
# 11. DEDUPLICATION
# ============================================================

def gap_identity(record: GapRecord) -> Tuple[Any, ...]:
    return (
        record.market,
        record.timeframe,
        record.gap_start,
        record.gap_end,
        record.gap_count,
    )


def deduplicate_gap_records(
    records: Sequence[GapRecord],
) -> List[GapRecord]:

    seen: Set[Tuple[Any, ...]] = set()
    result: List[GapRecord] = []

    for record in records:
        identity = gap_identity(record)

        if identity in seen:
            continue

        seen.add(identity)
        result.append(record)

    result.sort(
        key=lambda item: (
            item.timeframe,
            item.market,
            item.gap_start,
            item.gap_end,
            item.gap_count,
        )
    )

    return result


# ============================================================
# 12. CLASSIFICATION
# ============================================================

def classify_gap(record: GapRecord) -> Tuple[str, str]:
    """
    Clean V001에서는 API 확인을 하지 않는다.

    따라서 실제로 "복구 가능"이라고 확정하지 않고
    복구 검토 대상인지 위험도가 높은지만 분류한다.
    """

    count = record.gap_count

    if count >= EXTREME_GAP_THRESHOLD:
        return (
            "REVIEW_EXTREME",
            "Very large gap. Manual/API verification required before recovery.",
        )

    if count >= HIGH_GAP_THRESHOLD:
        return (
            "REVIEW_HIGH",
            "Large gap. API verification required before recovery.",
        )

    return (
        "RECOVERY_CANDIDATE",
        "Candidate only. API existence check required in next stage.",
    )


# ============================================================
# 13. CHECKPOINT
# ============================================================

def load_checkpoint() -> Dict[str, Any]:
    if not CHECKPOINT_FILE.is_file():
        return {
            "version": VERSION,
            "completed_jobs": [],
            "updated_at_utc": "",
        }

    try:
        with CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8",
        ) as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            raise ValueError(
                "Checkpoint root must be an object."
            )

        completed = data.get(
            "completed_jobs",
            [],
        )

        if not isinstance(completed, list):
            completed = []

        return {
            "version": VERSION,
            "completed_jobs": completed,
            "updated_at_utc": clean_text(
                data.get("updated_at_utc")
            ),
        }

    except Exception as exc:
        raise RuntimeError(
            f"Checkpoint read failed: {CHECKPOINT_FILE}\n"
            f"{exc}"
        )


def save_checkpoint(
    completed_jobs: Set[str],
) -> None:

    payload = {
        "version": VERSION,
        "dry_run_only": DRY_RUN_ONLY,
        "completed_jobs": sorted(completed_jobs),
        "completed_count": len(completed_jobs),
        "updated_at_utc": utc_now_iso(),
    }

    temp_file = CHECKPOINT_FILE.with_suffix(
        ".json.tmp"
    )

    with temp_file.open(
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
        temp_file,
        CHECKPOINT_FILE,
    )


# ============================================================
# 14. STATUS WRITER
# ============================================================

STATUS_FIELDS = (
    "utc_time",
    "version",
    "mode",
    "job_index",
    "job_total",
    "timeframe",
    "market",
    "gap_events",
    "missing_estimate",
    "recovery_candidates",
    "review_high",
    "review_extreme",
    "status",
)


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
        )

        if not exists:
            writer.writeheader()

        writer.writerow(
            {
                field: row.get(field, "")
                for field in STATUS_FIELDS
            }
        )


# ============================================================
# 15. BUILD INDEX
# ============================================================

def build_gap_index(
    records: Sequence[GapRecord],
) -> Dict[str, List[GapRecord]]:

    result: Dict[str, List[GapRecord]] = {}

    for record in records:
        key = f"{record.timeframe}|{record.market}"

        result.setdefault(
            key,
            [],
        ).append(record)

    return result


# ============================================================
# 16. WRITE RECOVERY PLAN
# ============================================================

PLAN_FIELDS = (
    "timeframe",
    "market",
    "gap_count",
    "gap_start",
    "gap_end",
    "classification",
    "reason",
    "original_status",
    "source_file",
    "source_report",
    "source_row",
    "dry_run",
)


def write_recovery_plan(
    records: Sequence[GapRecord],
) -> None:

    temp_file = RECOVERY_PLAN_FILE.with_suffix(
        ".csv.tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=PLAN_FIELDS,
        )

        writer.writeheader()

        for record in records:
            classification, reason = classify_gap(
                record
            )

            writer.writerow(
                {
                    "timeframe": record.timeframe,
                    "market": record.market,
                    "gap_count": record.gap_count,
                    "gap_start": record.gap_start,
                    "gap_end": record.gap_end,
                    "classification": classification,
                    "reason": reason,
                    "original_status": record.original_status,
                    "source_file": record.source_file,
                    "source_report": record.source_report,
                    "source_row": record.source_row,
                    "dry_run": "YES",
                }
            )

    os.replace(
        temp_file,
        RECOVERY_PLAN_FILE,
    )


# ============================================================
# 17. WRITE SUMMARY
# ============================================================

SUMMARY_FIELDS = (
    "timeframe",
    "market",
    "gap_events",
    "missing_estimate",
    "recovery_candidates",
    "review_high",
    "review_extreme",
)


def write_summary(
    rows: Sequence[Dict[str, Any]],
) -> None:

    temp_file = SUMMARY_FILE.with_suffix(
        ".csv.tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=SUMMARY_FIELDS,
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    field: row.get(field, "")
                    for field in SUMMARY_FIELDS
                }
            )

    os.replace(
        temp_file,
        SUMMARY_FILE,
    )


# ============================================================
# 18. WRITE META
# ============================================================

def write_meta(
    *,
    markets: Sequence[str],
    jobs: Sequence[JobKey],
    report_files: Sequence[Path],
    records: Sequence[GapRecord],
    completed_jobs: Set[str],
    summary_rows: Sequence[Dict[str, Any]],
) -> None:

    total_missing = sum(
        record.gap_count
        for record in records
    )

    classifications = {
        "RECOVERY_CANDIDATE": 0,
        "REVIEW_HIGH": 0,
        "REVIEW_EXTREME": 0,
    }

    for record in records:
        classification, _ = classify_gap(record)

        classifications[classification] = (
            classifications.get(
                classification,
                0,
            )
            + 1
        )

    payload = {
        "project": PROJECT_NAME,
        "version": VERSION,
        "mode": "DRY-RUN",
        "dry_run_only": True,
        "generated_at_utc": utc_now_iso(),
        "markets": len(markets),
        "timeframes": list(TIMEFRAMES),
        "jobs": len(jobs),
        "completed_jobs": len(completed_jobs),
        "gap_report_files": [
            str(path)
            for path in report_files
        ],
        "gap_events": len(records),
        "missing_estimate": total_missing,
        "classifications": classifications,
        "summary_rows": len(summary_rows),
        "safety": {
            "ohlcv_write": "DISABLED",
            "ohlcv_delete": "DISABLED",
            "gap_repair": "DISABLED",
            "api_recovery": "DISABLED",
            "feature_build": "DISABLED",
            "256_detector": "DISABLED",
            "future_labels": "DISABLED",
            "prediction": "DISABLED",
            "trading": "DISABLED",
            "git_reset": "DISABLED",
            "git_clean": "DISABLED",
            "git_commit": "DISABLED",
            "git_push": "DISABLED",
        },
    }

    temp_file = META_FILE.with_suffix(
        ".json.tmp"
    )

    with temp_file.open(
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
        temp_file,
        META_FILE,
    )


# ============================================================
# 19. JOB ANALYSIS
# ============================================================

def analyze_job(
    job: JobKey,
    records: Sequence[GapRecord],
) -> Dict[str, Any]:

    gap_events = len(records)

    missing_estimate = sum(
        record.gap_count
        for record in records
    )

    recovery_candidates = 0
    review_high = 0
    review_extreme = 0

    for record in records:
        classification, _ = classify_gap(record)

        if classification == "RECOVERY_CANDIDATE":
            recovery_candidates += 1

        elif classification == "REVIEW_HIGH":
            review_high += 1

        elif classification == "REVIEW_EXTREME":
            review_extreme += 1

    return {
        "timeframe": job.timeframe,
        "market": job.market,
        "gap_events": gap_events,
        "missing_estimate": missing_estimate,
        "recovery_candidates": recovery_candidates,
        "review_high": review_high,
        "review_extreme": review_extreme,
    }


# ============================================================
# 20. MAIN DRY-RUN
# ============================================================

def run_dry_run(
    *,
    reset_checkpoint: bool,
) -> int:

    start_time = time.time()

    ensure_runtime_directories()

    safe_print(separator())
    safe_print("HISTORICAL GAP RECOVERY - CLEAN V001")
    safe_print(separator())
    safe_print(f"Project        : {PROJECT_NAME}")
    safe_print(f"Version        : {VERSION}")
    safe_print("Execution mode : DRY-RUN ONLY")
    safe_print("OHLCV write    : DISABLED")
    safe_print("Gap repair     : DISABLED")
    safe_print("API recovery   : DISABLED")
    safe_print("Trading        : DISABLED")
    safe_print(separator())

    # --------------------------------------------------------
    # Discover markets
    # --------------------------------------------------------

    markets = discover_markets()
    jobs = build_jobs(markets)

    safe_print()
    safe_print("MARKET DISCOVERY")
    safe_print(separator("-"))
    safe_print(f"Markets        : {len(markets):,}")
    safe_print(f"Timeframes     : {len(TIMEFRAMES):,}")
    safe_print(f"Total jobs     : {len(jobs):,}")

    if len(markets) != EXPECTED_MARKET_COUNT:
        safe_print(
            f"[WARN] Expected {EXPECTED_MARKET_COUNT:,} markets, "
            f"found {len(markets):,}."
        )

    if len(jobs) != EXPECTED_JOB_COUNT:
        safe_print(
            f"[WARN] Expected {EXPECTED_JOB_COUNT:,} jobs, "
            f"found {len(jobs):,}."
        )

    # --------------------------------------------------------
    # SHA before
    # --------------------------------------------------------

    hashes_before = collect_ohlcv_hashes(jobs)

    # --------------------------------------------------------
    # Discover Gap Report
    # --------------------------------------------------------

    candidates = discover_gap_report_files()

    records, accepted_report_files = (
        load_all_gap_records(candidates)
    )

    records = deduplicate_gap_records(records)

    safe_print()
    safe_print("GAP REPORT DISCOVERY RESULT")
    safe_print(separator("-"))
    safe_print(
        f"Accepted report files : "
        f"{len(accepted_report_files):,}"
    )
    safe_print(
        f"Unique gap events      : "
        f"{len(records):,}"
    )
    safe_print(
        f"Missing estimate       : "
        f"{sum(r.gap_count for r in records):,}"
    )

    if not accepted_report_files:
        safe_print()
        safe_print(
            "[WARN] No compatible detailed Gap Report CSV was found."
        )
        safe_print(
            "[WARN] OHLCV will NOT be modified."
        )
        safe_print(
            "[WARN] Recovery plan will contain no gap events."
        )

    gap_index = build_gap_index(records)

    # --------------------------------------------------------
    # Checkpoint
    # --------------------------------------------------------

    if reset_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

        safe_print()
        safe_print(
            "[INFO] Recovery dry-run checkpoint reset."
        )

    checkpoint = load_checkpoint()

    completed_jobs: Set[str] = set(
        clean_text(value)
        for value in checkpoint.get(
            "completed_jobs",
            [],
        )
        if clean_text(value)
    )

    # 현재 존재하지 않는 과거 job key는 제거한다.
    valid_job_keys = {
        job.key
        for job in jobs
    }

    completed_jobs.intersection_update(
        valid_job_keys
    )

    safe_print()
    safe_print(separator())
    safe_print("RESUME / CHECKPOINT")
    safe_print(separator())
    safe_print(
        f"Checkpoint file : {CHECKPOINT_FILE}"
    )
    safe_print(
        f"Completed jobs  : {len(completed_jobs):,}"
    )
    safe_print(
        f"Remaining jobs  : "
        f"{len(jobs) - len(completed_jobs):,}"
    )

    # --------------------------------------------------------
    # Analyze all jobs
    # --------------------------------------------------------

    summary_rows: List[Dict[str, Any]] = []

    for job_index, job in enumerate(
        jobs,
        start=1,
    ):
        job_records = gap_index.get(
            job.key,
            [],
        )

        result = analyze_job(
            job,
            job_records,
        )

        summary_rows.append(result)

        if job.key in completed_jobs:
            if (
                job_index == 1
                or job_index % 100 == 0
                or job_index == len(jobs)
            ):
                safe_print(
                    f"[RESUME {job_index:,}/{len(jobs):,}] "
                    f"{job.timeframe.upper()} "
                    f"{job.market} : already completed"
                )

            continue

        safe_print()
        safe_print(
            f"[JOB {job_index:,}/{len(jobs):,}] "
            f"{job.timeframe.upper()} {job.market}"
        )
        safe_print(
            f"  Gap events       : "
            f"{result['gap_events']:,}"
        )
        safe_print(
            f"  Missing estimate : "
            f"{result['missing_estimate']:,}"
        )
        safe_print(
            f"  Candidates       : "
            f"{result['recovery_candidates']:,}"
        )
        safe_print(
            f"  Review high      : "
            f"{result['review_high']:,}"
        )
        safe_print(
            f"  Review extreme   : "
            f"{result['review_extreme']:,}"
        )

        append_status(
            {
                "utc_time": utc_now_iso(),
                "version": VERSION,
                "mode": "DRY-RUN",
                "job_index": job_index,
                "job_total": len(jobs),
                "timeframe": job.timeframe,
                "market": job.market,
                "gap_events": result[
                    "gap_events"
                ],
                "missing_estimate": result[
                    "missing_estimate"
                ],
                "recovery_candidates": result[
                    "recovery_candidates"
                ],
                "review_high": result[
                    "review_high"
                ],
                "review_extreme": result[
                    "review_extreme"
                ],
                "status": "PASSED",
            }
        )

        completed_jobs.add(job.key)

        # 매 job마다 checkpoint 저장.
        # PC 종료 / Runner 중단 시 최대 1 job만 재검사한다.
        save_checkpoint(completed_jobs)

    # --------------------------------------------------------
    # Write dry-run reports
    # --------------------------------------------------------

    write_recovery_plan(records)
    write_summary(summary_rows)

    write_meta(
        markets=markets,
        jobs=jobs,
        report_files=accepted_report_files,
        records=records,
        completed_jobs=completed_jobs,
        summary_rows=summary_rows,
    )

    # --------------------------------------------------------
    # SHA after
    # --------------------------------------------------------

    hashes_after = collect_ohlcv_hashes(jobs)

    changed_files: List[str] = []

    for key, before_hash in hashes_before.items():
        after_hash = hashes_after.get(key)

        if after_hash != before_hash:
            changed_files.append(key)

    if changed_files:
        safe_print()
        safe_print(separator())
        safe_print("FATAL SAFETY ERROR")
        safe_print(separator())
        safe_print(
            "OHLCV source data changed during DRY-RUN."
        )

        for key in changed_files[:20]:
            safe_print(
                f"[CHANGED] {key}"
            )

        if len(changed_files) > 20:
            safe_print(
                f"... additional changed files: "
                f"{len(changed_files) - 20:,}"
            )

        return 1

    # --------------------------------------------------------
    # Final statistics
    # --------------------------------------------------------

    total_gap_events = len(records)

    total_missing = sum(
        record.gap_count
        for record in records
    )

    recovery_candidate_events = 0
    review_high_events = 0
    review_extreme_events = 0

    for record in records:
        classification, _ = classify_gap(record)

        if classification == "RECOVERY_CANDIDATE":
            recovery_candidate_events += 1

        elif classification == "REVIEW_HIGH":
            review_high_events += 1

        elif classification == "REVIEW_EXTREME":
            review_extreme_events += 1

    markets_with_gaps = {
        record.market
        for record in records
    }

    jobs_with_gaps = {
        f"{record.timeframe}|{record.market}"
        for record in records
    }

    elapsed = time.time() - start_time

    # --------------------------------------------------------
    # Final summary
    # --------------------------------------------------------

    safe_print()
    safe_print(separator())
    safe_print("HISTORICAL GAP RECOVERY DRY-RUN SUMMARY")
    safe_print(separator())
    safe_print(f"Project           : {PROJECT_NAME}")
    safe_print(f"Version           : {VERSION}")
    safe_print("Execution mode    : DRY-RUN ONLY")
    safe_print(
        f"Markets           : {len(markets):,}"
    )
    safe_print(
        f"Timeframes        : {len(TIMEFRAMES):,}"
    )
    safe_print(
        f"Total jobs        : {len(jobs):,}"
    )
    safe_print(
        f"Completed jobs    : {len(completed_jobs):,}"
    )

    safe_print()
    safe_print("Gap Report:")
    safe_print(
        f"  Source files    : "
        f"{len(accepted_report_files):,}"
    )
    safe_print(
        f"  Markets w/gaps  : "
        f"{len(markets_with_gaps):,}"
    )
    safe_print(
        f"  Jobs w/gaps     : "
        f"{len(jobs_with_gaps):,}"
    )
    safe_print(
        f"  Gap events      : "
        f"{total_gap_events:,}"
    )
    safe_print(
        f"  Missing est.    : "
        f"{total_missing:,}"
    )

    safe_print()
    safe_print("Classification:")
    safe_print(
        f"  Candidate       : "
        f"{recovery_candidate_events:,}"
    )
    safe_print(
        f"  Review high     : "
        f"{review_high_events:,}"
    )
    safe_print(
        f"  Review extreme  : "
        f"{review_extreme_events:,}"
    )

    safe_print()
    safe_print("Output:")
    safe_print(
        f"  Recovery plan   : {RECOVERY_PLAN_FILE}"
    )
    safe_print(
        f"  Summary         : {SUMMARY_FILE}"
    )
    safe_print(
        f"  Meta            : {META_FILE}"
    )
    safe_print(
        f"  Checkpoint      : {CHECKPOINT_FILE}"
    )
    safe_print(
        f"  Status CSV      : {STATUS_FILE}"
    )

    safe_print()
    safe_print("Integrity:")
    safe_print("  OHLCV SHA256    : UNCHANGED")
    safe_print("  OHLCV write     : DISABLED")
    safe_print("  OHLCV delete    : DISABLED")

    safe_print()
    safe_print("Safety:")
    safe_print("  Gap repair      : DISABLED")
    safe_print("  API recovery    : DISABLED")
    safe_print("  Feature build   : DISABLED")
    safe_print("  256 Detector    : DISABLED")
    safe_print("  Future labels   : DISABLED")
    safe_print("  Prediction      : DISABLED")
    safe_print("  Trading         : DISABLED")
    safe_print("  Git reset       : DISABLED")
    safe_print("  Git clean       : DISABLED")
    safe_print("  Git commit      : DISABLED")
    safe_print("  Git push        : DISABLED")

    safe_print()
    safe_print(
        f"Elapsed total     : {elapsed:.2f}s"
    )

    safe_print(separator())
    safe_print(
        "[RESULT] HISTORICAL GAP RECOVERY DRY-RUN PASSED"
    )
    safe_print(
        f"[PASS] {len(completed_jobs):,} / "
        f"{len(jobs):,} jobs analyzed."
    )
    safe_print(
        "[PASS] Original OHLCV data remained unchanged."
    )
    safe_print(
        "[PASS] Resume/checkpoint state preserved."
    )
    safe_print(
        "[PASS] No historical data was repaired."
    )

    if total_gap_events > 0:
        safe_print(
            "[NEXT] Validate recovery candidates before "
            "enabling historical repair."
        )
    else:
        safe_print(
            "[NEXT] Review Gap Report source compatibility "
            "before historical repair."
        )

    safe_print(separator())

    return 0


# ============================================================
# 21. ARGUMENTS
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upbit Surge Monitor Historical Gap Recovery "
            "Clean V001 - DRY-RUN only"
        )
    )

    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help=(
            "Reset only this DRY-RUN checkpoint before analysis. "
            "OHLCV data is never deleted."
        ),
    )

    return parser.parse_args()


# ============================================================
# 22. ENTRY POINT
# ============================================================

def main() -> int:
    args = parse_args()

    try:
        return run_dry_run(
            reset_checkpoint=args.reset_checkpoint,
        )

    except KeyboardInterrupt:
        safe_print()
        safe_print(separator())
        safe_print("HISTORICAL GAP RECOVERY INTERRUPTED")
        safe_print(separator())
        safe_print(
            "[RESUME] Completed checkpoint jobs remain available."
        )
        safe_print(
            "[SAFETY] OHLCV automatic repair was not executed."
        )
        safe_print(separator())

        return 130

    except Exception as exc:
        safe_print()
        safe_print(separator())
        safe_print("HISTORICAL GAP RECOVERY FATAL ERROR")
        safe_print(separator())
        safe_print(
            f"{type(exc).__name__}: {exc}"
        )
        safe_print()
        safe_print(
            traceback.format_exc()
        )
        safe_print(
            "[SAFETY] OHLCV automatic repair was not executed."
        )
        safe_print(
            "[SAFETY] No automatic cleanup was executed."
        )
        safe_print(
            "[RESUME] Successfully completed checkpoint jobs "
            "remain available."
        )
        safe_print(separator())

        return 1


if __name__ == "__main__":
    raise SystemExit(main())
