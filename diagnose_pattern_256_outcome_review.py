
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Outcome REVIEW Diagnostics
Clean V001

목적:
- 기존 전체시장 검증 결과의 REVIEW 원인 집계
- 144MB 이상의 상세 CSV를 메모리에 적재하지 않고 분석
- 원본 파일 변경 금지
- SHA256, 총 행 수, 분류별 행 수 검증
- 시간봉/종목/평가기간별 진단 보고서 생성

이 스크립트는 매매 및 학습을 승인하지 않습니다.
"""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_outcome_review_diagnostics"

SOURCE_FOLDER = (
    Path("data")
    / "reports"
    / "pattern_256_outcomes_full_market"
)

OUTPUT_FOLDER = (
    Path("data")
    / "reports"
    / "pattern_256_outcome_review_diagnostics"
)

RESULT_NAME = "outcome_verification_result.json"
DETAIL_NAME = "outcome_verification_detail.csv"
MANIFEST_NAME = "outcome_verification_manifest.json"

MAX_SAMPLES_PER_CLASS = 5

CLASS_FIELDS = (
    "classification",
    "verification_classification",
    "comparison_classification",
    "result_classification",
)

TIMEFRAME_FIELDS = ("timeframe", "tf")
MARKET_FIELDS = ("market", "symbol")
HORIZON_FIELDS = ("horizon", "required_bars")
STATUS_FIELDS = ("status", "outcome_status")


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def save_json(path, value):
    with path.open("w", encoding="utf-8") as stream:
        json.dump(
            value,
            stream,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        stream.write("\n")


def find_column(columns, candidates):
    lookup = {
        str(column).strip().lower(): column
        for column in columns
        if column is not None
    }

    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]

    return None


def normalized(value):
    if value is None:
        return ""

    return str(value).strip()


def get_value(row, column, fallback="UNKNOWN"):
    if column is None:
        return fallback

    value = normalized(row.get(column))

    return value if value else fallback


def write_counter_csv(path, headers, rows):
    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)

        for row in rows:
            writer.writerow(row)


def verify_source_files(folder):
    result_path = folder / RESULT_NAME
    detail_path = folder / DETAIL_NAME
    manifest_path = folder / MANIFEST_NAME

    for path in (
        result_path,
        detail_path,
        manifest_path,
    ):
        if not path.is_file():
            raise RuntimeError(
                "Required source file missing: {}".format(path)
            )

    result = load_json(result_path)
    manifest = load_json(manifest_path)

    if result.get("audit") != "pattern_256_outcomes_full_market":
        raise RuntimeError("Unexpected source audit")

    if result.get("execution") != "PASS":
        raise RuntimeError("Source execution is not PASS")

    if result.get("classification") != "REVIEW":
        raise RuntimeError(
            "Expected REVIEW source; received {}".format(
                result.get("classification")
            )
        )

    if result.get("version") != "Clean V002":
        raise RuntimeError("Unexpected source version")

    if manifest.get("version") != result.get("version"):
        raise RuntimeError("Source version mismatch")

    if manifest.get("audit") != result.get("audit"):
        raise RuntimeError("Source audit mismatch")

    if result.get("errors"):
        raise RuntimeError("Source contains execution errors")

    outputs = manifest.get("outputs")

    if not isinstance(outputs, dict):
        raise RuntimeError("Source manifest outputs missing")

    for name in (RESULT_NAME, DETAIL_NAME):
        expected = normalized(outputs.get(name)).lower()

        if len(expected) != 64:
            raise RuntimeError(
                "Missing or invalid source SHA256: {}".format(name)
            )

        actual = sha256_file(folder / name)

        if actual != expected:
            raise RuntimeError(
                "Source SHA256 mismatch: {}".format(name)
            )

        print("[SHA256 PASS] {}".format(name), flush=True)

    expected_rows = int(result.get("rows_checked", 0))

    if expected_rows <= 0:
        raise RuntimeError("Invalid source row count")

    expected_classes = result.get("classification_counts")

    if not isinstance(expected_classes, dict):
        raise RuntimeError("Source classification counts missing")

    if sum(int(v) for v in expected_classes.values()) != expected_rows:
        raise RuntimeError(
            "Source classification totals do not match row count"
        )

    return result, detail_path


def run(root):
    source_folder = root / SOURCE_FOLDER
    output_folder = root / OUTPUT_FOLDER

    print("=== Pattern 256 REVIEW diagnostics ===", flush=True)
    print("Root: {}".format(root), flush=True)

    source_result, detail_path = verify_source_files(
        source_folder
    )

    expected_rows = int(source_result["rows_checked"])

    class_counts = Counter()
    timeframe_counts = Counter()
    timeframe_class_counts = Counter()
    market_class_counts = Counter()
    horizon_class_counts = Counter()
    status_class_counts = Counter()

    samples = defaultdict(list)

    total_rows = 0
    fieldnames = []

    with detail_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        fieldnames = list(reader.fieldnames or [])

        if not fieldnames:
            raise RuntimeError("Detail CSV has no header")

        class_column = find_column(
            fieldnames, CLASS_FIELDS
        )
        timeframe_column = find_column(
            fieldnames, TIMEFRAME_FIELDS
        )
        market_column = find_column(
            fieldnames, MARKET_FIELDS
        )
        horizon_column = find_column(
            fieldnames, HORIZON_FIELDS
        )
        status_column = find_column(
            fieldnames, STATUS_FIELDS
        )

        if class_column is None:
            raise RuntimeError(
                "Classification column not found. Columns: {}".format(
                    fieldnames
                )
            )

        if timeframe_column is None:
            raise RuntimeError(
                "Timeframe column not found. Columns: {}".format(
                    fieldnames
                )
            )

        if market_column is None:
            raise RuntimeError(
                "Market column not found. Columns: {}".format(
                    fieldnames
                )
            )

        for row in reader:
            total_rows += 1

            if None in row:
                raise RuntimeError(
                    "Malformed CSV row at data row {}".format(
                        total_rows
                    )
                )

            classification = get_value(
                row, class_column
            )
            timeframe = get_value(
                row, timeframe_column
            )
            market = get_value(
                row, market_column
            )
            horizon = get_value(
                row, horizon_column
            )
            status = get_value(
                row, status_column
            )

            class_counts[classification] += 1
            timeframe_counts[timeframe] += 1

            timeframe_class_counts[
                (timeframe, classification)
            ] += 1

            market_class_counts[
                (timeframe, market, classification)
            ] += 1

            horizon_class_counts[
                (timeframe, horizon, classification)
            ] += 1

            status_class_counts[
                (status, classification)
            ] += 1

            if (
                classification not in (
                    "BOTH_MATCH",
                    "INCLUDE_MATCH",
                    "EXCLUDE_MATCH",
                )
                and len(samples[classification])
                < MAX_SAMPLES_PER_CLASS
            ):
                samples[classification].append({
                    "row_number": total_rows,
                    "timeframe": timeframe,
                    "market": market,
                    "horizon": horizon,
                    "status": status,
                    "classification": classification,
                    "row": {
                        str(k): normalized(v)
                        for k, v in row.items()
                    },
                })

            if total_rows % 1000000 == 0:
                print(
                    "[PROGRESS] {:,} / {:,}".format(
                        total_rows,
                        expected_rows,
                    ),
                    flush=True,
                )

    if total_rows != expected_rows:
        raise RuntimeError(
            "Row count mismatch: {} != {}".format(
                total_rows,
                expected_rows,
            )
        )

    expected_classes = {
        str(k): int(v)
        for k, v in source_result[
            "classification_counts"
        ].items()
    }

    if dict(class_counts) != expected_classes:
        raise RuntimeError(
            "Classification counts do not match source result"
        )

    output_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    timeframe_rows = sorted(
        (
            tf,
            classification,
            count,
        )
        for (tf, classification), count
        in timeframe_class_counts.items()
    )

    market_rows = sorted(
        (
            tf,
            market,
            classification,
            count,
        )
        for (tf, market, classification), count
        in market_class_counts.items()
    )

    horizon_rows = sorted(
        (
            tf,
            horizon,
            classification,
            count,
        )
        for (tf, horizon, classification), count
        in horizon_class_counts.items()
    )

    status_rows = sorted(
        (
            status,
            classification,
            count,
        )
        for (status, classification), count
        in status_class_counts.items()
    )

    write_counter_csv(
        output_folder / "review_by_timeframe.csv",
        ["timeframe", "classification", "rows"],
        timeframe_rows,
    )

    write_counter_csv(
        output_folder / "review_by_market.csv",
        [
            "timeframe",
            "market",
            "classification",
            "rows",
        ],
        market_rows,
    )

    write_counter_csv(
        output_folder / "review_by_horizon.csv",
        ["timeframe", "horizon", "classification", "rows"],
        horizon_rows,
    )

    write_counter_csv(
        output_folder / "review_by_status.csv",
        ["status", "classification", "rows"],
        status_rows,
    )

    review_classes = (
        "WINDOW_GAP",
        "INCOMPLETE_COMPARISON",
        "ROW_ERROR:TIMESTAMP_EMPTY",
    )

    review_rows = sum(
        class_counts.get(name, 0)
        for name in review_classes
    )

    diagnostic = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "classification": "REVIEW",
        "research_decision": "HOLD",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_audit": source_result["audit"],
        "source_version": source_result["version"],
        "source_detail_sha256": sha256_file(
            detail_path
        ),
        "rows_checked": total_rows,
        "files_processed": source_result[
            "files_processed"
        ],
        "classification_counts": dict(
            sorted(class_counts.items())
        ),
        "timeframe_counts": dict(
            sorted(timeframe_counts.items())
        ),
        "review_rows": review_rows,
        "review_pct": round(
            100.0 * review_rows / total_rows,
            6,
        ),
        "source_columns": fieldnames,
        "samples": dict(samples),
        "limitations": [
            "This is a diagnostic aggregation, not a new backtest",
            "WINDOW_GAP root causes are not established",
            "Matching classifications do not prove profitability",
            "Current OHLCV snapshots are used",
            "Point-in-time availability is not proven",
            "Independent OOS is not proven",
            "Trading and training remain unapproved",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    result_path = (
        output_folder / "review_diagnostics_result.json"
    )

    save_json(result_path, diagnostic)

    output_names = (
        "review_diagnostics_result.json",
        "review_by_timeframe.csv",
        "review_by_market.csv",
        "review_by_horizon.csv",
        "review_by_status.csv",
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            name: sha256_file(output_folder / name)
            for name in output_names
        },
    }

    save_json(
        output_folder / "review_diagnostics_manifest.json",
        manifest,
    )

    print("", flush=True)
    print("=== DIAGNOSTIC RESULT ===", flush=True)
    print("Execution: PASS", flush=True)
    print("Classification: REVIEW", flush=True)
    print("Rows checked: {:,}".format(total_rows), flush=True)
    print("Review rows: {:,}".format(review_rows), flush=True)
    print("Review pct: {:.4f}%".format(
        diagnostic["review_pct"]
    ), flush=True)

    for name, count in sorted(class_counts.items()):
        print(
            "{}: {:,}".format(name, count),
            flush=True,
        )

    print("Research decision: HOLD", flush=True)
    print("Output: {}".format(output_folder), flush=True)

    return 0


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
        help="Persistent local research root",
    )

    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()

    if not root.is_dir():
        raise RuntimeError(
            "Research root not found: {}".format(root)
        )

    return run(root)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[ERROR] {}: {}".format(
                type(exc).__name__,
                exc,
            ),
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
