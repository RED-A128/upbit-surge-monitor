
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 OHLCV Gap Audit
Clean V001

- h1/h4/d1 OHLCV 원본 시간 간격 전수조사
- REVIEW 시장별 집계와 비교
- 원본 데이터 변경 금지
- SHA256 기반 체크포인트 검증 및 Resume
- 소용량 CSV/JSON 보고서 생성
- 학습 및 실거래 승인 금지
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_ohlcv_gap_audit"

TIMEFRAMES = {
    "h1": 3600000,
    "h4": 14400000,
    "d1": 86400000,
}

DIAG_REL = Path(
    "data/reports/pattern_256_outcome_review_diagnostics"
)
OUT_REL = Path(
    "data/reports/pattern_256_ohlcv_gap_audit"
)

REQUIRED_DIAG = (
    "review_diagnostics_result.json",
    "review_by_market.csv",
    "review_by_timeframe.csv",
)

REPORT_FILES = (
    "ohlcv_gap_result.json",
    "ohlcv_gap_by_market.csv",
    "ohlcv_gap_by_timeframe.csv",
    "ohlcv_gap_samples.csv",
)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(block)
    return digest.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def atomic_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")

    with temporary.open("w", encoding="utf-8") as f:
        json.dump(
            data, f, ensure_ascii=False,
            indent=2, sort_keys=True
        )
        f.write("\n")

    os.replace(temporary, path)


def write_csv(path, columns, records):
    temporary = path.with_name(path.name + ".tmp")

    with temporary.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=columns, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(records)

    os.replace(temporary, path)


def to_ms(row):
    raw = str(row.get("timestamp") or "").strip()

    if raw:
        value = int(raw)

        if value > 100000000000:
            return value
        if value > 100000000:
            return value * 1000

        raise ValueError("Invalid timestamp magnitude")

    raw = str(
        row.get("candle_date_time_utc") or ""
    ).strip()

    if not raw:
        raise ValueError("Timestamp missing")

    dt = datetime.fromisoformat(
        raw.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise ValueError("Timezone missing")

    return int(dt.timestamp() * 1000)


def iso_ms(value):
    return datetime.fromtimestamp(
        value / 1000, tz=timezone.utc
    ).isoformat()


def load_diagnostics(root):
    folder = root / DIAG_REL
    manifest_path = (
        folder / "review_diagnostics_manifest.json"
    )

    manifest = read_json(manifest_path)

    if manifest.get("audit") != (
        "pattern_256_outcome_review_diagnostics"
    ):
        raise RuntimeError("Unexpected diagnostic audit")

    if manifest.get("version") != "Clean V001":
        raise RuntimeError("Unexpected diagnostic version")

    outputs = manifest.get("outputs", {})

    for name in REQUIRED_DIAG:
        path = folder / name
        expected = str(outputs.get(name, "")).lower()

        if not path.is_file() or len(expected) != 64:
            raise RuntimeError(
                "Missing diagnostic input: " + name
            )

        if sha256(path) != expected:
            raise RuntimeError(
                "Diagnostic SHA256 mismatch: " + name
            )

    result = read_json(
        folder / "review_diagnostics_result.json"
    )

    if result.get("execution") != "PASS":
        raise RuntimeError("Diagnostics not PASS")

    if result.get("classification") != "REVIEW":
        raise RuntimeError("Unexpected classification")

    if result.get("research_decision") != "HOLD":
        raise RuntimeError("Unexpected research decision")

    if int(result.get("rows_checked", 0)) <= 0:
        raise RuntimeError("Invalid diagnostic row count")

    market_counts = defaultdict(Counter)

    with (
        folder / "review_by_market.csv"
    ).open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {
            "timeframe", "market",
            "classification", "rows"
        }

        if not required.issubset(
            set(reader.fieldnames or [])
        ):
            raise RuntimeError(
                "Unexpected review_by_market schema"
            )

        for row in reader:
            key = (row["timeframe"], row["market"])
            market_counts[key][
                row["classification"]
            ] += int(row["rows"])

    expected_counts = {
        str(k): int(v)
        for k, v in result[
            "classification_counts"
        ].items()
    }

    actual_counts = Counter()

    for counts in market_counts.values():
        actual_counts.update(counts)

    if dict(actual_counts) != expected_counts:
        raise RuntimeError(
            "Market aggregation mismatch"
        )

    return result, market_counts


def scan_ohlcv(path, timeframe, sample_limit):
    step = TIMEFRAMES[timeframe]

    total = 0
    invalid = 0
    duplicates = 0
    reverse = 0
    misaligned = 0
    gap_events = 0
    missing_bars = 0
    max_missing = 0

    first = None
    last = None
    previous = None
    samples = []

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        columns = set(reader.fieldnames or [])

        if not (
            "timestamp" in columns
            or "candle_date_time_utc" in columns
        ):
            raise RuntimeError(
                "OHLCV timestamp column missing"
            )

        for line_number, row in enumerate(
            reader, start=2
        ):
            total += 1

            try:
                current = to_ms(row)
            except (ValueError, OverflowError, TypeError):
                invalid += 1

                if len(samples) < sample_limit:
                    samples.append({
                        "type": "INVALID_TIMESTAMP",
                        "line": line_number,
                        "start": "",
                        "end": "",
                        "missing_bars": "",
                    })

                continue

            if first is None:
                first = current

            last = current

            if current % step != 0:
                misaligned += 1

            if previous is not None:
                delta = current - previous

                if delta == 0:
                    duplicates += 1

                elif delta < 0:
                    reverse += 1

                elif delta > step:
                    # 양의 시간 차이가 정상 간격보다 큰 경우.
                    # 간격이 정수배가 아닌 경우는 별도로
                    # misaligned 항목에서도 확인해야 한다.
                    count = max(
                        0, (delta - 1) // step
                    )

                    gap_events += 1
                    missing_bars += count
                    max_missing = max(
                        max_missing, count
                    )

                    if len(samples) < sample_limit:
                        samples.append({
                            "type": "TIME_GAP",
                            "line": line_number,
                            "start": iso_ms(previous),
                            "end": iso_ms(current),
                            "missing_bars": count,
                        })

            previous = current

    if total == 0:
        raise RuntimeError("Empty OHLCV CSV")

    return {
        "rows": total,
        "invalid_timestamps": invalid,
        "duplicate_timestamps": duplicates,
        "reverse_timestamps": reverse,
        "misaligned_timestamps": misaligned,
        "gap_events": gap_events,
        "missing_bars": missing_bars,
        "max_missing_bars": max_missing,
        "first_timestamp": (
            iso_ms(first) if first is not None else ""
        ),
        "last_timestamp": (
            iso_ms(last) if last is not None else ""
        ),
        "samples": samples,
    }


def valid_checkpoint(data, timeframe, market, source_hash,
                     script_hash, sample_limit):
    if not isinstance(data, dict):
        return False

    return (
        data.get("audit") == AUDIT
        and data.get("version") == VERSION
        and data.get("timeframe") == timeframe
        and data.get("market") == market
        and data.get("source_sha256") == source_hash
        and data.get("script_sha256") == script_hash
        and data.get("sample_limit") == sample_limit
        and isinstance(data.get("scan"), dict)
        and all(
            key in data["scan"]
            for key in (
                "rows", "invalid_timestamps",
                "duplicate_timestamps",
                "reverse_timestamps",
                "misaligned_timestamps",
                "gap_events", "missing_bars",
                "max_missing_bars",
                "samples"
            )
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--sample-limit", type=int, default=5
    )
    args = parser.parse_args()

    if not 1 <= args.sample_limit <= 100:
        raise RuntimeError(
            "sample-limit must be between 1 and 100"
        )

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        raise RuntimeError("Research root missing")

    script_hash = sha256(Path(__file__).resolve())

    diag_result, review = load_diagnostics(root)

    out = root / OUT_REL
    checkpoint_root = out / "checkpoints"
    out.mkdir(parents=True, exist_ok=True)

    records = []
    sample_records = []
    errors = []
    resumed = 0
    processed = 0

    expected_markets = sorted(review.keys())

    print(
        "[START] Markets: {}".format(
            len(expected_markets)
        ),
        flush=True,
    )

    for index, (tf, market) in enumerate(
        expected_markets, start=1
    ):
        if tf not in TIMEFRAMES:
            errors.append(
                "Unknown timeframe: {}".format(tf)
            )
            continue

        source = (
            root / "data" / "ohlcv"
            / tf / (market + ".csv")
        )

        if not source.is_file():
            errors.append(
                "Missing OHLCV: {}/{}".format(tf, market)
            )
            continue

        try:
            source_hash = sha256(source)

            checkpoint = (
                checkpoint_root / tf
                / (market + ".json")
            )

            data = None

            if checkpoint.is_file():
                try:
                    candidate = read_json(checkpoint)

                    if valid_checkpoint(
                        candidate, tf, market,
                        source_hash, script_hash,
                        args.sample_limit
                    ):
                        data = candidate
                        resumed += 1
                except (
                    ValueError, OSError,
                    TypeError, KeyError
                ):
                    pass

            if data is None:
                scan = scan_ohlcv(
                    source, tf, args.sample_limit
                )

                data = {
                    "audit": AUDIT,
                    "version": VERSION,
                    "timeframe": tf,
                    "market": market,
                    "source_sha256": source_hash,
                    "script_sha256": script_hash,
                    "sample_limit": args.sample_limit,
                    "scan": scan,
                }

                atomic_json(checkpoint, data)

            # 체크포인트를 디스크에서 다시 읽어
            # 파일 내용과 SHA256 일치 여부를 확인한다.
            saved = read_json(checkpoint)

            if not valid_checkpoint(
                saved, tf, market, source_hash,
                script_hash, args.sample_limit
            ):
                raise RuntimeError(
                    "Checkpoint validation failed"
                )

            scan = saved["scan"]
            counts = review[(tf, market)]

            review_total = sum(counts.values())
            window_gap = counts.get(
                "WINDOW_GAP", 0
            )

            record = {
                "timeframe": tf,
                "market": market,
                "ohlcv_rows": scan["rows"],
                "gap_events": scan["gap_events"],
                "missing_bars": scan["missing_bars"],
                "max_missing_bars": scan[
                    "max_missing_bars"
                ],
                "duplicate_timestamps": scan[
                    "duplicate_timestamps"
                ],
                "reverse_timestamps": scan[
                    "reverse_timestamps"
                ],
                "invalid_timestamps": scan[
                    "invalid_timestamps"
                ],
                "misaligned_timestamps": scan[
                    "misaligned_timestamps"
                ],
                "review_rows": review_total,
                "window_gap_rows": window_gap,
                "window_gap_pct": round(
                    window_gap * 100.0 / review_total,
                    6
                ) if review_total else 0.0,
                "first_timestamp": scan[
                    "first_timestamp"
                ],
                "last_timestamp": scan[
                    "last_timestamp"
                ],
            }

            records.append(record)

            for sample in scan["samples"]:
                sample_records.append({
                    "timeframe": tf,
                    "market": market,
                    **sample,
                })

            processed += 1

            if index % 25 == 0:
                print(
                    "[PROGRESS] {}/{} | resumed={}".format(
                        index, len(expected_markets),
                        resumed
                    ),
                    flush=True,
                )

        except Exception as exc:
            errors.append(
                "{}/{}: {}: {}".format(
                    tf, market,
                    type(exc).__name__, exc
                )
            )

    if processed != len(expected_markets):
        errors.append(
            "Incomplete market coverage: {} / {}".format(
                processed, len(expected_markets)
            )
        )

    grouped = defaultdict(Counter)

    numeric_fields = (
        "ohlcv_rows", "gap_events",
        "missing_bars", "duplicate_timestamps",
        "reverse_timestamps",
        "invalid_timestamps",
        "misaligned_timestamps",
        "review_rows", "window_gap_rows",
    )

    for row in records:
        group = grouped[row["timeframe"]]
        group["markets"] += 1

        for field in numeric_fields:
            group[field] += int(row[field])

        if int(row["gap_events"]) > 0:
            group["markets_with_gaps"] += 1

        if int(row["window_gap_rows"]) > 0:
            group["markets_with_review_gaps"] += 1

    timeframe_records = []

    for tf in TIMEFRAMES:
        row = {"timeframe": tf}
        row.update(grouped[tf])
        timeframe_records.append(row)

    # 시장별 원본 봉 간격의 이상 여부와
    # 기존 WINDOW_GAP 판정을 나란히 비교한다.
    # 두 지표의 인과관계는 이 단계에서 확정하지 않는다.

    market_columns = (
        "timeframe", "market", "ohlcv_rows",
        "gap_events", "missing_bars",
        "max_missing_bars",
        "duplicate_timestamps",
        "reverse_timestamps",
        "invalid_timestamps",
        "misaligned_timestamps",
        "review_rows", "window_gap_rows",
        "window_gap_pct",
        "first_timestamp", "last_timestamp",
    )

    timeframe_columns = (
        "timeframe", "markets",
        "markets_with_gaps",
        "markets_with_review_gaps",
        *numeric_fields,
    )

    sample_columns = (
        "timeframe", "market", "type",
        "line", "start", "end", "missing_bars",
    )

    write_csv(
        out / "ohlcv_gap_by_market.csv",
        market_columns, records
    )

    write_csv(
        out / "ohlcv_gap_by_timeframe.csv",
        timeframe_columns, timeframe_records
    )

    write_csv(
        out / "ohlcv_gap_samples.csv",
        sample_columns, sample_records
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": (
            "PASS" if not errors else "FAIL"
        ),
        "classification": "REVIEW",
        "research_decision": "HOLD",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_diagnostic_rows": diag_result[
            "rows_checked"
        ],
        "source_review_rows": diag_result[
            "review_rows"
        ],
        "markets_expected": len(expected_markets),
        "markets_processed": processed,
        "markets_resumed": resumed,
        "timeframes": {
            row["timeframe"]: {
                key: value
                for key, value in row.items()
                if key != "timeframe"
            }
            for row in timeframe_records
        },
        "errors": errors[:100],
        "error_count": len(errors),
        "limitations": [
            "Missing intervals are not proof of collection failure",
            "No-trade candles may be absent from exchange data",
            "Gap correlation does not establish causation",
            "WINDOW_GAP classifications are not recomputed",
            "Independent OOS and profitability are not proven",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    atomic_json(
        out / "ohlcv_gap_result.json", result
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            name: sha256(out / name)
            for name in REPORT_FILES
        },
    }

    atomic_json(
        out / "ohlcv_gap_manifest.json",
        manifest
    )

    print(
        json.dumps(
            {
                "execution": result["execution"],
                "markets_processed": processed,
                "markets_resumed": resumed,
                "error_count": len(errors),
                "research_decision": "HOLD",
            },
            indent=2,
        ),
        flush=True,
    )

    return 0 if not errors else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FATAL] {}: {}".format(
                type(exc).__name__, exc
            ),
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
