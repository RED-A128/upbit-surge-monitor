
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_independent_events.py

Clean V002

256 패턴 독립 이벤트 검증

기능:
1. 기존 백테스트 CSV 읽기
2. TRAIN / TEST 시간 분리
3. 동일 코인·시간봉·분류·관찰기간별 중복 신호 제거
4. 256 단독과 256+매집 결합 조건 비교
5. 코인별 성능 분석
6. 연도별 성능 분석
7. Checkpoint / Resume
8. SHA256 무결성 검증
9. 결과 파일 5개 생성

주의:
- 원본 CSV 수정 금지
- 미래 정보로 신호 생성 금지
- 자동매매 기능 없음
- 중복 제거가 통계적 독립성을 증명하지는 않음
"""

import argparse
import csv
import hashlib
import io
import json
import os
import sys

from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V002"

SOURCE_RELATIVE = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_independent_events"
)

SOURCE_EXPECTED_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

SOURCE_EXPECTED_ROWS = 7_949_826

CUTOFF = datetime(
    2025, 1, 1,
    tzinfo=timezone.utc
)

TEST_END = datetime(
    2026, 9, 1,
    tzinfo=timezone.utc
)

TIMEFRAME_HOURS = {
    "h1": 1,
    "h4": 4,
    "d1": 24,
}

HORIZONS = (
    24,
    72,
    168,
)

CHECKPOINT_INTERVAL = 100_000

REQUIRED_COLUMNS = {
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
    "entry_timestamp",
    "entry_price",
    "target_price",
    "status",
    "hit_30pct",
    "observed_bars",
    "required_bars",
    "entry_time_gap",
    "gap_count",
}

COMPARISON_COLUMNS = [
    "partition",
    "timeframe",
    "horizon",
    "baseline_completed",
    "baseline_hits",
    "baseline_rate_pct",
    "combined_completed",
    "combined_hits",
    "combined_rate_pct",
    "difference_pp",
    "status",
]

MARKET_COLUMNS = [
    "partition",
    "market",
    "timeframe",
    "horizon",
    "classification",
    "completed",
    "hits",
    "hit_rate_pct",
]

YEAR_COLUMNS = [
    "partition",
    "year",
    "timeframe",
    "horizon",
    "classification",
    "completed",
    "hits",
    "hit_rate_pct",
]


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(
                4 * 1024 * 1024
            )

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_json(path, payload):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )

        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(
        temporary,
        path,
    )


def atomic_csv(
    path,
    columns,
    records,
):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
        )

        writer.writeheader()

        for record in records:
            writer.writerow(record)

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(
        temporary,
        path,
    )


def parse_timestamp(value):
    if not value:
        raise ValueError(
            "Missing required timestamp"
        )

    parsed = datetime.fromisoformat(
        value.strip().replace(
            "Z",
            "+00:00",
        )
    )

    if parsed.tzinfo is None:
        raise ValueError(
            "Timestamp timezone is required"
        )

    return parsed.astimezone(
        timezone.utc
    )


def classify(value):
    normalized = (
        value.strip().upper()
    )

    if normalized == "PATTERN_256_ONLY":
        return "BASELINE"

    combined_names = {
        "PATTERN_256_AND_ACCUMULATION",
        "PATTERN_256_ACCUMULATION",
        "PATTERN_256_WITH_ACCUMULATION",
        "PATTERN_256_BOTH",
        "BOTH",
    }

    if normalized in combined_names:
        return "COMBINED"

    if (
        "256" in normalized
        and (
            "ACCUMULATION" in normalized
            or "BOTH" in normalized
        )
        and "ONLY" not in normalized
    ):
        return "COMBINED"

    return None


def initial_state():
    return {
        "version": VERSION,
        "processed": 0,
        "excluded": {},
        "counts": {},
        "markets": {},
        "years": {},
        "last_accepted_end": {},
        "last_seen_entry": {},
        "classifications": {},
    }


def increment(
    mapping,
    key,
    amount=1,
):
    mapping[key] = (
        mapping.get(key, 0)
        + amount
    )


def update_count(
    mapping,
    key,
    hit,
):
    current = mapping.setdefault(
        key,
        {
            "completed": 0,
            "hits": 0,
        },
    )

    current["completed"] += 1
    current["hits"] += int(hit)


def serialize_key(*values):
    return "|".join(
        str(value)
        for value in values
    )


def partition_for(
    entry,
    maturity,
):
    if entry < CUTOFF:
        if maturity <= CUTOFF:
            return "TRAIN", None

        return None, "train_boundary"

    if entry >= TEST_END:
        return None, "after_test_end"

    if maturity > TEST_END:
        return None, "test_boundary"

    return "TEST", None


def evaluate_row(
    row,
    state,
):
    timeframe = (
        row["timeframe"]
        .strip()
        .lower()
    )

    market = (
        row["market"]
        .strip()
        .upper()
    )

    if timeframe not in TIMEFRAME_HOURS:
        increment(
            state["excluded"],
            "unsupported_timeframe",
        )
        return

    try:
        horizon = int(
            row["horizon"]
        )
    except (ValueError, TypeError):
        increment(
            state["excluded"],
            "invalid_horizon",
        )
        return

    if horizon not in HORIZONS:
        increment(
            state["excluded"],
            "unsupported_horizon",
        )
        return

    raw_class = (
        row["classification"]
        .strip()
    )

    increment(
        state["classifications"],
        raw_class,
    )

    group = classify(
        raw_class
    )

    if group is None:
        increment(
            state["excluded"],
            "unsupported_classification",
        )
        return

    status = (
        row["status"]
        .strip()
        .upper()
    )

    if status not in {
        "HIT",
        "MISS",
    }:
        increment(
            state["excluded"],
            "incomplete",
        )
        return

    try:
        observed = int(
            row["observed_bars"]
        )

        required = int(
            row["required_bars"]
        )

        gap_count = int(
            row["gap_count"] or "0"
        )

        entry_gap = int(
            row["entry_time_gap"] or "0"
        )

    except (ValueError, TypeError):
        increment(
            state["excluded"],
            "invalid_observation",
        )
        return

    if (
        observed != horizon
        or required != horizon
    ):
        increment(
            state["excluded"],
            "incomplete_observation",
        )
        return

    if (
        gap_count != 0
        or entry_gap != 0
    ):
        increment(
            state["excluded"],
            "candle_gap",
        )
        return

    try:
        entry = parse_timestamp(
            row["entry_timestamp"]
        )

        signal = parse_timestamp(
            row["signal_timestamp"]
        )

    except (ValueError, TypeError):
        increment(
            state["excluded"],
            "invalid_timestamp",
        )
        return

    if entry <= signal:
        increment(
            state["excluded"],
            "invalid_entry_order",
        )
        return

    maturity = entry + timedelta(
        hours=(
            TIMEFRAME_HOURS[timeframe]
            * horizon
        )
    )

    partition, reason = partition_for(
        entry,
        maturity,
    )

    if reason:
        increment(
            state["excluded"],
            reason,
        )
        return

    raw_hit = (
        row["hit_30pct"]
        .strip()
    )

    if raw_hit not in {
        "0",
        "1",
    }:
        raise ValueError(
            "Invalid hit_30pct: "
            + raw_hit
        )

    hit = int(raw_hit)

    if (
        status == "HIT"
        and hit != 1
    ):
        raise ValueError(
            "HIT status mismatch"
        )

    if (
        status == "MISS"
        and hit != 0
    ):
        raise ValueError(
            "MISS status mismatch"
        )

    stream_key = serialize_key(
        partition,
        market,
        timeframe,
        group,
        horizon,
    )

    entry_iso = (
        entry.isoformat()
    )

    previous_seen = (
        state["last_seen_entry"]
        .get(stream_key)
    )

    if (
        previous_seen is not None
        and entry_iso < previous_seen
    ):
        raise ValueError(
            "Input is not chronologically "
            "ordered within stream: "
            + stream_key
        )

    state["last_seen_entry"][
        stream_key
    ] = entry_iso

    last_end = (
        state["last_accepted_end"]
        .get(stream_key)
    )

    if (
        last_end is not None
        and entry_iso < last_end
    ):
        increment(
            state["excluded"],
            "overlapping_signal",
        )
        return

    state["last_accepted_end"][
        stream_key
    ] = maturity.isoformat()

    overall_key = serialize_key(
        partition,
        timeframe,
        horizon,
        group,
    )

    market_key = serialize_key(
        partition,
        market,
        timeframe,
        horizon,
        group,
    )

    year_key = serialize_key(
        partition,
        entry.year,
        timeframe,
        horizon,
        group,
    )

    update_count(
        state["counts"],
        overall_key,
        hit,
    )

    update_count(
        state["markets"],
        market_key,
        hit,
    )

    update_count(
        state["years"],
        year_key,
        hit,
    )


def percentage(
    hits,
    completed,
):
    if completed == 0:
        return 0.0

    return round(
        100.0 * hits / completed,
        6,
    )


def build_comparison(state):
    rows = []

    for partition in (
        "TRAIN",
        "TEST",
    ):
        for timeframe in (
            "h1",
            "h4",
            "d1",
        ):
            for horizon in HORIZONS:
                baseline_key = serialize_key(
                    partition,
                    timeframe,
                    horizon,
                    "BASELINE",
                )

                combined_key = serialize_key(
                    partition,
                    timeframe,
                    horizon,
                    "COMBINED",
                )

                baseline = (
                    state["counts"]
                    .get(
                        baseline_key,
                        {
                            "completed": 0,
                            "hits": 0,
                        },
                    )
                )

                combined = (
                    state["counts"]
                    .get(
                        combined_key,
                        {
                            "completed": 0,
                            "hits": 0,
                        },
                    )
                )

                baseline_n = (
                    baseline["completed"]
                )

                combined_n = (
                    combined["completed"]
                )

                baseline_rate = percentage(
                    baseline["hits"],
                    baseline_n,
                )

                combined_rate = percentage(
                    combined["hits"],
                    combined_n,
                )

                if (
                    baseline_n == 0
                    or combined_n == 0
                ):
                    status = (
                        "INSUFFICIENT_DATA"
                    )

                elif min(
                    baseline_n,
                    combined_n,
                ) < 100:
                    status = (
                        "SMALL_SAMPLE"
                    )

                elif (
                    combined_rate
                    > baseline_rate
                ):
                    status = (
                        "OBSERVED_IMPROVEMENT"
                    )

                elif (
                    combined_rate
                    < baseline_rate
                ):
                    status = (
                        "OBSERVED_DETERIORATION"
                    )

                else:
                    status = (
                        "OBSERVED_EQUAL"
                    )

                rows.append(
                    {
                        "partition": partition,
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "baseline_completed": baseline_n,
                        "baseline_hits": baseline["hits"],
                        "baseline_rate_pct": baseline_rate,
                        "combined_completed": combined_n,
                        "combined_hits": combined["hits"],
                        "combined_rate_pct": combined_rate,
                        "difference_pp": round(
                            combined_rate
                            - baseline_rate,
                            6,
                        ),
                        "status": status,
                    }
                )

    return rows


def build_detail(
    mapping,
    dimensions,
):
    records = []

    for key, values in sorted(
        mapping.items()
    ):
        parts = key.split("|")

        record = dict(
            zip(
                dimensions,
                parts,
            )
        )

        record["completed"] = (
            values["completed"]
        )

        record["hits"] = (
            values["hits"]
        )

        record["hit_rate_pct"] = percentage(
            values["hits"],
            values["completed"],
        )

        records.append(
            record
        )

    return records


def source_identity_for(path):
    stat = path.stat()

    return {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sha256": sha256_file(path),
    }


def save_checkpoint(
    checkpoint_path,
    state,
    source_identity,
    offset,
):
    payload = {
        "version": VERSION,
        "source_identity": source_identity,
        "offset": offset,
        "state": state,
    }

    atomic_json(
        checkpoint_path,
        payload,
    )


def load_checkpoint(
    checkpoint_path,
    source_identity,
):
    if not checkpoint_path.exists():
        return None

    with checkpoint_path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        checkpoint = json.load(
            handle
        )

    if (
        checkpoint.get("version")
        != VERSION
    ):
        raise ValueError(
            "Checkpoint version mismatch"
        )

    if (
        checkpoint.get("source_identity")
        != source_identity
    ):
        raise ValueError(
            "Checkpoint source identity mismatch"
        )

    state = checkpoint.get(
        "state"
    )

    if not isinstance(
        state,
        dict,
    ):
        raise ValueError(
            "Invalid checkpoint state"
        )

    if (
        state.get("version")
        != VERSION
    ):
        raise ValueError(
            "Checkpoint state version mismatch"
        )

    return checkpoint


def parse_csv_line(
    raw,
    headers,
):
    if not raw.endswith(
        b"\n"
    ):
        raise ValueError(
            "CSV row is not newline terminated"
        )

    decoded = raw.decode(
        "utf-8"
    )

    reader = csv.reader(
        io.StringIO(
            decoded
        )
    )

    records = list(
        reader
    )

    if len(records) != 1:
        raise ValueError(
            "Unexpected multiline CSV record"
        )

    values = records[0]

    if len(values) != len(headers):
        raise ValueError(
            "CSV column count mismatch"
        )

    return dict(
        zip(
            headers,
            values,
        )
    )


def process_source(
    source,
    checkpoint_path,
    source_identity,
    resume,
    checkpoint_interval,
):
    checkpoint = None

    if resume:
        checkpoint = load_checkpoint(
            checkpoint_path,
            source_identity,
        )

    if checkpoint is None:
        state = initial_state()
        offset = None

    else:
        state = checkpoint[
            "state"
        ]

        offset = checkpoint[
            "offset"
        ]

        print(
            "[RESUME] rows="
            + format(
                state["processed"],
                ",",
            ),
            flush=True,
        )

    with source.open(
        "rb"
    ) as handle:
        header_line = (
            handle.readline()
        )

        headers = next(
            csv.reader(
                io.StringIO(
                    header_line.decode(
                        "utf-8-sig"
                    )
                )
            )
        )

        missing = (
            REQUIRED_COLUMNS
            - set(headers)
        )

        if missing:
            raise ValueError(
                "Missing columns: "
                + ", ".join(
                    sorted(missing)
                )
            )

        if offset is not None:
            if (
                offset < handle.tell()
                or offset > source_identity["size"]
            ):
                raise ValueError(
                    "Invalid checkpoint offset"
                )

            handle.seek(
                offset
            )

        while True:
            raw = (
                handle.readline()
            )

            if not raw:
                break

            row = parse_csv_line(
                raw,
                headers,
            )

            try:
                evaluate_row(
                    row,
                    state,
                )

            except Exception as exc:
                raise ValueError(
                    "Failure at source row "
                    + str(
                        state["processed"]
                        + 2
                    )
                    + ": "
                    + str(exc)
                ) from exc

            state["processed"] += 1

            if (
                state["processed"]
                % checkpoint_interval
                == 0
            ):
                save_checkpoint(
                    checkpoint_path,
                    state,
                    source_identity,
                    handle.tell(),
                )

                print(
                    "[PROGRESS] rows="
                    + format(
                        state["processed"],
                        ",",
                    ),
                    flush=True,
                )

        final_offset = (
            handle.tell()
        )

    save_checkpoint(
        checkpoint_path,
        state,
        source_identity,
        final_offset,
    )

    return state


def verify_outputs(
    output_dir,
    output_names,
):
    hashes = {}

    for name in output_names:
        path = (
            output_dir / name
        )

        if not path.is_file():
            raise ValueError(
                "Missing output: "
                + name
            )

        hashes[name] = sha256_file(
            path
        )

    return hashes


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=CHECKPOINT_INTERVAL,
    )

    parser.add_argument(
        "--no-resume",
        action="store_true",
    )

    args = parser.parse_args()

    if (
        args.checkpoint_interval
        < 1
    ):
        parser.error(
            "checkpoint-interval must be positive"
        )

    root = Path(
        args.root
    ).resolve()

    source = (
        root / SOURCE_RELATIVE
    )

    output_dir = (
        root / OUTPUT_RELATIVE
    )

    if not source.is_file():
        raise FileNotFoundError(
            "Source CSV not found: "
            + str(source)
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    checkpoint_path = (
        output_dir
        / "independent_events_checkpoint.json"
    )

    print(
        "[START] "
        + VERSION,
        flush=True,
    )

    print(
        "[SOURCE] "
        + str(source),
        flush=True,
    )

    source_identity = (
        source_identity_for(
            source
        )
    )

    print(
        "[SOURCE SHA256] "
        + source_identity["sha256"],
        flush=True,
    )

    if (
        source_identity["sha256"]
        != SOURCE_EXPECTED_SHA256
    ):
        raise ValueError(
            "Source SHA256 does not match "
            "validated baseline"
        )

    state = process_source(
        source=source,
        checkpoint_path=checkpoint_path,
        source_identity=source_identity,
        resume=not args.no_resume,
        checkpoint_interval=(
            args.checkpoint_interval
        ),
    )

    if (
        state["processed"]
        != SOURCE_EXPECTED_ROWS
    ):
        raise ValueError(
            "Source row count mismatch: "
            + str(
                state["processed"]
            )
        )

    comparisons = (
        build_comparison(
            state
        )
    )

    market_rows = (
        build_detail(
            state["markets"],
            [
                "partition",
                "market",
                "timeframe",
                "horizon",
                "classification",
            ],
        )
    )

    year_rows = (
        build_detail(
            state["years"],
            [
                "partition",
                "year",
                "timeframe",
                "horizon",
                "classification",
            ],
        )
    )

    comparison_path = (
        output_dir
        / "independent_events_comparison.csv"
    )

    market_path = (
        output_dir
        / "independent_events_by_market.csv"
    )

    year_path = (
        output_dir
        / "independent_events_by_year.csv"
    )

    result_path = (
        output_dir
        / "independent_events_result.json"
    )

    manifest_path = (
        output_dir
        / "independent_events_manifest.json"
    )

    atomic_csv(
        comparison_path,
        COMPARISON_COLUMNS,
        comparisons,
    )

    atomic_csv(
        market_path,
        MARKET_COLUMNS,
        market_rows,
    )

    atomic_csv(
        year_path,
        YEAR_COLUMNS,
        year_rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": state["processed"],
        "source_sha256": source_identity["sha256"],
        "comparison_rows": len(comparisons),
        "market_rows": len(market_rows),
        "year_rows": len(year_rows),
        "excluded": state["excluded"],
        "classifications": state["classifications"],
        "settings": {
            "cutoff": CUTOFF.isoformat(),
            "test_end": TEST_END.isoformat(),
            "overlap_rule": (
                "Within each partition, market, "
                "timeframe, classification and "
                "horizon, accept the earliest "
                "nonoverlapping entry."
            ),
            "boundary_rule": (
                "Theoretical maturity must "
                "remain inside its partition."
            ),
            "candle_gap_policy": (
                "Exclude rows with nonzero "
                "gap_count or entry_time_gap."
            ),
        },
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "caution": (
            "Overlap filtering is diagnostic only. "
            "Across-horizon dependence, "
            "selection bias and market regimes "
            "require further validation."
        ),
    }

    atomic_json(
        result_path,
        result,
    )

    output_names = [
        "independent_events_comparison.csv",
        "independent_events_by_market.csv",
        "independent_events_by_year.csv",
        "independent_events_result.json",
    ]

    output_hashes = verify_outputs(
        output_dir,
        output_names,
    )

    manifest = {
        "version": VERSION,
        "source_sha256": source_identity["sha256"],
        "source_size": source_identity["size"],
        "output_sha256": output_hashes,
        "checkpoint_rows": state["processed"],
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    final_identity = (
        source_identity_for(
            source
        )
    )

    if (
        final_identity
        != source_identity
    ):
        raise RuntimeError(
            "Source changed during evaluation"
        )

    print(
        "[PASS] "
        + format(
            state["processed"],
            ",",
        )
        + " source rows; "
        + str(
            len(comparisons)
        )
        + " comparison rows",
        flush=True,
    )

    print(
        "[OUTPUT] "
        + str(output_dir),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(
            main()
        )

    except KeyboardInterrupt:
        print(
            "\n[INTERRUPTED] "
            "Resume from last checkpoint.",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] "
            + str(exc),
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
