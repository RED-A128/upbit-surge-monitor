
# -*- coding: utf-8 -*-
"""
Pattern 256 CENSORED Boundary Audit
Clean V001

Read-only audit:
- Verify 291 previously reconciled events.
- Verify source checkpoint SHA256.
- Compare signal timestamps against OHLCV history.
- Distinguish no future candles from suspicious censoring.
- Never modify original data or existing checkpoints.

Standard library only.
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
AUDIT = "pattern_256_censored_boundary"

INPUT_RESULT = (
    "data/reports/"
    "pattern_256_entry_timestamp_root_cause/"
    "entry_timestamp_root_cause_result.json"
)

INPUT_DETAIL = (
    "data/reports/"
    "pattern_256_entry_timestamp_root_cause/"
    "entry_timestamp_root_cause_detail.csv"
)

OUTPUT = (
    "data/reports/"
    "pattern_256_censored_boundary"
)

TIME_COLUMNS = (
    "timestamp",
    "candle_date_time_utc",
    "datetime",
    "date",
    "time",
)

TIMEFRAMES = ("h1", "h4", "d1")


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1048576),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r", encoding="utf-8-sig"
    ) as stream:
        return json.load(stream)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        return list(csv.DictReader(stream))


def write_json(path, value):
    with path.open(
        "w", encoding="utf-8"
    ) as stream:
        json.dump(
            value,
            stream,
            indent=2,
            ensure_ascii=False,
        )


def write_csv(path, columns, rows):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_time(value):
    text = str(value or "").strip()

    if not text:
        raise ValueError("Empty timestamp")

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    if text.isdigit():
        number = int(text)

        if number > 10**12:
            number /= 1000

        return datetime.fromtimestamp(
            number,
            tz=timezone.utc,
        )

    result = datetime.fromisoformat(text)

    if result.tzinfo is None:
        # This assumption must be visible in reports.
        result = result.replace(
            tzinfo=timezone.utc
        )

    return result.astimezone(timezone.utc)


def utc_text(value):
    return value.isoformat() if value else ""


def safe_file(root, relative):
    result = (root / relative).resolve()
    result.relative_to(root)
    return result


def read_ohlcv_timestamps(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        columns = reader.fieldnames or []

        selected = next(
            (
                name for name in TIME_COLUMNS
                if name in columns
            ),
            None,
        )

        if selected is None:
            raise ValueError(
                "No supported OHLCV time column. "
                f"Columns: {columns}"
            )

        timestamps = []

        for line, row in enumerate(
            reader,
            start=2,
        ):
            try:
                timestamps.append(
                    parse_time(row[selected])
                )
            except Exception as exc:
                raise ValueError(
                    f"Invalid OHLCV timestamp "
                    f"at line {line}: {exc}"
                )

    if not timestamps:
        raise ValueError("Empty OHLCV file")

    if timestamps != sorted(timestamps):
        raise ValueError(
            "OHLCV timestamps are not ascending"
        )

    if len(timestamps) != len(set(timestamps)):
        raise ValueError(
            "Duplicate OHLCV timestamps"
        )

    return selected, timestamps


def evaluate_event(event, timestamps):
    signal = parse_time(
        event["signal_timestamp"]
    )

    future_count = sum(
        stamp > signal
        for stamp in timestamps
    )

    signal_exists = signal in set(timestamps)

    last = timestamps[-1]
    first = timestamps[0]

    if signal > last:
        diagnosis = "SIGNAL_AFTER_LAST_CANDLE"

    elif signal < first:
        diagnosis = "SIGNAL_BEFORE_FIRST_CANDLE"

    elif future_count == 0:
        diagnosis = "NO_FUTURE_CANDLES"

    else:
        diagnosis = "FUTURE_CANDLES_PRESENT_REVIEW"

    return {
        "signal_exists": signal_exists,
        "first_candle": utc_text(first),
        "last_candle": utc_text(last),
        "future_candles": future_count,
        "diagnosis": diagnosis,
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()

    input_result_path = root / INPUT_RESULT
    input_detail_path = root / INPUT_DETAIL

    for path in (
        input_result_path,
        input_detail_path,
    ):
        if not path.is_file():
            raise FileNotFoundError(path)

    previous = read_json(input_result_path)
    events = read_csv(input_detail_path)

    if previous.get("execution") != "PASS":
        raise ValueError(
            "Previous audit did not PASS"
        )

    if len(events) != 291:
        raise ValueError(
            "Expected exactly 291 events"
        )

    if previous.get("checked_events") != 291:
        raise ValueError(
            "Previous event count mismatch"
        )

    expected_hashes = previous.get(
        "source_hashes", {}
    )

    grouped = defaultdict(list)

    for event in events:
        relative = event.get(
            "source_file", ""
        ).strip()

        grouped[relative].append(event)

    if len(grouped) != 97:
        raise ValueError(
            "Expected exactly 97 checkpoint files"
        )

    if set(grouped) != set(expected_hashes):
        raise ValueError(
            "Checkpoint file set mismatch"
        )

    results = []
    issues = []
    diagnosis_counts = Counter()
    timeframe_counts = Counter()
    horizon_counts = Counter()

    verified_sources = {}

    # Cache OHLCV timestamps for each market/timeframe.
    ohlcv_cache = {}

    for relative, group in sorted(
        grouped.items()
    ):
        try:
            checkpoint = safe_file(
                root, relative
            )

            if not checkpoint.is_file():
                raise FileNotFoundError(
                    checkpoint
                )

            actual_hash = sha256(checkpoint)

            if (
                actual_hash
                != expected_hashes[relative]
            ):
                raise ValueError(
                    "Checkpoint SHA256 mismatch"
                )

            verified_sources[relative] = (
                actual_hash
            )

            for event in group:
                timeframe = event[
                    "timeframe"
                ].strip()

                market = event[
                    "market"
                ].strip()

                if timeframe not in TIMEFRAMES:
                    raise ValueError(
                        "Unsupported timeframe"
                    )

                if not market.startswith("KRW-"):
                    raise ValueError(
                        "Unexpected market"
                    )

                if (
                    event["status"].strip().upper()
                    != "CENSORED"
                ):
                    raise ValueError(
                        "Unexpected event status"
                    )

                if (
                    event["entry_timestamp"].strip()
                    or event["entry_price"].strip()
                ):
                    raise ValueError(
                        "Unexpected entry field"
                    )

                if (
                    event["observed_bars"].strip()
                    != "0"
                ):
                    raise ValueError(
                        "Unexpected observed_bars"
                    )

                cache_key = (
                    timeframe, market
                )

                ohlcv_relative = (
                    f"data/ohlcv/"
                    f"{timeframe}/{market}.csv"
                )

                ohlcv_path = safe_file(
                    root,
                    ohlcv_relative,
                )

                if cache_key not in ohlcv_cache:
                    if not ohlcv_path.is_file():
                        raise FileNotFoundError(
                            ohlcv_path
                        )

                    column, timestamps = (
                        read_ohlcv_timestamps(
                            ohlcv_path
                        )
                    )

                    ohlcv_cache[cache_key] = {
                        "column": column,
                        "timestamps": timestamps,
                        "sha256": sha256(
                            ohlcv_path
                        ),
                    }

                history = ohlcv_cache[
                    cache_key
                ]

                evaluated = evaluate_event(
                    event,
                    history["timestamps"],
                )

                future_count = evaluated[
                    "future_candles"
                ]

                try:
                    horizon = int(
                        event["horizon"]
                    )
                    required = int(
                        event["required_bars"]
                    )
                except ValueError:
                    raise ValueError(
                        "Invalid horizon/required_bars"
                    )

                if horizon != required:
                    raise ValueError(
                        "Horizon differs from "
                        "required_bars"
                    )

                if horizon <= 0:
                    raise ValueError(
                        "Non-positive horizon"
                    )

                diagnosis = evaluated[
                    "diagnosis"
                ]

                diagnosis_counts[
                    diagnosis
                ] += 1

                timeframe_counts[
                    timeframe
                ] += 1

                horizon_counts[
                    str(horizon)
                ] += 1

                results.append({
                    "source_file": relative,
                    "row_number": event[
                        "row_number"
                    ],
                    "timeframe": timeframe,
                    "market": market,
                    "classification": event[
                        "classification"
                    ],
                    "signal_timestamp": event[
                        "signal_timestamp"
                    ],
                    "status": event["status"],
                    "horizon": horizon,
                    "observed_bars": 0,
                    "required_bars": required,
                    "ohlcv_file": ohlcv_relative,
                    "ohlcv_time_column": history[
                        "column"
                    ],
                    "first_candle": evaluated[
                        "first_candle"
                    ],
                    "last_candle": evaluated[
                        "last_candle"
                    ],
                    "signal_in_ohlcv": evaluated[
                        "signal_exists"
                    ],
                    "future_candles": future_count,
                    "diagnosis": diagnosis,
                })

        except Exception as exc:
            issues.append({
                "source_file": relative,
                "error": str(exc),
            })

    all_verified = (
        len(results) == 291
        and len(verified_sources) == 97
        and not issues
    )

    no_future = diagnosis_counts[
        "NO_FUTURE_CANDLES"
    ]

    # PASS means this audit ran and verified inputs.
    # It does not imply that censoring is correct.
    execution = (
        "PASS" if all_verified else "FAIL"
    )

    if not all_verified:
        conclusion = (
            "INCOMPLETE_AUDIT"
        )

    elif no_future == 291:
        conclusion = (
            "ALL_EVENTS_AT_OHLCV_END_BOUNDARY"
        )

    elif diagnosis_counts[
        "FUTURE_CANDLES_PRESENT_REVIEW"
    ]:
        conclusion = (
            "CENSORED_WITH_AVAILABLE_FUTURE_DATA"
        )

    else:
        conclusion = (
            "MIXED_BOUNDARY_CONDITIONS"
        )

    output_dir = root / OUTPUT

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_dir
        / "censored_boundary_detail.csv"
    )

    result_path = (
        output_dir
        / "censored_boundary_result.json"
    )

    manifest_path = (
        output_dir
        / "censored_boundary_manifest.json"
    )

    columns = [
        "source_file",
        "row_number",
        "timeframe",
        "market",
        "classification",
        "signal_timestamp",
        "status",
        "horizon",
        "observed_bars",
        "required_bars",
        "ohlcv_file",
        "ohlcv_time_column",
        "first_candle",
        "last_candle",
        "signal_in_ohlcv",
        "future_candles",
        "diagnosis",
    ]

    write_csv(
        detail_path,
        columns,
        results,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "research_decision": "HOLD",
        "input_events": len(events),
        "checked_events": len(results),
        "verified_checkpoint_files": len(
            verified_sources
        ),
        "ohlcv_files_checked": len(
            ohlcv_cache
        ),
        "diagnosis_counts": dict(
            diagnosis_counts
        ),
        "timeframe_counts": dict(
            timeframe_counts
        ),
        "horizon_counts": dict(
            horizon_counts
        ),
        "conclusion": conclusion,
        "issues": issues,
        "checkpoint_hashes": verified_sources,
        "ohlcv_hashes": {
            f"data/ohlcv/{tf}/{market}.csv":
                data["sha256"]
            for (tf, market), data
            in ohlcv_cache.items()
        },
        "limitations": [
            "Checks 291 events only",
            "Uses current OHLCV snapshots",
            "Does not prove point-in-time data availability",
            "Naive timestamps assumed UTC",
            "Does not verify original evaluator logic",
            "Does not modify source data",
            "Does not prove OOS or profitability",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    write_json(
        result_path,
        result,
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "input_sha256": {
            input_result_path.name:
                sha256(input_result_path),
            input_detail_path.name:
                sha256(input_detail_path),
        },
        "output_sha256": {
            detail_path.name:
                sha256(detail_path),
            result_path.name:
                sha256(result_path),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print("=" * 60)
    print("Pattern 256 CENSORED Boundary Audit")
    print("=" * 60)
    print("Version:", VERSION)
    print("Execution:", execution)
    print("Events:", len(results))
    print("Checkpoints:", len(verified_sources))
    print("OHLCV files:", len(ohlcv_cache))
    print("Diagnosis:", dict(diagnosis_counts))
    print("Conclusion:", conclusion)
    print("Issues:", len(issues))
    print("Research decision: HOLD")
    print("=" * 60)

    return 0 if all_verified else 1


if __name__ == "__main__":
    sys.exit(main())
