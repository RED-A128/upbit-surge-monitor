
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Full-Market Independent Outcome Verification
Clean V002

Read-only verification of original OHLCV and backtest checkpoints.

Fixes:
- Prefer candle_date_time_utc over numeric timestamp
- Verify Unix millisecond timestamp against UTC
- Handle timestamp units explicitly
- Check candle continuity
- Stream final detail CSV instead of accumulating all rows
- Per-file SHA256-verified resume
- Atomic writes
- Preserve research HOLD
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V002"
AUDIT = "pattern_256_outcomes_full_market"

TIMEFRAMES = {
    "h1": timedelta(hours=1),
    "h4": timedelta(hours=4),
    "d1": timedelta(days=1),
}

REQUIRED = {
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "target_return_pct",
    "target_price",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "first_hit_timestamp",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
    "source_ohlcv_sha256",
}

DETAIL_COLUMNS = [
    "timeframe",
    "market",
    "checkpoint_row",
    "signal_timestamp",
    "entry_timestamp",
    "horizon",
    "recorded_status",
    "include_result",
    "exclude_result",
    "classification",
    "include_observed",
    "exclude_observed",
    "include_max_return_pct",
    "exclude_max_return_pct",
    "include_max_drawdown_pct",
    "exclude_max_drawdown_pct",
]


def sha256(path):
    digest = hashlib.sha256()

    with open(path, "rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path, value):
    atomic_text(
        path,
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
    )


def number(value):
    result = float(value)

    if not math.isfinite(result):
        raise ValueError("NONFINITE_NUMBER")

    return result


def integer(value):
    result = number(value)

    if not result.is_integer():
        raise ValueError("NOT_INTEGER")

    return int(result)


def near(a, b):
    return math.isclose(
        a,
        b,
        rel_tol=1e-8,
        abs_tol=1e-7,
    )


def timestamp(value):
    raw = str(value or "").strip()

    if not raw:
        raise ValueError("TIMESTAMP_EMPTY")

    if re.fullmatch(r"[+-]?\d{10,19}", raw):
        numeric = int(raw)

        if len(raw.lstrip("+-")) == 10:
            seconds = numeric

        elif len(raw.lstrip("+-")) == 13:
            seconds = numeric / 1000

        elif len(raw.lstrip("+-")) == 16:
            seconds = numeric / 1000000

        elif len(raw.lstrip("+-")) == 19:
            seconds = numeric / 1000000000

        else:
            raise ValueError(
                "UNSUPPORTED_UNIX_TIMESTAMP_UNIT"
            )

        try:
            return datetime.fromtimestamp(
                seconds,
                timezone.utc,
            )
        except (OverflowError, OSError, ValueError):
            raise ValueError("UNIX_TIMESTAMP_OUT_OF_RANGE")

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        result = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(
            "INVALID_ISO_TIMESTAMP:" + raw[:80]
        )

    if result.tzinfo is None:
        raise ValueError("TIMEZONE_MISSING")

    return result.astimezone(timezone.utc)


def normalize_header(name):
    return (
        str(name)
        .strip()
        .lower()
        .replace("-", "_")
    )


def pick_column(names, candidates):
    lookup = {
        normalize_header(name): name
        for name in names
    }

    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]

    return None


def load_ohlcv(path, timeframe):
    candles = []
    previous = None
    step = TIMEFRAMES[timeframe]

    with open(
        path, "r", encoding="utf-8-sig", newline=""
    ) as handle:

        reader = csv.DictReader(handle)
        names = reader.fieldnames or []

        utc_column = pick_column(
            names,
            ["candle_date_time_utc"],
        )

        numeric_column = pick_column(
            names,
            ["timestamp"],
        )

        fallback_column = pick_column(
            names,
            ["datetime", "time", "date"],
        )

        time_column = (
            utc_column
            or numeric_column
            or fallback_column
        )

        open_column = pick_column(
            names,
            ["open", "opening_price", "open_price"],
        )

        high_column = pick_column(
            names,
            ["high", "high_price"],
        )

        low_column = pick_column(
            names,
            ["low", "low_price"],
        )

        if not all([
            time_column,
            open_column,
            high_column,
            low_column,
        ]):
            raise ValueError(
                "OHLCV_SCHEMA_UNSUPPORTED:"
                + ",".join(names)
            )

        for line_no, row in enumerate(reader, 2):

            dt = timestamp(row[time_column])

            if utc_column and numeric_column:
                numeric_time = timestamp(
                    row[numeric_column]
                )

                if dt != numeric_time:
                    raise ValueError(
                        "UTC_UNIX_TIMESTAMP_MISMATCH:"
                        + str(line_no)
                    )

            opening = number(row[open_column])
            high = number(row[high_column])
            low = number(row[low_column])

            if min(opening, high, low) <= 0:
                raise ValueError(
                    "NONPOSITIVE_PRICE_LINE_"
                    + str(line_no)
                )

            if low > high:
                raise ValueError(
                    "LOW_GREATER_THAN_HIGH_LINE_"
                    + str(line_no)
                )

            if not low <= opening <= high:
                raise ValueError(
                    "OPEN_OUTSIDE_RANGE_LINE_"
                    + str(line_no)
                )

            if previous is not None:
                if dt <= previous:
                    raise ValueError(
                        "TIMESTAMP_ORDER_LINE_"
                        + str(line_no)
                    )

            candles.append({
                "timestamp": dt,
                "open": opening,
                "high": high,
                "low": low,
            })

            previous = dt

    if not candles:
        raise ValueError("EMPTY_OHLCV")

    return candles


def evaluate(
    candles,
    start,
    horizon,
    entry_price,
    target,
    timeframe,
):
    available = max(0, len(candles) - start)
    observed = min(available, horizon)

    if observed <= 0:
        return {
            "complete": False,
            "observed": 0,
            "hit": None,
            "first_hit_bars": None,
            "first_hit_timestamp": None,
            "max_high": None,
            "min_low": None,
            "max_return_pct": None,
            "max_drawdown_pct": None,
            "gap": False,
        }

    window = candles[start:start + observed]
    step = TIMEFRAMES[timeframe]

    gap = False

    for index in range(1, len(window)):
        delta = (
            window[index]["timestamp"]
            - window[index - 1]["timestamp"]
        )

        if delta != step:
            gap = True
            break

    highs = [x["high"] for x in window]
    lows = [x["low"] for x in window]

    first = None

    for index, candle in enumerate(window, 1):
        if candle["high"] >= target:
            first = (
                index,
                candle["timestamp"].isoformat(),
            )
            break

    max_high = max(highs)
    min_low = min(lows)

    complete = (
        observed == horizon
        and not gap
    )

    return {
        "complete": complete,
        "observed": observed,
        "hit": (
            bool(first)
            if complete
            else None
        ),
        "first_hit_bars": (
            first[0] if first else None
        ),
        "first_hit_timestamp": (
            first[1] if first else None
        ),
        "max_high": max_high,
        "min_low": min_low,
        "max_return_pct": (
            max_high / entry_price - 1
        ) * 100,
        "max_drawdown_pct": (
            min_low / entry_price - 1
        ) * 100,
        "gap": gap,
    }


def compare_record(row, outcome):
    if outcome["gap"]:
        return "WINDOW_GAP"

    if not outcome["complete"]:
        return "CENSORED"

    try:
        expected_status = (
            "HIT" if outcome["hit"] else "MISS"
        )

        if (
            str(row["status"]).strip().upper()
            != expected_status
        ):
            return "STATUS_MISMATCH"

        expected_hit = (
            1 if outcome["hit"] else 0
        )

        if integer(row["hit_30pct"]) != expected_hit:
            return "HIT_FLAG_MISMATCH"

        if (
            integer(row["observed_bars"])
            != outcome["observed"]
        ):
            return "OBSERVED_BARS_MISMATCH"

        if (
            integer(row["required_bars"])
            != integer(row["horizon"])
        ):
            return "REQUIRED_BARS_MISMATCH"

        for field in [
            "max_high",
            "max_return_pct",
            "min_low",
            "max_drawdown_pct",
        ]:
            actual = number(row[field])
            expected = outcome[field]

            if not near(actual, expected):
                return field.upper() + "_MISMATCH"

        first_bars = str(
            row["first_hit_bars"] or ""
        ).strip()

        first_time = str(
            row["first_hit_timestamp"] or ""
        ).strip()

        if outcome["hit"]:
            if not first_bars:
                return "FIRST_HIT_BARS_MISSING"

            if (
                integer(first_bars)
                != outcome["first_hit_bars"]
            ):
                return "FIRST_HIT_BARS_MISMATCH"

            if not first_time:
                return "FIRST_HIT_TIME_MISSING"

            if (
                timestamp(first_time).isoformat()
                != outcome["first_hit_timestamp"]
            ):
                return "FIRST_HIT_TIME_MISMATCH"

        elif first_bars or first_time:
            return "UNEXPECTED_FIRST_HIT"

        return "MATCH"

    except (ValueError, TypeError, KeyError):
        return "RECORDED_FIELD_INVALID"


def process_file(
    checkpoint,
    ohlcv_path,
    timeframe,
    market,
    detail_path,
):
    checkpoint_hash = sha256(checkpoint)
    source_hash = sha256(ohlcv_path)

    candles = load_ohlcv(
        ohlcv_path,
        timeframe,
    )

    positions = {
        candle["timestamp"]: index
        for index, candle in enumerate(candles)
    }

    counts = Counter()
    total_rows = 0

    detail_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary = tempfile.mkstemp(
        prefix=".tmp_detail_",
        dir=str(detail_path.parent),
    )

    try:
        with (
            open(
                checkpoint,
                "r",
                encoding="utf-8-sig",
                newline="",
            ) as source,
            os.fdopen(
                fd,
                "w",
                encoding="utf-8",
                newline="",
            ) as output,
        ):
            reader = csv.DictReader(source)

            missing = REQUIRED - set(
                reader.fieldnames or []
            )

            if missing:
                raise ValueError(
                    "CHECKPOINT_COLUMNS_MISSING:"
                    + ",".join(sorted(missing))
                )

            writer = csv.DictWriter(
                output,
                fieldnames=DETAIL_COLUMNS,
                extrasaction="ignore",
            )

            writer.writeheader()

            for row_no, row in enumerate(reader, 2):
                total_rows += 1

                if row["timeframe"] != timeframe:
                    raise ValueError(
                        "TIMEFRAME_MISMATCH:"
                        + str(row_no)
                    )

                if row["market"] != market:
                    raise ValueError(
                        "MARKET_MISMATCH:"
                        + str(row_no)
                    )

                if (
                    row["source_ohlcv_sha256"].lower()
                    != source_hash.lower()
                ):
                    raise ValueError(
                        "SOURCE_SHA256_MISMATCH:"
                        + str(row_no)
                    )

                detail = {
                    "timeframe": timeframe,
                    "market": market,
                    "checkpoint_row": row_no,
                    "signal_timestamp": row[
                        "signal_timestamp"
                    ],
                    "entry_timestamp": row[
                        "entry_timestamp"
                    ],
                    "horizon": row["horizon"],
                    "recorded_status": row["status"],
                    "include_result": "",
                    "exclude_result": "",
                    "classification": "",
                    "include_observed": "",
                    "exclude_observed": "",
                    "include_max_return_pct": "",
                    "exclude_max_return_pct": "",
                    "include_max_drawdown_pct": "",
                    "exclude_max_drawdown_pct": "",
                }

                try:
                    entry_time = timestamp(
                        row["entry_timestamp"]
                    )

                    entry_price = number(
                        row["entry_price"]
                    )

                    horizon = integer(
                        row["horizon"]
                    )

                    target_pct = number(
                        row["target_return_pct"]
                    )

                    target = number(
                        row["target_price"]
                    )

                    if entry_price <= 0 or horizon <= 0:
                        raise ValueError(
                            "INVALID_ENTRY_OR_HORIZON"
                        )

                    expected_target = entry_price * (
                        1 + target_pct / 100
                    )

                    if not near(
                        target,
                        expected_target,
                    ):
                        raise ValueError(
                            "TARGET_PRICE_MISMATCH"
                        )

                    if entry_time not in positions:
                        raise ValueError(
                            "ENTRY_TIMESTAMP_NOT_IN_OHLCV"
                        )

                    start = positions[entry_time]

                    if not near(
                        candles[start]["open"],
                        entry_price,
                    ):
                        raise ValueError(
                            "ENTRY_OPEN_PRICE_MISMATCH"
                        )

                    include = evaluate(
                        candles,
                        start,
                        horizon,
                        entry_price,
                        target,
                        timeframe,
                    )

                    exclude = evaluate(
                        candles,
                        start + 1,
                        horizon,
                        entry_price,
                        target,
                        timeframe,
                    )

                    a = compare_record(
                        row,
                        include,
                    )

                    b = compare_record(
                        row,
                        exclude,
                    )

                    detail["include_result"] = a
                    detail["exclude_result"] = b

                    detail["include_observed"] = (
                        include["observed"]
                    )

                    detail["exclude_observed"] = (
                        exclude["observed"]
                    )

                    for prefix, outcome in (
                        ("include", include),
                        ("exclude", exclude),
                    ):
                        for field in (
                            "max_return_pct",
                            "max_drawdown_pct",
                        ):
                            value = outcome[field]

                            detail[
                                prefix + "_" + field
                            ] = (
                                "" if value is None
                                else value
                            )

                    if a == "MATCH" and b == "MATCH":
                        classification = "BOTH_MATCH"

                    elif a == "MATCH":
                        classification = "INCLUDE_MATCH"

                    elif b == "MATCH":
                        classification = "EXCLUDE_MATCH"

                    elif (
                        a == "WINDOW_GAP"
                        or b == "WINDOW_GAP"
                    ):
                        classification = "WINDOW_GAP"

                    elif (
                        a == "CENSORED"
                        or b == "CENSORED"
                    ):
                        classification = (
                            "INCOMPLETE_COMPARISON"
                        )

                    else:
                        classification = "NEITHER_MATCH"

                except (
                    ValueError,
                    TypeError,
                    KeyError,
                ) as error:
                    classification = (
                        "ROW_ERROR:"
                        + str(error)
                    )

                detail["classification"] = classification

                counts[classification] += 1
                writer.writerow(detail)

            output.flush()
            os.fsync(output.fileno())

        if total_rows == 0:
            raise ValueError("EMPTY_CHECKPOINT")

        os.replace(
            temporary,
            detail_path,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

    return {
        "version": VERSION,
        "timeframe": timeframe,
        "market": market,
        "checkpoint_sha256": checkpoint_hash,
        "ohlcv_sha256": source_hash,
        "detail_sha256": sha256(detail_path),
        "rows": total_rows,
        "counts": dict(counts),
    }


def cache_paths(report, timeframe, market):
    folder = report / "cache" / timeframe

    return (
        folder / (market + ".json"),
        folder / (market + ".csv"),
    )


def cache_valid(
    metadata_path,
    detail_path,
    checkpoint_hash,
    ohlcv_hash,
):
    if not (
        metadata_path.is_file()
        and detail_path.is_file()
    ):
        return None

    try:
        obj = json.loads(
            metadata_path.read_text(
                encoding="utf-8"
            )
        )

        payload = obj["payload"]

        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        digest = hashlib.sha256(
            encoded
        ).hexdigest()

        if obj["payload_sha256"] != digest:
            return None

        if payload["version"] != VERSION:
            return None

        if (
            payload["checkpoint_sha256"]
            != checkpoint_hash
        ):
            return None

        if (
            payload["ohlcv_sha256"]
            != ohlcv_hash
        ):
            return None

        if (
            payload["detail_sha256"]
            != sha256(detail_path)
        ):
            return None

        if payload["rows"] <= 0:
            return None

        if (
            sum(payload["counts"].values())
            != payload["rows"]
        ):
            return None

        return payload

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
    ):
        return None


def save_cache(path, payload):
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    atomic_json(path, {
        "payload": payload,
        "payload_sha256": hashlib.sha256(
            encoded
        ).hexdigest(),
    })


def combine_details(report, entries, output_path):
    fd, temporary = tempfile.mkstemp(
        prefix=".tmp_combined_",
        dir=str(report),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as output:

            writer = csv.writer(output)
            writer.writerow(DETAIL_COLUMNS)

            for path in entries:
                with open(
                    path,
                    "r",
                    encoding="utf-8",
                    newline="",
                ) as source:

                    reader = csv.DictReader(source)

                    if reader.fieldnames != DETAIL_COLUMNS:
                        raise ValueError(
                            "DETAIL_SCHEMA_MISMATCH:"
                            + str(path)
                        )

                    for row in reader:
                        writer.writerow([
                            row[column]
                            for column in DETAIL_COLUMNS
                        ])

            output.flush()
            os.fsync(output.fileno())

        os.replace(
            temporary,
            output_path,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--fresh",
        action="store_true",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source = (
        root
        / "data"
        / "reports"
        / "pattern_256_accumulation_30pct"
        / "checkpoints"
    )

    report = (
        root
        / "data"
        / "reports"
        / "pattern_256_outcomes_full_market"
    )

    report.mkdir(
        parents=True,
        exist_ok=True,
    )

    counts = Counter()
    errors = []
    input_hashes = {}
    detail_entries = []

    total_files = 0
    successful_files = 0
    resumed_files = 0
    total_rows = 0

    for timeframe in TIMEFRAMES:
        folder = source / timeframe

        files = sorted(
            folder.glob("KRW-*.csv")
        )

        if not files:
            errors.append(
                "NO_CHECKPOINT_FILES:"
                + timeframe
            )
            continue

        for checkpoint in files:
            total_files += 1

            market = checkpoint.stem

            ohlcv_path = (
                root
                / "data"
                / "ohlcv"
                / timeframe
                / checkpoint.name
            )

            key = (
                timeframe
                + "/"
                + market
            )

            if not ohlcv_path.is_file():
                errors.append(
                    "MISSING_OHLCV:" + key
                )
                continue

            try:
                checkpoint_hash = sha256(
                    checkpoint
                )

                ohlcv_hash = sha256(
                    ohlcv_path
                )

                input_hashes[key] = {
                    "checkpoint_sha256": checkpoint_hash,
                    "ohlcv_sha256": ohlcv_hash,
                }

                metadata_path, detail_path = cache_paths(
                    report,
                    timeframe,
                    market,
                )

                payload = None
                resumed = False

                if not args.fresh:
                    payload = cache_valid(
                        metadata_path,
                        detail_path,
                        checkpoint_hash,
                        ohlcv_hash,
                    )

                if payload is None:
                    payload = process_file(
                        checkpoint,
                        ohlcv_path,
                        timeframe,
                        market,
                        detail_path,
                    )

                    save_cache(
                        metadata_path,
                        payload,
                    )

                else:
                    resumed = True
                    resumed_files += 1

                successful_files += 1
                total_rows += payload["rows"]

                counts.update(
                    payload["counts"]
                )

                detail_entries.append(
                    detail_path
                )

                print(
                    "[OK] "
                    + key
                    + " rows="
                    + str(payload["rows"])
                    + " resumed="
                    + str(resumed),
                    flush=True,
                )

            except Exception as error:
                message = (
                    key
                    + ":"
                    + type(error).__name__
                    + ":"
                    + str(error)
                )

                errors.append(message)

                print(
                    "[ERROR] " + message,
                    flush=True,
                )

    detail_output = (
        report
        / "outcome_verification_detail.csv"
    )

    result_output = (
        report
        / "outcome_verification_result.json"
    )

    manifest_output = (
        report
        / "outcome_verification_manifest.json"
    )

    combined_ok = False

    try:
        combine_details(
            report,
            detail_entries,
            detail_output,
        )

        combined_ok = True

    except Exception as error:
        errors.append(
            "COMBINE_DETAILS:"
            + type(error).__name__
            + ":"
            + str(error)
        )

    complete = (
        total_files > 0
        and successful_files == total_files
        and total_rows > 0
        and sum(counts.values()) == total_rows
        and not errors
        and combined_ok
    )

    review_count = sum(
        value
        for name, value in counts.items()
        if (
            name == "NEITHER_MATCH"
            or name == "WINDOW_GAP"
            or name == "INCOMPLETE_COMPARISON"
            or name.startswith("ROW_ERROR:")
        )
    )

    if not complete:
        classification = "FAIL"

    elif review_count:
        classification = "REVIEW"

    else:
        classification = "PASS"

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": (
            "PASS" if complete else "FAIL"
        ),
        "classification": classification,
        "research_decision": "HOLD",
        "files_discovered": total_files,
        "files_processed": successful_files,
        "files_resumed": resumed_files,
        "rows_checked": total_rows,
        "classification_counts": dict(
            sorted(counts.items())
        ),
        "errors": errors,
        "limitations": [
            "Uses current OHLCV snapshots",
            "Does not prove point-in-time availability",
            "Entry convention is not established",
            "Does not independently reconstruct signals",
            "Does not prove independent OOS",
            "Does not establish executable fills",
            "Does not prove profitability",
            "Incomplete or gapped windows require review",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    atomic_json(
        result_output,
        result,
    )

    outputs = {
        result_output.name: sha256(
            result_output
        ),
    }

    if combined_ok:
        outputs[detail_output.name] = sha256(
            detail_output
        )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "inputs": input_hashes,
        "outputs": outputs,
    }

    atomic_json(
        manifest_output,
        manifest,
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
