
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 OHLCV Outcome Replay
Clean V001

- Reads the verified horizon sample.
- Uses gap = elapsed hours.
- Uses independent = candle bars.
- Replays independent +30% high-price hits.
- Does not invent missing gap entry prices.
- Does not modify original research data.
- Writes SHA256 verified research artifacts.
- Research decision remains HOLD.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

OUTPUT_FIELDS = [
    "source",
    "source_line",
    "timeframe",
    "market",
    "horizon",
    "entry_epoch",
    "entry_price_reported",
    "entry_open_ohlcv",
    "entry_price_difference_pct",
    "expected_bars",
    "available_bars",
    "contiguous_bars",
    "max_high",
    "replay_hit_30pct",
    "reported_hit_30pct",
    "hit_agreement",
    "first_hit_bar",
    "status",
    "reason",
]

VERSION = "Clean V001"


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def write_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )


def to_float(value):
    value = str(value or "").strip()

    if not value:
        return None

    result = float(value)

    if not math.isfinite(result):
        raise ValueError("Nonfinite numeric value")

    return result


def parse_epoch(value):
    value = str(value or "").strip()

    if not value:
        raise ValueError("Missing timestamp")

    try:
        result = float(value)

        if not math.isfinite(result):
            raise ValueError("Invalid timestamp")

        if abs(result) > 100000000000:
            result /= 1000.0

        return result

    except ValueError:
        timestamp = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(
                tzinfo=timezone.utc
            )

        return timestamp.timestamp()


def parse_flag(value):
    text = str(value or "").strip().lower()

    if text in ("true", "1", "yes", "hit"):
        return True

    if text in ("false", "0", "no", "miss"):
        return False

    return None


def load_candles(path):
    candles = []

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        required = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
        ]

        missing = [
            name for name in required
            if name not in (reader.fieldnames or [])
        ]

        if missing:
            raise ValueError(
                "Missing OHLCV columns: "
                + ",".join(missing)
            )

        previous = None

        for line, row in enumerate(reader, 2):
            epoch = parse_epoch(row["timestamp"])

            if previous is not None:
                if epoch <= previous:
                    raise ValueError(
                        f"Unordered timestamps at line {line}"
                    )

            previous = epoch

            prices = [
                to_float(row[name])
                for name in (
                    "open",
                    "high",
                    "low",
                    "close",
                )
            ]

            if any(
                value is None or value <= 0
                for value in prices
            ):
                raise ValueError(
                    f"Invalid OHLCV prices at line {line}"
                )

            op, high, low, close = prices

            if high < max(op, close, low):
                raise ValueError(
                    f"Invalid high at line {line}"
                )

            if low > min(op, close, high):
                raise ValueError(
                    f"Invalid low at line {line}"
                )

            candles.append({
                "epoch": epoch,
                "open": op,
                "high": high,
                "low": low,
                "close": close,
            })

    if not candles:
        raise ValueError("Empty OHLCV file")

    return candles


def index_candles(candles):
    return {
        round(candle["epoch"]): index
        for index, candle in enumerate(candles)
    }


def result_template(row):
    return {
        "source": row.get("source", ""),
        "source_line": row.get("source_line", ""),
        "timeframe": row.get("timeframe", ""),
        "market": row.get("market", ""),
        "horizon": row.get("horizon", ""),
        "entry_epoch": row.get("entry_epoch", ""),
        "entry_price_reported": row.get(
            "entry_price", ""
        ),
        "entry_open_ohlcv": "",
        "entry_price_difference_pct": "",
        "expected_bars": "",
        "available_bars": 0,
        "contiguous_bars": False,
        "max_high": "",
        "replay_hit_30pct": "",
        "reported_hit_30pct": row.get(
            "hit_30pct", ""
        ),
        "hit_agreement": "",
        "first_hit_bar": "",
        "status": "NOT_EVALUATED",
        "reason": "",
    }


def replay(row, candles, lookup):
    result = result_template(row)

    source = row["source"].strip().lower()
    timeframe = row["timeframe"].strip().lower()

    if source not in ("gap", "independent"):
        raise ValueError("Unknown source")

    if timeframe not in SECONDS:
        raise ValueError("Unknown timeframe")

    horizon = to_float(row["horizon"])
    entry_epoch = parse_epoch(
        row["entry_epoch"]
    )

    if horizon is None or horizon <= 0:
        raise ValueError("Invalid horizon")

    entry_index = lookup.get(
        round(entry_epoch)
    )

    if entry_index is None:
        result["status"] = "NOT_COMPARABLE"
        result["reason"] = "ENTRY_CANDLE_MISSING"
        return result

    entry_candle = candles[entry_index]

    result["entry_open_ohlcv"] = (
        entry_candle["open"]
    )

    interval = SECONDS[timeframe]

    if source == "gap":
        expected = math.ceil(
            horizon * 3600 / interval
        )

    else:
        if not horizon.is_integer():
            result["status"] = "NOT_COMPARABLE"
            result["reason"] = (
                "NON_INTEGER_BAR_HORIZON"
            )
            return result

        expected = int(horizon)

    if expected < 1:
        result["status"] = "NOT_COMPARABLE"
        result["reason"] = "INVALID_EXPECTED_BARS"
        return result

    result["expected_bars"] = expected

    # Entry candle is included in the window.
    # This is a candidate convention, not yet
    # proof of the original evaluation convention.
    window = candles[
        entry_index:entry_index + expected
    ]

    result["available_bars"] = len(window)

    if len(window) != expected:
        result["status"] = "NOT_COMPARABLE"
        result["reason"] = "INCOMPLETE_WINDOW"
        return result

    for index, candle in enumerate(window):
        expected_epoch = (
            entry_epoch + index * interval
        )

        if abs(
            candle["epoch"] - expected_epoch
        ) > 1:
            result["status"] = "NOT_COMPARABLE"
            result["reason"] = (
                "NON_CONTIGUOUS_WINDOW"
            )
            return result

    result["contiguous_bars"] = True

    if source == "gap":
        result["status"] = "WINDOW_VERIFIED"
        result["reason"] = (
            "GAP_ENTRY_PRICE_NOT_AVAILABLE"
        )
        return result

    entry_price = to_float(
        row.get("entry_price")
    )

    if entry_price is None or entry_price <= 0:
        result["status"] = "NOT_COMPARABLE"
        result["reason"] = "INVALID_ENTRY_PRICE"
        return result

    difference = (
        entry_price / entry_candle["open"] - 1
    ) * 100

    result[
        "entry_price_difference_pct"
    ] = difference

    threshold = entry_price * 1.30

    max_high = max(
        candle["high"]
        for candle in window
    )

    result["max_high"] = max_high

    first_hit = None

    for index, candle in enumerate(
        window, 1
    ):
        if candle["high"] >= threshold:
            first_hit = index
            break

    replay_hit = first_hit is not None

    result["replay_hit_30pct"] = replay_hit

    if first_hit is not None:
        result["first_hit_bar"] = first_hit

    reported = parse_flag(
        row.get("hit_30pct")
    )

    if reported is None:
        result["status"] = "REPLAY_ONLY"
        result["reason"] = (
            "REPORTED_HIT_UNKNOWN"
        )
        return result

    agreement = replay_hit == reported

    result["hit_agreement"] = agreement

    if agreement:
        result["status"] = "MATCH"
        result["reason"] = (
            "REPLAY_AGREES_WITH_REPORTED"
        )

    else:
        result["status"] = "MISMATCH"
        result["reason"] = (
            "REPLAY_DIFFERS_FROM_REPORTED"
        )

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--samples",
        required=True,
    )

    parser.add_argument(
        "--manifest",
        required=True,
    )

    parser.add_argument(
        "--output",
        required=True,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    samples = Path(args.samples).resolve()
    manifest_path = Path(
        args.manifest
    ).resolve()
    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError("Missing data root")

    if not samples.is_file():
        raise RuntimeError("Missing samples")

    if not manifest_path.is_file():
        raise RuntimeError("Missing manifest")

    if output.exists():
        raise RuntimeError(
            "Output directory already exists"
        )

    if output == root or root in output.parents:
        raise RuntimeError(
            "Output must be outside data root"
        )

    manifest = json.loads(
        manifest_path.read_text(
            encoding="utf-8-sig"
        )
    )

    expected_hash = manifest[
        "output_sha256"
    ]["horizon_samples.csv"]

    if sha256(samples) != expected_hash:
        raise RuntimeError(
            "Input sample SHA256 mismatch"
        )

    if manifest.get("execution") != "PASS":
        raise RuntimeError(
            "Input audit did not pass"
        )

    with samples.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        required = [
            "source",
            "source_line",
            "timeframe",
            "market",
            "entry_epoch",
            "horizon",
            "entry_price",
            "hit_30pct",
        ]

        missing = [
            name for name in required
            if name not in (
                reader.fieldnames or []
            )
        ]

        if missing:
            raise RuntimeError(
                "Missing sample columns: "
                + ",".join(missing)
            )

        events = list(reader)

    if not events:
        raise RuntimeError("Empty sample")

    output.mkdir(
        parents=True,
        exist_ok=False,
    )

    grouped = {}

    for event in events:
        key = (
            event["timeframe"].strip().lower(),
            event["market"].strip().upper(),
        )

        grouped.setdefault(key, []).append(
            event
        )

    records = []
    status_counts = Counter()
    source_counts = Counter()
    errors = []

    for number, (key, group) in enumerate(
        sorted(grouped.items()), 1
    ):
        timeframe, market = key

        path = (
            root
            / "data"
            / "ohlcv"
            / timeframe
            / f"{market}.csv"
        )

        print(
            f"[{number}/{len(grouped)}] "
            f"{timeframe} {market}",
            flush=True,
        )

        if not path.is_file():
            for event in group:
                result = result_template(event)
                result["status"] = "NOT_COMPARABLE"
                result["reason"] = (
                    "OHLCV_FILE_MISSING"
                )
                records.append(result)

            continue

        try:
            candles = load_candles(path)
            lookup = index_candles(candles)

        except Exception as exc:
            errors.append({
                "timeframe": timeframe,
                "market": market,
                "error": str(exc),
            })

            for event in group:
                result = result_template(event)
                result["status"] = "NOT_COMPARABLE"
                result["reason"] = (
                    "OHLCV_FILE_INVALID"
                )
                records.append(result)

            continue

        for event in group:
            try:
                result = replay(
                    event,
                    candles,
                    lookup,
                )

            except Exception as exc:
                result = result_template(event)
                result["status"] = "ERROR"
                result["reason"] = str(exc)

            records.append(result)

    for record in records:
        status_counts[
            record["status"]
        ] += 1

        source_counts[
            (
                record["source"],
                record["timeframe"],
                record["status"],
            )
        ] += 1

    detail_path = (
        output / "replay_details.csv"
    )

    with detail_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=OUTPUT_FIELDS,
        )

        writer.writeheader()

        for record in records:
            writer.writerow(record)

    result = {
        "audit": "pattern_256_ohlcv_outcome_replay",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "sampled_events": len(events),
        "processed_events": len(records),
        "market_timeframe_pairs": len(grouped),
        "status_counts": dict(
            sorted(status_counts.items())
        ),
        "source_timeframe_status": [
            {
                "source": source,
                "timeframe": timeframe,
                "status": status,
                "count": count,
            }
            for (
                source,
                timeframe,
                status,
            ), count in sorted(
                source_counts.items()
            )
        ],
        "ohlcv_file_errors": errors,
        "gap_horizon_interpretation": (
            "elapsed_hours"
        ),
        "independent_horizon_interpretation": (
            "candle_bars"
        ),
        "entry_candle_convention": (
            "included_as_first_evaluation_bar"
        ),
        "limitations": [
            "Sample audit, not full population replay",
            "Gap hit outcome cannot be replayed without entry price",
            "Entry candle inclusion convention is not proven",
            "Reported outcome status and hit flag may differ",
            "OHLCV high does not prove executable profit",
            "Intrabar execution and fees not modeled",
            "No independent OOS validation",
        ],
        "horizon_unit_verified": False,
        "outcome_replay_performed": True,
        "full_population_replay_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    result_path = (
        output / "replay_result.json"
    )

    manifest_output = (
        output / "replay_manifest.json"
    )

    write_json(result_path, result)

    output_manifest = {
        "audit": result["audit"],
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "input_sha256": {
            "horizon_samples.csv": sha256(
                samples
            ),
            "horizon_manifest.json": sha256(
                manifest_path
            ),
        },
        "output_sha256": {
            "replay_result.json": sha256(
                result_path
            ),
            "replay_details.csv": sha256(
                detail_path
            ),
        },
        "research_decision": "HOLD",
    }

    write_json(
        manifest_output,
        output_manifest,
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
