
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pattern 256 Full-Market Independent Outcome Verification
Clean V001

- Read-only source verification
- Full checkpoint population
- Independent OHLCV outcome replay
- Entry-candle INCLUDE / EXCLUDE hypotheses
- Strict source SHA256 checks
- Per-file SHA256-verified resume
- Atomic report/checkpoint writes
- No training/trading approval

Run:
  py verify_pattern_256_outcomes_full_market.py --root .
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_outcomes_full_market"
REQUIRED = {
    "timeframe", "market", "signal_timestamp",
    "entry_timestamp", "entry_price", "horizon",
    "target_return_pct", "target_price", "status",
    "hit_30pct", "first_hit_bars",
    "first_hit_timestamp", "max_high",
    "max_return_pct", "min_low", "max_drawdown_pct",
    "observed_bars", "required_bars",
    "source_ohlcv_sha256"
}
NUMERIC_FIELDS = [
    "target_price", "max_high", "max_return_pct",
    "min_low", "max_drawdown_pct"
]
TIMEFRAMES = ("h1", "h4", "d1")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".tmp_", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8",
                       newline="") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_json(path, obj):
    atomic_text(
        path,
        json.dumps(obj, ensure_ascii=False,
                   indent=2, sort_keys=True) + "\n"
    )


def write_csv(path, rows, columns):
    import io
    s = io.StringIO(newline="")
    writer = csv.DictWriter(
        s, fieldnames=columns, extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)
    atomic_text(path, s.getvalue())


def read_csv(path):
    with open(path, "r", encoding="utf-8-sig",
              newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV_HEADER_MISSING")
        return list(reader)


def timestamp(value):
    value = str(value or "").strip()
    if not value:
        raise ValueError("TIMESTAMP_EMPTY")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("TIMEZONE_MISSING")
    return dt.astimezone(timezone.utc)


def number(value):
    x = float(value)
    if not math.isfinite(x):
        raise ValueError("NONFINITE_NUMBER")
    return x


def integer(value):
    x = number(value)
    if not x.is_integer():
        raise ValueError("NOT_INTEGER")
    return int(x)


def near(a, b):
    return math.isclose(
        a, b, rel_tol=1e-8, abs_tol=1e-7
    )


def normalize_header(name):
    return str(name).strip().lower().replace("-", "_")


def pick_column(names, candidates):
    lookup = {
        normalize_header(name): name for name in names
    }
    for candidate in candidates:
        if candidate in lookup:
            return lookup[candidate]
    return None


def load_ohlcv(path):
    with open(path, "r", encoding="utf-8-sig",
              newline="") as f:
        reader = csv.DictReader(f)
        names = reader.fieldnames or []

        time_col = pick_column(names, [
            "timestamp", "candle_date_time_utc",
            "datetime", "time", "date"
        ])
        high_col = pick_column(names, [
            "high", "high_price", "trade_price_high"
        ])
        low_col = pick_column(names, [
            "low", "low_price", "trade_price_low"
        ])
        open_col = pick_column(names, [
            "open", "opening_price", "open_price"
        ])

        if not all([time_col, high_col,
                    low_col, open_col]):
            raise ValueError(
                "OHLCV_SCHEMA_UNSUPPORTED:" +
                ",".join(names)
            )

        candles = []
        previous = None

        for line_no, row in enumerate(reader, 2):
            dt = timestamp(row[time_col])
            high = number(row[high_col])
            low = number(row[low_col])
            opening = number(row[open_col])

            if min(high, low, opening) <= 0:
                raise ValueError(
                    f"NONPOSITIVE_PRICE_LINE_{line_no}"
                )
            if low > high or not (
                low <= opening <= high
            ):
                raise ValueError(
                    f"INVALID_OHLC_LINE_{line_no}"
                )
            if previous is not None and dt <= previous:
                raise ValueError(
                    f"TIMESTAMP_ORDER_LINE_{line_no}"
                )

            candles.append({
                "timestamp": dt,
                "high": high,
                "low": low,
                "open": opening
            })
            previous = dt

    return candles


def evaluate(candles, start, horizon,
             entry_price, target):
    available = max(0, len(candles) - start)
    observed = min(available, horizon)
    complete = observed == horizon

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
            "max_drawdown_pct": None
        }

    window = candles[start:start + observed]
    highs = [x["high"] for x in window]
    lows = [x["low"] for x in window]

    first = None
    for i, candle in enumerate(window, 1):
        if candle["high"] >= target:
            first = (i, candle["timestamp"])
            break

    max_high = max(highs)
    min_low = min(lows)

    return {
        "complete": complete,
        "observed": observed,
        "hit": bool(first) if complete else None,
        "first_hit_bars": first[0] if first else None,
        "first_hit_timestamp": (
            first[1].isoformat() if first else None
        ),
        "max_high": max_high,
        "min_low": min_low,
        "max_return_pct": (
            max_high / entry_price - 1
        ) * 100,
        "max_drawdown_pct": (
            min_low / entry_price - 1
        ) * 100
    }


def compare_record(row, outcome):
    if not outcome["complete"]:
        return "CENSORED"

    try:
        expected_status = (
            "HIT" if outcome["hit"] else "MISS"
        )
        status = str(row["status"]).strip().upper()

        if status != expected_status:
            return "STATUS_MISMATCH"

        expected_hit = 1 if outcome["hit"] else 0
        if integer(row["hit_30pct"]) != expected_hit:
            return "HIT_FLAG_MISMATCH"

        if integer(row["observed_bars"]) != (
            outcome["observed"]
        ):
            return "OBSERVED_BARS_MISMATCH"

        if integer(row["required_bars"]) != integer(
            row["horizon"]
        ):
            return "REQUIRED_BARS_MISMATCH"

        for field in NUMERIC_FIELDS:
            if field == "target_price":
                continue
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
            if not first_bars or (
                integer(first_bars) !=
                outcome["first_hit_bars"]
            ):
                return "FIRST_HIT_BARS_MISMATCH"
            if not first_time or (
                timestamp(first_time).isoformat() !=
                outcome["first_hit_timestamp"]
            ):
                return "FIRST_HIT_TIME_MISMATCH"
        elif first_bars or first_time:
            return "UNEXPECTED_FIRST_HIT"

        return "MATCH"

    except (ValueError, TypeError, KeyError):
        return "RECORDED_FIELD_INVALID"


def process_file(checkpoint, ohlcv_path,
                 timeframe, market):
    checkpoint_hash = sha256(checkpoint)
    source_hash = sha256(ohlcv_path)

    records = read_csv(checkpoint)
    if not records:
        raise ValueError("EMPTY_CHECKPOINT")

    missing = REQUIRED - set(records[0])
    if missing:
        raise ValueError(
            "CHECKPOINT_COLUMNS_MISSING:" +
            ",".join(sorted(missing))
        )

    for row in records:
        if row["timeframe"] != timeframe:
            raise ValueError("TIMEFRAME_MISMATCH")
        if row["market"] != market:
            raise ValueError("MARKET_MISMATCH")
        if (
            row["source_ohlcv_sha256"].lower()
            != source_hash.lower()
        ):
            raise ValueError(
                "SOURCE_SHA256_MISMATCH"
            )

    candles = load_ohlcv(ohlcv_path)
    positions = {
        x["timestamp"]: i
        for i, x in enumerate(candles)
    }

    details = []
    counts = Counter()

    for row_no, row in enumerate(records, 2):
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
            "exclude_max_drawdown_pct": ""
        }

        try:
            entry_time = timestamp(
                row["entry_timestamp"]
            )
            entry_price = number(
                row["entry_price"]
            )
            horizon = integer(row["horizon"])
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

            calculated_target = entry_price * (
                1 + target_pct / 100
            )
            if not near(target, calculated_target):
                raise ValueError(
                    "TARGET_PRICE_MISMATCH"
                )

            if entry_time not in positions:
                raise ValueError(
                    "ENTRY_TIMESTAMP_NOT_IN_OHLCV"
                )

            start = positions[entry_time]

            if not near(
                candles[start]["open"], entry_price
            ):
                raise ValueError(
                    "ENTRY_OPEN_PRICE_MISMATCH"
                )

            include = evaluate(
                candles, start, horizon,
                entry_price, target
            )
            exclude = evaluate(
                candles, start + 1, horizon,
                entry_price, target
            )

            a = compare_record(row, include)
            b = compare_record(row, exclude)

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
                ("exclude", exclude)
            ):
                for field in (
                    "max_return_pct",
                    "max_drawdown_pct"
                ):
                    value = outcome[field]
                    detail[prefix + "_" + field] = (
                        "" if value is None else value
                    )

            if a == "MATCH" and b == "MATCH":
                result = "BOTH_MATCH"
            elif a == "MATCH":
                result = "INCLUDE_MATCH"
            elif b == "MATCH":
                result = "EXCLUDE_MATCH"
            elif (
                a == "CENSORED" or
                b == "CENSORED"
            ):
                result = "INCOMPLETE_COMPARISON"
            else:
                result = "NEITHER_MATCH"

            detail["classification"] = result

        except (ValueError, TypeError, KeyError) as e:
            detail["classification"] = (
                "ROW_ERROR:" + str(e)
            )

        counts[detail["classification"]] += 1
        details.append(detail)

    return {
        "checkpoint_sha256": checkpoint_hash,
        "ohlcv_sha256": source_hash,
        "rows": len(records),
        "counts": dict(counts),
        "details": details
    }


def cache_path(root, timeframe, market):
    return (
        root / "cache" / timeframe /
        (market + ".json")
    )


def cache_valid(path, checkpoint_hash,
                ohlcv_hash):
    if not path.exists():
        return None

    try:
        obj = json.loads(
            path.read_text(encoding="utf-8")
        )
        payload = obj["payload"]
        digest = hashlib.sha256(
            json.dumps(
                payload, ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()

        if obj["payload_sha256"] != digest:
            return None

        if payload["version"] != VERSION:
            return None

        if payload["checkpoint_sha256"] != (
            checkpoint_hash
        ):
            return None

        if payload["ohlcv_sha256"] != ohlcv_hash:
            return None

        return payload

    except (OSError, ValueError, KeyError,
            TypeError):
        return None


def save_cache(path, payload):
    encoded = json.dumps(
        payload, ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":")
    ).encode("utf-8")

    write_json(path, {
        "payload": payload,
        "payload_sha256": hashlib.sha256(
            encoded
        ).hexdigest()
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", default="."
    )
    parser.add_argument(
        "--fresh", action="store_true"
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = (
        root / "data" / "reports" /
        "pattern_256_accumulation_30pct" /
        "checkpoints"
    )
    report = (
        root / "data" / "reports" /
        "pattern_256_outcomes_full_market"
    )

    report.mkdir(parents=True, exist_ok=True)

    all_details = []
    all_counts = Counter()
    errors = []
    total_files = 0
    resumed = 0
    total_rows = 0
    input_hashes = {}

    for tf in TIMEFRAMES:
        folder = source / tf
        files = sorted(folder.glob("KRW-*.csv"))

        if not files:
            errors.append(
                f"NO_CHECKPOINT_FILES:{tf}"
            )
            continue

        for checkpoint in files:
            market = checkpoint.stem
            ohlcv = (
                root / "data" / "ohlcv" /
                tf / checkpoint.name
            )
            total_files += 1

            if not ohlcv.is_file():
                errors.append(
                    f"MISSING_OHLCV:{tf}/{market}"
                )
                continue

            try:
                ch = sha256(checkpoint)
                oh = sha256(ohlcv)

                key = f"{tf}/{market}"
                input_hashes[key] = {
                    "checkpoint_sha256": ch,
                    "ohlcv_sha256": oh
                }

                cp = cache_path(
                    report, tf, market
                )

                payload = None
                if not args.fresh:
                    payload = cache_valid(
                        cp, ch, oh
                    )

                if payload is None:
                    result = process_file(
                        checkpoint, ohlcv,
                        tf, market
                    )
                    payload = {
                        "version": VERSION,
                        "timeframe": tf,
                        "market": market,
                        **result
                    }
                    save_cache(cp, payload)
                else:
                    resumed += 1

                total_rows += payload["rows"]
                all_details.extend(
                    payload["details"]
                )
                all_counts.update(
                    payload["counts"]
                )

                print(
                    f"[OK] {tf}/{market} "
                    f"rows={payload['rows']} "
                    f"resume={payload is not None and cp.exists()}",
                    flush=True
                )

            except Exception as e:
                errors.append(
                    f"{tf}/{market}:"
                    f"{type(e).__name__}:{e}"
                )
                print(
                    "[ERROR] " + errors[-1],
                    flush=True
                )

    columns = [
        "timeframe", "market",
        "checkpoint_row", "signal_timestamp",
        "entry_timestamp", "horizon",
        "recorded_status", "include_result",
        "exclude_result", "classification",
        "include_observed", "exclude_observed",
        "include_max_return_pct",
        "exclude_max_return_pct",
        "include_max_drawdown_pct",
        "exclude_max_drawdown_pct"
    ]

    detail_path = (
        report / "outcome_verification_detail.csv"
    )
    result_path = (
        report / "outcome_verification_result.json"
    )
    manifest_path = (
        report / "outcome_verification_manifest.json"
    )

    write_csv(
        detail_path, all_details, columns
    )

    bad_rows = sum(
        count for name, count in all_counts.items()
        if name == "NEITHER_MATCH" or
        name.startswith("ROW_ERROR:")
    )

    complete = (
        total_files > 0 and
        not errors and
        total_rows == len(all_details)
    )

    if not complete:
        classification = "FAIL"
    elif bad_rows:
        classification = "REVIEW"
    elif all_counts.get(
        "INCOMPLETE_COMPARISON", 0
    ):
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
        "files_resumed": resumed,
        "rows_checked": total_rows,
        "classification_counts": dict(
            sorted(all_counts.items())
        ),
        "errors": errors,
        "limitations": [
            "Uses current OHLCV snapshots",
            "Does not prove point-in-time availability",
            "Entry convention is not assumed",
            "Does not independently reconstruct signals",
            "Does not prove independent OOS",
            "Does not establish executable fills",
            "Does not prove profitability"
        ],
        "train_approved": False,
        "live_trading_approved": False
    }

    write_json(result_path, result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "inputs": input_hashes,
        "outputs": {
            detail_path.name: sha256(detail_path),
            result_path.name: sha256(result_path)
        }
    }
    write_json(manifest_path, manifest)

    print(
        json.dumps(
            result, ensure_ascii=False, indent=2
        ),
        flush=True
    )

    if not complete:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
