
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pattern 256 OHLCV Gap Integrity Audit - Clean V001.

Read-only OHLCV timestamp continuity audit.
Source: verified concentration audit.
No data repair, model training or trading approval.
"""
import argparse
import csv
import hashlib
import json
import os
import sys
import traceback
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
SOURCE_VERSION = "Clean V001"
SOURCE_DIR = "pattern_256_censoring_concentration"
REPORT_DIR = "pattern_256_ohlcv_gap_integrity"
INTERVAL_MS = {
    "h1": 3600000,
    "h4": 14400000,
    "d1": 86400000,
}
TIME_COLUMNS = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "date_time",
    "time",
    "date",
    "candle_date_time_kst",
)
MAX_GAP_SAMPLES_PER_MARKET = 200


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def read_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        require(reader.fieldnames, f"Empty CSV: {path}")
        return list(reader.fieldnames), list(reader)


def write_json(path, obj):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            obj, f, ensure_ascii=False, indent=2,
            sort_keys=True, allow_nan=False
        )
        f.write("\n")
    os.replace(tmp, path)


def write_csv(path, fields, rows):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, extrasaction="raise"
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def percentage(n, d):
    return round(100 * n / d, 4) if d else 0.0


def parse_timestamp(value, column):
    s = str(value).strip()
    if not s:
        raise ValueError("Empty timestamp")

    # Numeric epoch timestamps: seconds, milliseconds,
    # microseconds or nanoseconds.
    try:
        n = float(s)
        if not (-1e25 < n < 1e25):
            raise ValueError("Invalid numeric timestamp")
        a = abs(n)
        if a >= 1e17:
            return int(round(n / 1000000))
        if a >= 1e14:
            return int(round(n / 1000))
        if a >= 1e11:
            return int(round(n))
        return int(round(n * 1000))
    except ValueError:
        pass

    s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)

    if dt.tzinfo is None:
        # Upbit UTC candle strings are naive UTC;
        # Upbit KST candle strings are naive UTC+09.
        if column == "candle_date_time_kst":
            from datetime import timedelta
            dt = dt.replace(
                tzinfo=timezone(timedelta(hours=9))
            )
        else:
            dt = dt.replace(tzinfo=timezone.utc)

    return int(round(dt.timestamp() * 1000))


def utc_string(ms):
    if ms is None:
        return ""
    return datetime.fromtimestamp(
        ms / 1000, tz=timezone.utc
    ).isoformat()


def verify_source(folder):
    manifest_path = (
        folder / "concentration_manifest.json"
    )
    require(
        manifest_path.is_file(),
        f"Missing source manifest: {manifest_path}"
    )

    manifest = read_json(manifest_path)
    require(
        manifest.get("execution") == "PASS"
        and manifest.get("version") == SOURCE_VERSION,
        "Source manifest not verified PASS"
    )

    expected = manifest.get("output_sha256", {})
    required = (
        "concentration_result.json",
        "concentration_ranking.csv",
        "concentration_topk.csv",
        "concentration_exclusions.csv",
    )
    require(
        set(expected) == set(required),
        "Unexpected source manifest file list"
    )

    for name in required:
        path = folder / name
        require(path.is_file(), f"Missing source: {path}")
        require(
            sha256(path) == str(expected[name]).lower(),
            f"Source SHA256 mismatch: {name}"
        )

    result = read_json(
        folder / "concentration_result.json"
    )
    require(
        result.get("execution") == "PASS"
        and result.get("audited_market_timeframe_groups")
        == 853
        and result.get("audited_event_records")
        == 252600
        and result.get("research_decision") == "HOLD",
        "Source result mismatch"
    )

    _, ranking = read_csv(
        folder / "concentration_ranking.csv"
    )
    targets = []
    seen = set()

    for row in ranking:
        if row["scope"] != "ALL":
            continue

        tf = row["timeframe"]
        market = row["market"]
        key = (tf, market)

        require(
            tf in INTERVAL_MS
            and market.startswith("KRW-")
            and "/" not in market
            and "\\" not in market,
            f"Invalid market identity: {key}"
        )
        require(
            key not in seen,
            f"Duplicate market identity: {key}"
        )
        seen.add(key)
        targets.append({
            "timeframe": tf,
            "market": market,
            "censored_total": int(
                row["censored_total"]
            ),
            "events": int(row["events"]),
        })

    require(
        len(targets) == 853,
        f"Expected 853 targets, got {len(targets)}"
    )

    return (
        targets,
        sha256(manifest_path),
        result,
    )


def inspect_market(root, target):
    tf = target["timeframe"]
    market = target["market"]
    path = (
        root / "data" / "ohlcv"
        / tf / f"{market}.csv"
    )
    interval = INTERVAL_MS[tf]

    report = {
        "timeframe": tf,
        "market": market,
        "source_events": target["events"],
        "source_censored": target["censored_total"],
        "status": "OK",
        "timestamp_column": "",
        "rows": 0,
        "unique_timestamps": 0,
        "duplicates": 0,
        "nonmonotonic_rows": 0,
        "off_grid_intervals": 0,
        "gap_intervals": 0,
        "missing_candle_slots": 0,
        "max_gap_slots": 0,
        "first_utc": "",
        "last_utc": "",
        "error": "",
    }
    samples = []

    if not path.is_file():
        report["status"] = "MISSING_FILE"
        report["error"] = str(path)
        return report, samples

    try:
        with path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            column = next(
                (x for x in TIME_COLUMNS if x in columns),
                None
            )
            if column is None:
                raise ValueError(
                    f"No supported timestamp column: {columns}"
                )

            report["timestamp_column"] = column
            timestamps = []

            for line_no, row in enumerate(reader, 2):
                try:
                    stamp = parse_timestamp(
                        row.get(column, ""), column
                    )
                except Exception as exc:
                    raise ValueError(
                        f"Line {line_no}: {exc}"
                    ) from exc
                timestamps.append(stamp)

        report["rows"] = len(timestamps)
        if not timestamps:
            raise ValueError("No OHLCV rows")

        report["nonmonotonic_rows"] = sum(
            timestamps[i] < timestamps[i - 1]
            for i in range(1, len(timestamps))
        )

        ordered = sorted(timestamps)
        counts = Counter(ordered)
        unique = sorted(counts)

        report["unique_timestamps"] = len(unique)
        report["duplicates"] = (
            len(timestamps) - len(unique)
        )
        report["first_utc"] = utc_string(unique[0])
        report["last_utc"] = utc_string(unique[-1])

        for previous, current in zip(
            unique, unique[1:]
        ):
            delta = current - previous
            if delta <= 0:
                raise ValueError(
                    "Nonpositive unique timestamp interval"
                )

            if delta % interval != 0:
                report["off_grid_intervals"] += 1
                continue

            slots = delta // interval - 1
            if slots <= 0:
                continue

            report["gap_intervals"] += 1
            report["missing_candle_slots"] += slots
            report["max_gap_slots"] = max(
                report["max_gap_slots"], slots
            )

            if len(samples) < MAX_GAP_SAMPLES_PER_MARKET:
                samples.append({
                    "timeframe": tf,
                    "market": market,
                    "previous_utc": utc_string(previous),
                    "next_utc": utc_string(current),
                    "missing_slots": slots,
                })

        if (
            report["duplicates"]
            or report["nonmonotonic_rows"]
            or report["off_grid_intervals"]
            or report["gap_intervals"]
        ):
            report["status"] = "ANOMALY"

    except Exception as exc:
        report["status"] = "ERROR"
        report["error"] = str(exc)

    return report, samples


def main(root):
    root = Path(root).resolve()
    reports = root / "data" / "reports"
    source = reports / SOURCE_DIR

    targets, source_hash, source_result = (
        verify_source(source)
    )

    out = reports / REPORT_DIR
    out.mkdir(parents=True, exist_ok=True)

    result_path = out / "gap_integrity_result.json"
    manifest_path = out / "gap_integrity_manifest.json"

    # Prevent a stale PASS from surviving a failed run.
    for path in (result_path, manifest_path):
        if path.exists():
            path.unlink()

    market_rows = []
    gap_rows = []

    for index, target in enumerate(targets, 1):
        row, samples = inspect_market(root, target)
        market_rows.append(row)
        gap_rows.extend(samples)

        if index % 50 == 0 or index == len(targets):
            print(
                f"[PROGRESS] {index}/{len(targets)}",
                flush=True
            )

    summary = []
    for scope in ("ALL", "h1", "h4", "d1"):
        rows = [
            r for r in market_rows
            if scope == "ALL"
            or r["timeframe"] == scope
        ]
        counts = Counter(
            r["status"] for r in rows
        )
        summary.append({
            "scope": scope,
            "market_groups": len(rows),
            "ok": counts["OK"],
            "anomaly": counts["ANOMALY"],
            "missing_file": counts["MISSING_FILE"],
            "error": counts["ERROR"],
            "ohlcv_rows": sum(
                r["rows"] for r in rows
            ),
            "duplicate_rows": sum(
                r["duplicates"] for r in rows
            ),
            "nonmonotonic_rows": sum(
                r["nonmonotonic_rows"] for r in rows
            ),
            "off_grid_intervals": sum(
                r["off_grid_intervals"] for r in rows
            ),
            "gap_intervals": sum(
                r["gap_intervals"] for r in rows
            ),
            "missing_candle_slots": sum(
                r["missing_candle_slots"] for r in rows
            ),
            "source_censored_events": sum(
                r["source_censored"] for r in rows
            ),
        })

    market_fields = [
        "timeframe", "market", "source_events",
        "source_censored", "status",
        "timestamp_column", "rows",
        "unique_timestamps", "duplicates",
        "nonmonotonic_rows", "off_grid_intervals",
        "gap_intervals", "missing_candle_slots",
        "max_gap_slots", "first_utc", "last_utc",
        "error",
    ]
    gap_fields = [
        "timeframe", "market", "previous_utc",
        "next_utc", "missing_slots",
    ]
    summary_fields = list(summary[0])

    market_path = out / "gap_integrity_market.csv"
    gap_path = out / "gap_integrity_samples.csv"
    summary_path = out / "gap_integrity_summary.csv"

    write_csv(market_path, market_fields, market_rows)
    write_csv(gap_path, gap_fields, gap_rows)
    write_csv(summary_path, summary_fields, summary)

    # PASS means the audit executed and reconciled,
    # not that the underlying OHLCV is gap-free.
    overall = summary[0]
    execution = (
        "PASS"
        if overall["missing_file"] == 0
        and overall["error"] == 0
        else "FAIL"
    )

    result = {
        "audit": "pattern_256_ohlcv_gap_integrity",
        "version": VERSION,
        "execution": execution,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_manifest_sha256": source_hash,
        "source_audit": SOURCE_DIR,
        "source_event_records": (
            source_result["audited_event_records"]
        ),
        "source_market_groups": len(targets),
        "source_censored_events": (
            source_result["overall"]["censored"]
        ),
        "summary": summary,
        "market_report_rows": len(market_rows),
        "gap_sample_rows": len(gap_rows),
        "sample_limit_per_market": (
            MAX_GAP_SAMPLES_PER_MARKET
        ),
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Timestamp gaps are not identical to "
            "censored events.",
            "Some absent candles may reflect "
            "periods without trades.",
            "The audit does not classify exchange "
            "outages or collection failures.",
            "Gaps outside available file boundaries "
            "cannot be counted.",
            "The audit does not repair source data.",
            "Gap sample CSV is limited to 200 "
            "intervals per market.",
            "No independent OOS or profitability "
            "is established.",
        ],
    }

    write_json(result_path, result)

    output_files = [
        market_path,
        gap_path,
        summary_path,
        result_path,
    ]

    write_json(manifest_path, {
        "audit": result["audit"],
        "version": VERSION,
        "execution": execution,
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "source_manifest_sha256": source_hash,
        "output_sha256": {
            p.name: sha256(p)
            for p in output_files
        },
    })

    print(
        json.dumps({
            "execution": execution,
            "version": VERSION,
            "market_groups": len(market_rows),
            "missing_files": overall["missing_file"],
            "errors": overall["error"],
            "gap_intervals": overall["gap_intervals"],
            "missing_candle_slots": (
                overall["missing_candle_slots"]
            ),
            "research_decision": "HOLD",
        }, indent=2),
        flush=True
    )

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    try:
        sys.exit(main(args.root))
    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted",
            file=sys.stderr
        )
        sys.exit(130)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
