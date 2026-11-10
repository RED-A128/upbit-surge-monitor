
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 WINDOW_GAP Reconciliation
Clean V001

Read-only diagnostic:
- Compare outcome verification classifications
  with actual OHLCV candle intervals
- Stratified deterministic sample
- Resume using SHA256-verified checkpoints
- No source modifications
- No training/trading approval
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_window_gap_reconciliation"

STEPS = {
    "h1": 3600000,
    "h4": 14400000,
    "d1": 86400000,
}

OUT_REL = Path(
    "data/reports/pattern_256_window_gap_reconciliation"
)

SOURCE_NAMES = (
    "outcome_verification_detail.csv",
    "outcomes_verification_detail.csv",
    "outcome_verification_details.csv",
)

CATEGORIES = (
    "WINDOW_GAP",
    "BOTH_MATCH",
    "INCLUDE_MATCH",
    "EXCLUDE_MATCH",
    "INCOMPLETE_COMPARISON",
    "ROW_ERROR:TIMESTAMP_EMPTY",
)

DETAIL_COLUMNS = (
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "horizon",
    "recorded_status",
    "classification",
    "include_result",
    "exclude_result",
)

OUTPUTS = (
    "window_gap_reconciliation_result.json",
    "window_gap_reconciliation_samples.csv",
    "window_gap_reconciliation_by_category.csv",
    "window_gap_reconciliation_by_timeframe.csv",
)


def file_hash(path):
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


def atomic_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            obj, f, ensure_ascii=False,
            indent=2, sort_keys=True
        )
        f.write("\n")
    os.replace(tmp, path)


def atomic_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=columns,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def parse_time(value):
    value = str(value or "").strip()
    if not value:
        raise ValueError("EMPTY_TIMESTAMP")

    if value.isdigit():
        n = int(value)
        if n > 100000000000:
            return n
        if n > 100000000:
            return n * 1000
        raise ValueError("INVALID_TIMESTAMP")

    dt = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        raise ValueError("TIMEZONE_MISSING")
    return int(dt.timestamp() * 1000)


def iso(ms):
    return datetime.fromtimestamp(
        ms / 1000, tz=timezone.utc
    ).isoformat()


def discover_detail(root, explicit):
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise RuntimeError(
                "Detail file missing: " + str(path)
            )
        return path

    base = root / "data" / "reports"
    candidates = []

    for name in SOURCE_NAMES:
        candidates.extend(base.rglob(name))

    candidates = [
        p for p in candidates if p.is_file()
    ]

    if len(candidates) != 1:
        raise RuntimeError(
            "Expected exactly one detail CSV, found {}. "
            "Use --detail with the exact path. "
            "Candidates: {}".format(
                len(candidates),
                [str(p) for p in candidates[:10]]
            )
        )

    return candidates[0]


def select_samples(detail, max_per_category):
    """
    Deterministic first-N per category/timeframe.
    Not a random or statistically representative sample.
    """
    limits = max_per_category
    counts = Counter()
    selected = []
    totals = Counter()
    scanned = 0

    with detail.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        missing = set(DETAIL_COLUMNS) - set(
            reader.fieldnames or []
        )
        if missing:
            raise RuntimeError(
                "Missing detail columns: {}".format(
                    sorted(missing)
                )
            )

        for line, row in enumerate(reader, start=2):
            scanned += 1
            tf = row["timeframe"].strip()
            category = row["classification"].strip()

            totals[(tf, category)] += 1

            if tf not in STEPS:
                continue
            if category not in CATEGORIES:
                continue

            key = (tf, category)

            if counts[key] >= limits:
                continue

            counts[key] += 1

            selected.append({
                "detail_line": line,
                **{
                    name: row.get(name, "")
                    for name in DETAIL_COLUMNS
                }
            })

    return selected, totals, scanned


def load_ohlcv(path):
    stamps = []

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        if not (
            "timestamp" in (reader.fieldnames or [])
            or "candle_date_time_utc"
            in (reader.fieldnames or [])
        ):
            raise RuntimeError(
                "OHLCV timestamp column missing"
            )

        for row in reader:
            raw = (
                row.get("timestamp")
                or row.get("candle_date_time_utc")
            )
            stamps.append(parse_time(raw))

    if not stamps:
        raise RuntimeError("Empty OHLCV")

    if any(
        stamps[i] <= stamps[i - 1]
        for i in range(1, len(stamps))
    ):
        raise RuntimeError(
            "OHLCV not strictly increasing"
        )

    return stamps


def inspect_window(row, stamps):
    tf = row["timeframe"]
    step = STEPS[tf]
    horizon = int(row["horizon"])

    if horizon <= 0 or horizon > 100000:
        raise ValueError("INVALID_HORIZON")

    signal = parse_time(row["signal_timestamp"])
    entry = parse_time(row["entry_timestamp"])

    if entry <= signal:
        raise ValueError("ENTRY_NOT_AFTER_SIGNAL")

    # Two candidate window conventions:
    # include entry candle: [entry, entry+(horizon-1)*step]
    # exclude entry candle: [entry+step, entry+horizon*step]
    #
    # Compare observed timestamps against expected grid.
    # This diagnostic does not assume which convention
    # the original evaluator used.

    results = {}

    for convention, start in (
        ("include", entry),
        ("exclude", entry + step),
    ):
        end = start + (horizon - 1) * step

        left = bisect_left(stamps, start)
        right = bisect_right(stamps, end)
        actual = stamps[left:right]

        expected = set(
            start + i * step
            for i in range(horizon)
        )

        observed = set(actual)
        missing = expected - observed

        results[convention] = {
            "expected": horizon,
            "observed": len(actual),
            "missing_slots": len(missing),
            "first_missing": (
                iso(min(missing)) if missing else ""
            ),
            "start": iso(start),
            "end": iso(end),
            "complete": len(missing) == 0,
        }

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--detail", default="")
    parser.add_argument(
        "--max-per-category", type=int, default=300
    )
    args = parser.parse_args()

    if not 1 <= args.max_per_category <= 5000:
        raise RuntimeError(
            "max-per-category must be 1..5000"
        )

    root = Path(args.root).expanduser().resolve()
    detail = discover_detail(root, args.detail)

    out = root / OUT_REL
    out.mkdir(parents=True, exist_ok=True)

    script_hash = file_hash(
        Path(__file__).resolve()
    )
    detail_hash = file_hash(detail)

    checkpoint = (
        out / "checkpoints" / "selection.json"
    )

    selection = None

    if checkpoint.is_file():
        try:
            candidate = read_json(checkpoint)
            if (
                candidate.get("audit") == AUDIT
                and candidate.get("version") == VERSION
                and candidate.get("script_sha256")
                == script_hash
                and candidate.get("detail_sha256")
                == detail_hash
                and candidate.get("max_per_category")
                == args.max_per_category
                and isinstance(
                    candidate.get("selected"), list
                )
                and isinstance(
                    candidate.get("totals"), list
                )
            ):
                selection = candidate
                print(
                    "[RESUME] Valid selection checkpoint",
                    flush=True
                )
        except (OSError, ValueError, TypeError):
            pass

    if selection is None:
        print(
            "[SCAN] Reading detail: {}".format(detail),
            flush=True
        )

        selected, totals, scanned = select_samples(
            detail, args.max_per_category
        )

        selection = {
            "audit": AUDIT,
            "version": VERSION,
            "script_sha256": script_hash,
            "detail_sha256": detail_hash,
            "max_per_category":
                args.max_per_category,
            "scanned_rows": scanned,
            "selected": selected,
            "totals": [
                {
                    "timeframe": tf,
                    "classification": category,
                    "rows": count,
                }
                for (tf, category), count
                in sorted(totals.items())
            ],
        }

        atomic_json(checkpoint, selection)

    selected = selection["selected"]
    grouped = defaultdict(list)

    for row in selected:
        grouped[
            (row["timeframe"], row["market"])
        ].append(row)

    results = []
    errors = []
    missing_sources = 0

    for index, ((tf, market), rows) in enumerate(
        sorted(grouped.items()), start=1
    ):
        path = (
            root / "data" / "ohlcv"
            / tf / (market + ".csv")
        )

        if not path.is_file():
            missing_sources += 1
            errors.append(
                "OHLCV missing: {}/{}".format(
                    tf, market
                )
            )
            continue

        try:
            stamps = load_ohlcv(path)

            for row in rows:
                item = {
                    "timeframe": tf,
                    "market": market,
                    "detail_line": row["detail_line"],
                    "classification":
                        row["classification"],
                    "recorded_status":
                        row["recorded_status"],
                    "horizon": row["horizon"],
                    "signal_timestamp":
                        row["signal_timestamp"],
                    "entry_timestamp":
                        row["entry_timestamp"],
                    "include_result":
                        row["include_result"],
                    "exclude_result":
                        row["exclude_result"],
                    "include_missing_slots": "",
                    "exclude_missing_slots": "",
                    "include_observed": "",
                    "exclude_observed": "",
                    "include_complete": "",
                    "exclude_complete": "",
                    "first_include_missing": "",
                    "first_exclude_missing": "",
                    "diagnosis": "",
                }

                try:
                    window = inspect_window(
                        row, stamps
                    )
                    inc = window["include"]
                    exc = window["exclude"]

                    item.update({
                        "include_missing_slots":
                            inc["missing_slots"],
                        "exclude_missing_slots":
                            exc["missing_slots"],
                        "include_observed":
                            inc["observed"],
                        "exclude_observed":
                            exc["observed"],
                        "include_complete":
                            inc["complete"],
                        "exclude_complete":
                            exc["complete"],
                        "first_include_missing":
                            inc["first_missing"],
                        "first_exclude_missing":
                            exc["first_missing"],
                    })

                    if (
                        inc["missing_slots"] > 0
                        and exc["missing_slots"] > 0
                    ):
                        diagnosis = (
                            "BOTH_WINDOWS_HAVE_MISSING"
                        )
                    elif inc["missing_slots"] > 0:
                        diagnosis = (
                            "INCLUDE_WINDOW_MISSING"
                        )
                    elif exc["missing_slots"] > 0:
                        diagnosis = (
                            "EXCLUDE_WINDOW_MISSING"
                        )
                    else:
                        diagnosis = (
                            "BOTH_WINDOWS_COMPLETE"
                        )

                    item["diagnosis"] = diagnosis

                except (
                    ValueError, OverflowError,
                    TypeError
                ) as exc:
                    item["diagnosis"] = (
                        "ROW_ERROR:" +
                        str(exc).split(":")[0]
                    )

                results.append(item)

        except Exception as exc:
            errors.append(
                "{}/{}: {}: {}".format(
                    tf, market,
                    type(exc).__name__, exc
                )
            )

        if index % 25 == 0:
            print(
                "[PROGRESS] {}/{} markets".format(
                    index, len(grouped)
                ),
                flush=True
            )

    by_category = defaultdict(Counter)
    by_tf = defaultdict(Counter)

    for row in results:
        key = (
            row["timeframe"],
            row["classification"]
        )
        diagnosis = row["diagnosis"]

        by_category[key][diagnosis] += 1
        by_category[key]["total"] += 1
        by_tf[row["timeframe"]][diagnosis] += 1
        by_tf[row["timeframe"]]["total"] += 1

    categories = sorted({
        row["diagnosis"] for row in results
    })

    category_rows = []

    for (tf, category), counts in sorted(
        by_category.items()
    ):
        category_rows.append({
            "timeframe": tf,
            "classification": category,
            "total": counts["total"],
            **{
                name: counts[name]
                for name in categories
            }
        })

    timeframe_rows = []

    for tf in STEPS:
        counts = by_tf[tf]
        timeframe_rows.append({
            "timeframe": tf,
            "total": counts["total"],
            **{
                name: counts[name]
                for name in categories
            }
        })

    sample_columns = (
        "timeframe", "market", "detail_line",
        "classification", "recorded_status",
        "horizon", "signal_timestamp",
        "entry_timestamp", "include_result",
        "exclude_result",
        "include_missing_slots",
        "exclude_missing_slots",
        "include_observed",
        "exclude_observed",
        "include_complete",
        "exclude_complete",
        "first_include_missing",
        "first_exclude_missing",
        "diagnosis",
    )

    atomic_csv(
        out / "window_gap_reconciliation_samples.csv",
        sample_columns, results
    )

    atomic_csv(
        out / "window_gap_reconciliation_by_category.csv",
        ("timeframe", "classification",
         "total", *categories),
        category_rows
    )

    atomic_csv(
        out / "window_gap_reconciliation_by_timeframe.csv",
        ("timeframe", "total", *categories),
        timeframe_rows
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
        "detail_path": str(detail),
        "detail_sha256": detail_hash,
        "detail_rows_scanned":
            selection["scanned_rows"],
        "sample_rows_selected": len(selected),
        "sample_rows_processed": len(results),
        "sample_selection_method":
            "FIRST_N_PER_TIMEFRAME_CATEGORY",
        "max_per_category":
            args.max_per_category,
        "source_markets": len(grouped),
        "missing_source_files": missing_sources,
        "diagnosis_counts": dict(
            Counter(
                r["diagnosis"] for r in results
            )
        ),
        "errors": errors[:100],
        "error_count": len(errors),
        "limitations": [
            "This is a deterministic diagnostic sample",
            "The sample is not statistically representative",
            "The original evaluator is not rerun",
            "Both possible entry-window conventions are tested",
            "Missing exchange candles do not prove collection failure",
            "Source data snapshots may differ from historical availability",
            "No independent OOS or profitability proof",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    atomic_json(
        out / "window_gap_reconciliation_result.json",
        result
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            name: file_hash(out / name)
            for name in OUTPUTS
        },
    }

    atomic_json(
        out / "window_gap_reconciliation_manifest.json",
        manifest
    )

    print(
        json.dumps({
            "execution": result["execution"],
            "detail_rows_scanned":
                result["detail_rows_scanned"],
            "sample_rows_processed":
                result["sample_rows_processed"],
            "diagnosis_counts":
                result["diagnosis_counts"],
            "error_count": len(errors),
            "research_decision": "HOLD",
        }, ensure_ascii=False, indent=2),
        flush=True
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
            flush=True
        )
        sys.exit(1)
