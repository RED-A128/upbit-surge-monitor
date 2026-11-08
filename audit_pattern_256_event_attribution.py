
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event-Level Attribution Audit
Clean V001

Read-only audit of:
- gap_integrity_events.csv
- independent_detail.csv

Uses SQLite for disk-backed event matching.
Does not modify OHLCV, source reports or trading signals.

PASS means audit execution succeeded.
It does NOT mean attribution was proven.
"""

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_event_attribution"

GAP_REL = (
    "data/reports/pattern_256_event_gap_integrity/"
    "gap_integrity_events.csv"
)

INDEPENDENT_REL = (
    "data/reports/pattern_256_accumulation_independent/"
    "independent_detail.csv"
)

GAP_REQUIRED = {
    "timeframe", "market", "signal_utc", "entry_utc",
    "original_outcome", "corrected_outcome",
    "missing_candles", "period",
}

INDEPENDENT_REQUIRED = {
    "timeframe", "market", "signal_timestamp",
    "entry_timestamp", "status", "hit_30pct", "period",
}


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    path.write_text(
        json.dumps(
            value, ensure_ascii=False,
            indent=2, sort_keys=True
        ) + "\n",
        encoding="utf-8",
    )


def utc_epoch(value):
    """
    Normalize timestamp to UTC seconds.

    Naive ISO timestamps are interpreted as UTC.
    Invalid timestamps raise ValueError.
    """
    text = str(value).strip()

    if not text:
        raise ValueError("empty timestamp")

    if re.fullmatch(r"\d{10}(?:\.\d+)?", text):
        return round(float(text), 3)

    if re.fullmatch(r"\d{13}", text):
        return round(int(text) / 1000, 3)

    normalized = text.replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        formats = (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        )
        dt = None
        for fmt in formats:
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                pass
        if dt is None:
            raise ValueError(
                f"unsupported timestamp: {text[:60]}"
            )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return round(
        dt.astimezone(timezone.utc).timestamp(), 3
    )


def event_key(row, signal_column, entry_column):
    timeframe = row.get("timeframe", "").strip().lower()
    market = row.get("market", "").strip().upper()

    if timeframe not in {"h1", "h4", "d1"}:
        raise ValueError("invalid timeframe")

    if not market.startswith("KRW-"):
        raise ValueError("invalid market")

    signal = utc_epoch(row.get(signal_column, ""))
    entry = utc_epoch(row.get(entry_column, ""))

    if entry < signal:
        raise ValueError("entry precedes signal")

    return timeframe, market, signal, entry


def normalized_status(value):
    status = str(value).strip().upper()

    if status == "HIT":
        return "HIT"
    if status == "MISS":
        return "MISS"
    if status in {
        "CENSORED", "END_CENSORED",
        "GAP_CENSORED",
    }:
        return "CENSORED"

    return "UNKNOWN"


def open_csv(path, required):
    stream = path.open(
        "r", encoding="utf-8-sig", newline=""
    )
    reader = csv.DictReader(stream)

    columns = reader.fieldnames or []

    if not required.issubset(set(columns)):
        stream.close()
        missing = sorted(required - set(columns))
        raise RuntimeError(
            f"Missing columns in {path.name}: {missing}"
        )

    return stream, reader


def prepare_database(path):
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=FILE")

    conn.executescript("""
        CREATE TABLE independent (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            signal_epoch REAL NOT NULL,
            entry_epoch REAL NOT NULL,
            period TEXT NOT NULL,
            status TEXT NOT NULL,
            hit TEXT NOT NULL,
            occurrences INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (
                timeframe, market,
                signal_epoch, entry_epoch
            )
        );

        CREATE TABLE gap_events (
            timeframe TEXT NOT NULL,
            market TEXT NOT NULL,
            signal_epoch REAL NOT NULL,
            entry_epoch REAL NOT NULL,
            period TEXT NOT NULL,
            original_status TEXT NOT NULL,
            corrected_status TEXT NOT NULL,
            missing_candles TEXT NOT NULL,
            occurrences INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (
                timeframe, market,
                signal_epoch, entry_epoch
            )
        );
    """)
    return conn


def add_issue(issues, max_issues, kind, row_number, detail):
    if len(issues) < max_issues:
        issues.append({
            "kind": kind,
            "row_number": row_number,
            "detail": str(detail)[:400],
        })


def load_independent(conn, path, issues, max_issues):
    counts = Counter()
    stream, reader = open_csv(
        path, INDEPENDENT_REQUIRED
    )

    try:
        with conn:
            for number, row in enumerate(reader, 2):
                counts["rows"] += 1

                try:
                    tf, market, signal, entry = event_key(
                        row,
                        "signal_timestamp",
                        "entry_timestamp",
                    )
                except ValueError as exc:
                    counts["invalid_keys"] += 1
                    add_issue(
                        issues, max_issues,
                        "INDEPENDENT_INVALID_KEY",
                        number, exc,
                    )
                    continue

                status = normalized_status(
                    row.get("status", "")
                )
                period = row.get("period", "").strip()
                hit = row.get("hit_30pct", "").strip()

                if status == "UNKNOWN":
                    counts["unknown_status"] += 1

                cursor = conn.execute("""
                    INSERT OR IGNORE INTO independent (
                        timeframe, market, signal_epoch,
                        entry_epoch, period, status, hit
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tf, market, signal, entry,
                    period, status, hit,
                ))

                if cursor.rowcount == 0:
                    counts["duplicate_keys"] += 1
                    conn.execute("""
                        UPDATE independent
                        SET occurrences = occurrences + 1
                        WHERE timeframe=? AND market=?
                          AND signal_epoch=? AND entry_epoch=?
                    """, (tf, market, signal, entry))
                else:
                    counts["unique_keys"] += 1

                if counts["rows"] % 50000 == 0:
                    print(
                        "[INDEPENDENT] rows:",
                        counts["rows"],
                        flush=True,
                    )
    finally:
        stream.close()

    return dict(counts)


def load_gap(conn, path, issues, max_issues):
    counts = Counter()
    stream, reader = open_csv(
        path, GAP_REQUIRED
    )

    try:
        with conn:
            for number, row in enumerate(reader, 2):
                counts["rows"] += 1

                try:
                    tf, market, signal, entry = event_key(
                        row, "signal_utc", "entry_utc"
                    )
                except ValueError as exc:
                    counts["invalid_keys"] += 1
                    add_issue(
                        issues, max_issues,
                        "GAP_INVALID_KEY",
                        number, exc,
                    )
                    continue

                original = normalized_status(
                    row.get("original_outcome", "")
                )
                corrected = normalized_status(
                    row.get("corrected_outcome", "")
                )

                if (
                    original == "UNKNOWN"
                    or corrected == "UNKNOWN"
                ):
                    counts["unknown_status"] += 1

                cursor = conn.execute("""
                    INSERT OR IGNORE INTO gap_events (
                        timeframe, market, signal_epoch,
                        entry_epoch, period,
                        original_status, corrected_status,
                        missing_candles
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tf, market, signal, entry,
                    row.get("period", "").strip(),
                    original, corrected,
                    row.get("missing_candles", "").strip(),
                ))

                if cursor.rowcount == 0:
                    counts["duplicate_keys"] += 1
                    conn.execute("""
                        UPDATE gap_events
                        SET occurrences = occurrences + 1
                        WHERE timeframe=? AND market=?
                          AND signal_epoch=? AND entry_epoch=?
                    """, (tf, market, signal, entry))
                else:
                    counts["unique_keys"] += 1

                if counts["rows"] % 50000 == 0:
                    print(
                        "[GAP] rows:",
                        counts["rows"],
                        flush=True,
                    )
    finally:
        stream.close()

    return dict(counts)


def analyze(conn, issues, max_issues):
    totals = Counter()
    by_timeframe = {}

    for tf in ("h1", "h4", "d1"):
        by_timeframe[tf] = Counter()

    query = """
        SELECT
            g.timeframe,
            g.market,
            g.signal_epoch,
            g.entry_epoch,
            g.period,
            g.original_status,
            g.corrected_status,
            g.missing_candles,
            g.occurrences,
            i.period,
            i.status,
            i.hit,
            i.occurrences
        FROM gap_events g
        LEFT JOIN independent i
          ON g.timeframe=i.timeframe
         AND g.market=i.market
         AND g.signal_epoch=i.signal_epoch
         AND g.entry_epoch=i.entry_epoch
    """

    for row in conn.execute(query):
        (
            tf, market, signal, entry,
            gap_period, original, corrected,
            missing, gap_occurrences,
            independent_period, independent_status,
            independent_hit, independent_occurrences,
        ) = row

        local = by_timeframe[tf]
        totals["gap_unique_events"] += 1
        local["gap_unique_events"] += 1

        if independent_status is None:
            totals["unmatched_gap_events"] += 1
            local["unmatched_gap_events"] += 1
            continue

        totals["matched_events"] += 1
        local["matched_events"] += 1

        if gap_occurrences > 1 or independent_occurrences > 1:
            totals["ambiguous_duplicate_matches"] += 1
            local["ambiguous_duplicate_matches"] += 1

        if gap_period != independent_period:
            totals["period_mismatches"] += 1
            local["period_mismatches"] += 1

        if original != independent_status:
            totals["original_status_mismatches"] += 1
            local["original_status_mismatches"] += 1

            add_issue(
                issues, max_issues,
                "ORIGINAL_STATUS_MISMATCH",
                0,
                (
                    f"{tf}/{market} signal={signal} "
                    f"entry={entry} gap={original} "
                    f"independent={independent_status}"
                ),
            )

        if corrected != independent_status:
            totals["corrected_status_differences"] += 1
            local["corrected_status_differences"] += 1

        if independent_status in {"HIT", "MISS"}:
            expected_hit = (
                "1" if independent_status == "HIT" else "0"
            )

            if independent_hit != expected_hit:
                totals["hit_flag_mismatches"] += 1
                local["hit_flag_mismatches"] += 1

        if missing:
            try:
                if float(missing) > 0:
                    totals["matched_events_with_gaps"] += 1
                    local["matched_events_with_gaps"] += 1
            except ValueError:
                totals["invalid_missing_counts"] += 1

    for tf, count in conn.execute("""
        SELECT timeframe, COUNT(*)
        FROM independent
        GROUP BY timeframe
    """):
        by_timeframe[tf]["independent_unique_events"] = count

    totals["independent_unique_events"] = conn.execute(
        "SELECT COUNT(*) FROM independent"
    ).fetchone()[0]

    totals["unmatched_independent_events"] = (
        conn.execute("""
            SELECT COUNT(*)
            FROM independent i
            LEFT JOIN gap_events g
              ON g.timeframe=i.timeframe
             AND g.market=i.market
             AND g.signal_epoch=i.signal_epoch
             AND g.entry_epoch=i.entry_epoch
            WHERE g.market IS NULL
        """).fetchone()[0]
    )

    for tf in ("h1", "h4", "d1"):
        local = by_timeframe[tf]
        local["unmatched_independent_events"] = (
            local["independent_unique_events"]
            - local["matched_events"]
        )

    return dict(totals), {
        tf: dict(value)
        for tf, value in by_timeframe.items()
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--max-issues", type=int, default=100
    )

    args = parser.parse_args()

    if not 1 <= args.max_issues <= 1000:
        raise RuntimeError("max-issues must be 1..1000")

    root = Path(args.root).resolve()
    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError("Data root does not exist")

    # Prevent placing audit output inside the source tree.
    try:
        output.relative_to(root)
        raise RuntimeError(
            "Output must be outside data root"
        )
    except ValueError:
        pass

    gap_path = (root / GAP_REL).resolve()
    independent_path = (
        root / INDEPENDENT_REL
    ).resolve()

    for path in (gap_path, independent_path):
        path.relative_to(root)

        if not path.is_file():
            raise RuntimeError(
                f"Required source missing: {path}"
            )

    output.mkdir(parents=True, exist_ok=True)

    issues = []
    db_path = output / "attribution_work.sqlite3"

    if db_path.exists():
        raise RuntimeError(
            "Output directory already contains audit DB"
        )

    conn = prepare_database(db_path)

    try:
        independent_counts = load_independent(
            conn, independent_path,
            issues, args.max_issues,
        )

        gap_counts = load_gap(
            conn, gap_path,
            issues, args.max_issues,
        )

        totals, timeframes = analyze(
            conn, issues, args.max_issues
        )
    finally:
        conn.close()

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "sources": {
            "gap_integrity": {
                "relative_path": GAP_REL,
                "size_bytes": gap_path.stat().st_size,
                "sha256": sha256_file(gap_path),
                "counts": gap_counts,
            },
            "independent_detail": {
                "relative_path": INDEPENDENT_REL,
                "size_bytes": independent_path.stat().st_size,
                "sha256": sha256_file(independent_path),
                "counts": independent_counts,
            },
        },
        "matching_key": [
            "timeframe",
            "market",
            "signal_utc_epoch",
            "entry_utc_epoch",
        ],
        "timestamp_assumption": (
            "Timezone-naive timestamps interpreted as UTC"
        ),
        "totals": totals,
        "by_timeframe": timeframes,
        "sample_issues": issues,
        "sample_issues_truncated": (
            len(issues) >= args.max_issues
        ),
        "limitations": [
            "Duplicates are counted but not resolved",
            "Matching does not prove causal attribution",
            "Original outcome and corrected outcome "
            "have different meanings",
            "No independent OHLCV replay",
            "No independent OOS validation",
            "No executable trading simulation",
        ],
        "event_level_attribution_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    result_path = output / "attribution_result.json"
    summary_path = output / "attribution_timeframes.csv"
    issues_path = output / "attribution_issues.csv"
    manifest_path = output / "attribution_manifest.json"

    write_json(result_path, result)

    fields = [
        "timeframe",
        "gap_unique_events",
        "independent_unique_events",
        "matched_events",
        "unmatched_gap_events",
        "unmatched_independent_events",
        "ambiguous_duplicate_matches",
        "period_mismatches",
        "original_status_mismatches",
        "corrected_status_differences",
        "hit_flag_mismatches",
        "matched_events_with_gaps",
    ]

    with summary_path.open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.DictWriter(
            stream, fieldnames=fields
        )
        writer.writeheader()

        for tf in ("h1", "h4", "d1"):
            writer.writerow({
                "timeframe": tf,
                **timeframes[tf],
            })

    with issues_path.open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "kind", "row_number", "detail"
            ],
        )
        writer.writeheader()
        writer.writerows(issues)

    output_files = [
        result_path,
        summary_path,
        issues_path,
    ]

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": sha256_file(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            path.name: sha256_file(path)
            for path in output_files
        },
        "research_decision": "HOLD",
    }

    write_json(manifest_path, manifest)

    print(
        json.dumps(
            {
                "execution": "PASS",
                "totals": totals,
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
