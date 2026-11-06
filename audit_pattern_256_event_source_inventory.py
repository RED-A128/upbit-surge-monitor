
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pattern 256 Event Source Inventory - Clean V001 (read-only)."""

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
OUT_NAME = "pattern_256_event_source_inventory"

COLUMNS = [
    "relative_path", "size_bytes", "sha256",
    "columns", "sample_rows", "event_score",
    "event_terms", "time_terms", "market_terms",
    "timeframe_terms", "status", "error",
]

TERMS = {
    "event": [
        "event", "trigger", "entry", "signal",
        "selected", "outcome", "censor",
        "hit", "miss", "horizon", "target",
    ],
    "time": [
        "timestamp", "datetime", "date",
        "time", "utc", "start", "end", "entry",
    ],
    "market": [
        "market", "ticker", "symbol", "coin",
    ],
    "timeframe": [
        "timeframe", "interval", "tf",
    ],
}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            obj, f, ensure_ascii=False, indent=2,
            sort_keys=True, allow_nan=False,
        )
        f.write("\n")
    os.replace(tmp, path)


def atomic_csv(path, rows):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def inspect(path, root):
    row = {k: "" for k in COLUMNS}
    row["relative_path"] = (
        path.relative_to(root).as_posix()
    )
    row["size_bytes"] = path.stat().st_size
    row["sha256"] = digest(path)

    try:
        with path.open(
            "r", encoding="utf-8-sig", newline=""
        ) as f:
            reader = csv.reader(f)
            header = next(reader, [])

            if not header or not any(
                s.strip() for s in header
            ):
                raise ValueError("Empty CSV header")

            if len(header) != len(set(header)):
                raise ValueError("Duplicate CSV columns")

            sample = []
            for _, r in zip(range(3), reader):
                sample.append({
                    header[i]: (
                        r[i] if i < len(r) else ""
                    )
                    for i in range(len(header))
                })

            row["columns"] = json.dumps(
                header, ensure_ascii=False
            )
            row["sample_rows"] = json.dumps(
                sample, ensure_ascii=False
            )

            low = [x.lower() for x in header]

            for category, terms in TERMS.items():
                matched = sorted({
                    h for h in low
                    if any(t in h for t in terms)
                })
                row[category + "_terms"] = json.dumps(
                    matched, ensure_ascii=False
                )

            row["event_score"] = sum(
                bool(json.loads(
                    row[c + "_terms"]
                ))
                for c in TERMS
            )
            row["status"] = "OK"

        return row

    except (
        UnicodeError, csv.Error, OSError, ValueError
    ) as e:
        row["status"] = "ERROR"
        row["error"] = str(e)[:500]
        return row


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument(
        "--max-files", type=int, default=3000
    )
    args = p.parse_args()

    root = Path(args.root).resolve()
    reports = root / "data" / "reports"
    ohlcv = root / "data" / "ohlcv"

    if not reports.is_dir() or not ohlcv.is_dir():
        raise RuntimeError(
            "Missing data/reports or data/ohlcv"
        )

    output = reports / OUT_NAME
    output.mkdir(parents=True, exist_ok=True)

    candidates = sorted(
        x for x in reports.rglob("*.csv")
        if output not in x.parents and x.is_file()
    )

    if not candidates:
        raise RuntimeError(
            "No report CSV files found"
        )

    if len(candidates) > args.max_files:
        raise RuntimeError(
            f"Too many report CSV files: "
            f"{len(candidates)} > {args.max_files}"
        )

    rows = [
        inspect(x, root) for x in candidates
    ]
    rows.sort(key=lambda r: (
        -int(r["event_score"] or 0),
        r["relative_path"],
    ))

    frames = {}

    for tf in ("h1", "h4", "d1"):
        folder = ohlcv / tf

        if not folder.is_dir():
            raise RuntimeError(
                f"Missing OHLCV folder: {folder}"
            )

        files = sorted(
            folder.glob("KRW-*.csv")
        )
        frames[tf] = {
            "market_files": len(files),
            "example_filename": (
                files[0].name if files else None
            ),
        }

    result = {
        "audit": (
            "pattern_256_event_source_inventory"
        ),
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "report_csv_files": len(rows),
        "readable_csv_files": sum(
            r["status"] == "OK" for r in rows
        ),
        "unreadable_csv_files": sum(
            r["status"] == "ERROR" for r in rows
        ),
        "candidate_files": sum(
            int(r["event_score"] or 0) >= 3
            for r in rows
        ),
        "ohlcv": frames,
        "top_candidates": [
            {
                "path": r["relative_path"],
                "score": r["event_score"],
                "columns": json.loads(
                    r["columns"]
                ),
            }
            for r in rows
            if (
                r["status"] == "OK"
                and int(r["event_score"]) >= 3
            )
        ][:30],
        "event_level_attribution_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Column-name matching is heuristic, "
            "not semantic verification.",
            "Only CSV report files are inventoried; "
            "JSON/Parquet event data may exist.",
            "No full CSV row counts or event-level "
            "outcome verification.",
            "No OHLCV source data modified.",
        ],
    }

    atomic_csv(
        output / "inventory_files.csv", rows
    )
    atomic_json(
        output / "inventory_result.json", result
    )

    manifest = {
        "audit": result["audit"],
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": digest(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            name: digest(output / name)
            for name in (
                "inventory_files.csv",
                "inventory_result.json",
            )
        },
    }

    atomic_json(
        output / "inventory_manifest.json",
        manifest,
    )

    print(json.dumps(
        {
            k: result[k]
            for k in (
                "execution",
                "report_csv_files",
                "readable_csv_files",
                "unreadable_csv_files",
                "candidate_files",
                "top_candidates",
                "research_decision",
            )
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(
            f"[FAIL] {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)
