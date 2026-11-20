
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 PIT Schema & Alignment Audit
Clean V006

Read-only D1 CSV alignment audit.
Does not prove point-in-time causality, OOS validity,
statistical significance or profitability.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = "Clean V006"
KST = timezone(timedelta(hours=9))

FOLDERS = {
    "ohlcv": "data/ohlcv/d1",
    "features": "data/features/d1",
    "signals": "data/signals/256/d1",
}

PREVIOUS = (
    "data/reports/pattern_256_pit_readiness"
)

OUTPUT = (
    "data/reports/pattern_256_pit_alignment"
)

TIME_FIELDS = (
    "timestamp",
    "candle_date_time_utc",
    "candle_date_time_kst",
)

PRICE_FIELDS = (
    "open", "high", "low", "close",
    "volume", "trade_value",
)

SIGNAL_FIELDS = (
    "signal_setup",
    "signal_ready",
    "signal_trigger",
    "signal_stage",
)

BOOL_VALUES = {"0", "1", "0.0", "1.0", "true", "false"}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, value):
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            value, f, indent=2, ensure_ascii=False,
            allow_nan=False,
        )
        f.write("\n")


def fail(message):
    raise RuntimeError(message)


def parse_datetime(value, zone):
    if value is None or not str(value).strip():
        raise ValueError("empty datetime")

    raw = str(value).strip()
    dt = datetime.fromisoformat(
        raw.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone)

    return dt.astimezone(timezone.utc)


def parse_epoch(value):
    number = float(value)

    if not math.isfinite(number):
        raise ValueError("nonfinite timestamp")

    # Upbit timestamps are commonly milliseconds.
    if abs(number) >= 1e14:
        number /= 1000000
    elif abs(number) >= 1e11:
        number /= 1000

    return datetime.fromtimestamp(
        number, tz=timezone.utc
    )


def parse_time(row):
    utc = parse_datetime(
        row["candle_date_time_utc"],
        timezone.utc,
    )

    kst = parse_datetime(
        row["candle_date_time_kst"],
        KST,
    )

    epoch = parse_epoch(row["timestamp"])

    if abs((utc - kst).total_seconds()) > 1:
        raise ValueError("UTC_KST_MISMATCH")

    if abs((utc - epoch).total_seconds()) > 1:
        raise ValueError("UTC_EPOCH_MISMATCH")

    return utc


def numeric_equal(a, b):
    if a == b:
        return True

    try:
        x = float(a)
        y = float(b)

        if not math.isfinite(x) or not math.isfinite(y):
            return False

        return math.isclose(
            x, y, rel_tol=1e-10, abs_tol=1e-12
        )
    except (ValueError, TypeError):
        return False


def read_csv(path, category, market):
    rows = []
    times = []
    issues = Counter()
    examples = []

    def issue(name, row_number=None):
        issues[name] += 1

        if len(examples) < 12:
            examples.append({
                "issue": name,
                "row": row_number,
            })

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            fail("Missing CSV header: " + str(path))

        columns = reader.fieldnames

        if len(columns) != len(set(columns)):
            fail("Duplicate CSV header: " + str(path))

        required = (
            ["market", *TIME_FIELDS, *PRICE_FIELDS]
        )

        if category == "signals":
            required += list(SIGNAL_FIELDS)

        missing = [
            c for c in required if c not in columns
        ]

        if missing:
            fail(
                f"{path}: missing columns {missing}"
            )

        previous = None
        seen = set()

        for number, row in enumerate(reader, start=2):
            if None in row:
                issue("MALFORMED_ROW", number)

            if row.get("market", "").upper() != market:
                issue("MARKET_MISMATCH", number)

            try:
                stamp = parse_time(row)
                key = stamp.isoformat()
            except (
                ValueError, OverflowError, OSError,
                KeyError, TypeError,
            ) as exc:
                issue(
                    "TIME_INVALID_" + type(exc).__name__,
                    number,
                )
                stamp = None
                key = None

            if stamp is not None:
                if key in seen:
                    issue("DUPLICATE_TIME", number)

                seen.add(key)

                if previous is not None:
                    if stamp < previous:
                        issue("OUT_OF_ORDER", number)
                    elif stamp == previous:
                        issue("REPEATED_TIME", number)
                    elif (
                        stamp - previous
                    ) > timedelta(days=1, seconds=1):
                        issue("DAILY_GAP", number)

                previous = stamp

                if stamp > datetime.now(
                    timezone.utc
                ) + timedelta(days=1):
                    issue("FUTURE_TIME", number)

            for field in PRICE_FIELDS:
                value = row.get(field, "")

                try:
                    number_value = float(value)

                    if not math.isfinite(number_value):
                        issue("NONFINITE_" + field, number)
                except (ValueError, TypeError):
                    issue("NONNUMERIC_" + field, number)

            if category == "signals":
                for field in SIGNAL_FIELDS[:3]:
                    value = str(
                        row.get(field, "")
                    ).strip().lower()

                    if value not in BOOL_VALUES:
                        issue(
                            "INVALID_" + field,
                            number,
                        )

                if not str(
                    row.get("signal_stage", "")
                ).strip():
                    issue("EMPTY_SIGNAL_STAGE", number)

            rows.append(row)
            times.append(key)

    return {
        "rows": rows,
        "times": times,
        "issues": issues,
        "examples": examples,
        "columns": columns,
    }


def verify_previous(root):
    folder = root / PREVIOUS
    result_path = folder / "result.json"
    manifest_path = folder / "manifest.json"

    if not result_path.is_file():
        fail("Clean V005 result missing")

    if not manifest_path.is_file():
        fail("Clean V005 manifest missing")

    result = load_json(result_path)
    manifest = load_json(manifest_path)

    if result.get("version") != "Clean V005":
        fail("Unexpected previous version")

    if result.get("execution") != "PASS":
        fail("Previous execution not PASS")

    if result.get("research_decision") != "HOLD":
        fail("Previous research decision mismatch")

    expected = manifest.get(
        "sha256", {}
    ).get("result.json", "")

    actual = digest(result_path)

    if actual != expected.lower():
        fail("Clean V005 SHA256 mismatch")

    return result, actual


def compare_layers(market, data):
    issues = Counter()
    examples = []

    def add(name, row):
        issues[name] += 1
        if len(examples) < 12:
            examples.append({
                "issue": name,
                "row": row,
            })

    base = data["ohlcv"]
    features = data["features"]
    signals = data["signals"]

    if not (
        len(base["rows"])
        == len(features["rows"])
        == len(signals["rows"])
    ):
        add("ROW_COUNT_MISMATCH", None)

    for category in ("features", "signals"):
        target = data[category]

        if base["times"] != target["times"]:
            add("TIME_SEQUENCE_MISMATCH_" + category, None)

        for index in range(
            min(len(base["rows"]), len(target["rows"]))
        ):
            left = base["rows"][index]
            right = target["rows"][index]

            if (
                base["times"][index]
                != target["times"][index]
            ):
                continue

            for field in PRICE_FIELDS:
                if not numeric_equal(
                    left[field], right[field]
                ):
                    add(
                        "OHLCV_MISMATCH_"
                        + category + "_" + field,
                        index + 2,
                    )

    # Signals should retain the feature input values
    # without changing them.
    common_features = [
        c for c in features["columns"]
        if c not in (
            "market", *TIME_FIELDS,
            *PRICE_FIELDS,
        ) and c in signals["columns"]
    ]

    for index in range(
        min(
            len(features["rows"]),
            len(signals["rows"]),
        )
    ):
        if (
            features["times"][index]
            != signals["times"][index]
        ):
            continue

        left = features["rows"][index]
        right = signals["rows"][index]

        for field in common_features:
            a = str(left[field]).strip()
            b = str(right[field]).strip()

            if a == b:
                continue

            if a == "" and b == "":
                continue

            if not numeric_equal(a, b):
                add(
                    "FEATURE_SIGNAL_MISMATCH_"
                    + field,
                    index + 2,
                )

    return issues, examples


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()

    if not root.is_dir():
        fail("Project root missing")

    previous, previous_hash = verify_previous(root)

    inventories = {}
    markets = {}

    for category, relative in FOLDERS.items():
        folder = root / relative

        if not folder.is_dir():
            fail("Missing folder: " + str(folder))

        inventory = {
            p.stem.upper(): p
            for p in folder.glob("KRW-*.csv")
            if p.is_file()
        }

        if not inventory:
            fail("No CSV files: " + str(folder))

        inventories[category] = inventory
        markets[category] = set(inventory)

    common = set.intersection(*markets.values())
    union = set.union(*markets.values())

    issue_counts = Counter()
    details = []
    total_rows = Counter()

    for category in FOLDERS:
        missing = union - markets[category]
        if missing:
            issue_counts[
                "MISSING_MARKET_" + category
            ] += len(missing)

    for market in sorted(common):
        layers = {}
        file_info = {}

        for category in FOLDERS:
            path = inventories[category][market]
            layer = read_csv(path, category, market)
            layers[category] = layer

            total_rows[category] += len(layer["rows"])

            issue_counts.update(layer["issues"])

            file_info[category] = {
                "path": str(path),
                "sha256": digest(path),
                "rows": len(layer["rows"]),
                "first_time": next(
                    (
                        t for t in layer["times"]
                        if t is not None
                    ),
                    None,
                ),
                "last_time": next(
                    (
                        t for t in reversed(
                            layer["times"]
                        ) if t is not None
                    ),
                    None,
                ),
                "issues": dict(layer["issues"]),
                "examples": layer["examples"],
            }

        alignment_issues, examples = compare_layers(
            market, layers
        )

        issue_counts.update(alignment_issues)

        details.append({
            "market": market,
            "files": file_info,
            "alignment_issues": dict(
                alignment_issues
            ),
            "alignment_examples": examples,
        })

        print(
            f"[SCAN] {market} "
            f"issues={sum(alignment_issues.values())}",
            flush=True,
        )

    expected = previous.get("categories", {})

    for category in FOLDERS:
        old_count = expected.get(
            category, {}
        ).get("row_count")

        if old_count != total_rows[category]:
            issue_counts[
                "V005_ROW_COUNT_CHANGED_" + category
            ] += 1

    readiness = (
        "READY_FOR_CAUSAL_REPLAY"
        if not issue_counts
        else "BLOCKED"
    )

    output = root / OUTPUT
    output.mkdir(parents=True, exist_ok=True)

    result = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "source_version": "Clean V005",
        "source_result_sha256": previous_hash,
        "candidate": "d1|DOWN|LOW",
        "market_counts": {
            c: len(markets[c]) for c in FOLDERS
        },
        "common_market_count": len(common),
        "row_counts": dict(total_rows),
        "issue_counts": dict(issue_counts),
        "data_readiness": readiness,
        "details": details,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    result_path = output / "result.json"
    report_path = output / "report.md"
    manifest_path = output / "manifest.json"

    save_json(result_path, result)

    report = [
        "# Pattern 256 PIT Schema & Alignment Audit",
        "",
        f"Version: {VERSION}",
        "",
        "Execution: PASS",
        "",
        f"Data readiness: {readiness}",
        "",
        f"Common markets: {len(common)}",
        "",
        "## Issue counts",
        "",
    ]

    if issue_counts:
        for name, count in sorted(
            issue_counts.items()
        ):
            report.append(f"- {name}: {count}")
    else:
        report.append("- None detected")

    report.extend([
        "",
        "## Limitations",
        "",
        "- Time alignment is not causal replay proof.",
        "- OHLCV equality is not feature formula proof.",
        "- No independent OOS verification.",
        "- No statistical significance verification.",
        "- No realized profitability verification.",
        "- Research decision remains HOLD.",
        "",
    ])

    report_path.write_text(
        "\n".join(report), encoding="utf-8"
    )

    save_json(manifest_path, {
        "version": VERSION,
        "source_result_sha256": previous_hash,
        "sha256": {
            "result.json": digest(result_path),
            "report.md": digest(report_path),
        },
    })

    print("[PASS] Clean V006 execution")
    print("Readiness:", readiness)
    print("Markets:", len(common))
    print("Issues:", dict(issue_counts))
    print("Research decision: HOLD")
    print("Report:", output)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
