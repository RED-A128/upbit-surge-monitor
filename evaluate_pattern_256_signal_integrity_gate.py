
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upbit Surge Monitor
Clean V003-20

Frozen primary-market OHLCV integrity and signal-source gate.

- Verifies Clean V003-19 audit outputs and SHA256.
- Verifies exact KRW-DKA OHLCV paths for h1/h4/d1.
- Audits timestamp order, duplicates, candle values and gaps.
- Checks signal implementation files against prior audit hashes.
- Does not generate or register signals.
- Does not alter OHLCV or frozen research data.
- Does not trade.
"""

import argparse
import ast
import csv
import hashlib
import io
import json
import math
import os
import sys
import tempfile

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V003-20"
PREVIOUS_VERSION = "Clean V003-19"

CUTOFF = "2026-10-24T00:00:00+00:00"
PRIMARY_MARKET = "KRW-DKA"

AUDIT_REL = Path(
    "data/reports/pattern_256_signal_source_audit"
)

OUTPUT_REL = Path(
    "data/reports/pattern_256_signal_integrity_gate"
)

TIMEFRAMES = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

REQUIRED_IMPLEMENTATIONS = (
    "build_features.py",
    "detect_256.py",
    "validate_256.py",
)

REQUIRED_CSV_COLUMNS = (
    "market",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
)

OUTPUT_FILES = (
    "signal_integrity_result.json",
    "ohlcv_integrity_details.csv",
    "signal_implementation_integrity.csv",
    "signal_integrity_manifest.json",
)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    return json.loads(
        path.read_text(encoding="utf-8-sig")
    )


def json_text(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"


def csv_text(columns, records):
    stream = io.StringIO(newline="")
    writer = csv.writer(
        stream,
        lineterminator="\n",
    )

    writer.writerow(columns)
    writer.writerows(records)

    return stream.getvalue()


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temp_name = tempfile.mkstemp(
        prefix=".signal_gate_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temp_name)

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def relative(path, root):
    return path.relative_to(root).as_posix()


def require_file(path):
    if not path.is_file():
        raise FileNotFoundError(
            "Required file missing: " + str(path)
        )

    return path


def parse_timestamp(value):
    value = str(value).strip()

    if not value:
        raise ValueError("Empty timestamp")

    # Upbit collector commonly stores Unix milliseconds.
    try:
        numeric = float(value)

        if not math.isfinite(numeric):
            raise ValueError("Non-finite timestamp")

        if abs(numeric) >= 100_000_000_000:
            numeric /= 1000.0

        return datetime.fromtimestamp(
            numeric,
            tz=timezone.utc,
        )

    except ValueError:
        pass

    result = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if result.tzinfo is None:
        raise ValueError(
            "Timezone missing in timestamp"
        )

    return result.astimezone(timezone.utc)


def finite_number(value, field, line):
    try:
        number = float(value)
    except (ValueError, TypeError):
        raise ValueError(
            f"Line {line}: invalid {field}"
        )

    if not math.isfinite(number):
        raise ValueError(
            f"Line {line}: non-finite {field}"
        )

    return number


def verify_previous_audit(root):
    directory = root / AUDIT_REL

    manifest_path = require_file(
        directory / "signal_source_audit_manifest.json"
    )

    result_path = require_file(
        directory / "signal_source_audit_result.json"
    )

    implementation_path = require_file(
        directory / "signal_implementation_candidates.csv"
    )

    inventory_path = require_file(
        directory / "ohlcv_source_inventory.csv"
    )

    manifest = load_json(manifest_path)
    result = load_json(result_path)

    if (
        manifest.get("version") != PREVIOUS_VERSION
        or result.get("version") != PREVIOUS_VERSION
    ):
        raise ValueError("Previous audit version mismatch")

    if (
        result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
        or result.get("approved_for_live_trading") is not False
        or result.get("signal_generation_implemented") is not False
    ):
        raise ValueError("Previous audit state invalid")

    if (
        result.get("primary_market") != PRIMARY_MARKET
        or result.get("freeze_cutoff_utc") != CUTOFF
    ):
        raise ValueError("Frozen configuration mismatch")

    outputs = {
        "result_sha256": result_path,
        "implementation_sha256": implementation_path,
        "inventory_sha256": inventory_path,
    }

    for key, path in outputs.items():
        if sha256_file(path) != manifest.get(key):
            raise ValueError(
                "Previous audit SHA256 mismatch: " + key
            )

    previous_inputs = manifest.get("input_sha256")

    if not isinstance(previous_inputs, dict):
        raise ValueError("Missing previous input manifest")

    for name, expected in previous_inputs.items():
        path = require_file(root / name)

        if sha256_file(path) != expected:
            raise ValueError(
                "Frozen source changed: " + name
            )

    with implementation_path.open(
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                "Implementation inventory header missing"
            )

        implementations = {
            row["python_file"]: row
            for row in reader
        }

    with inventory_path.open(
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                "OHLCV inventory header missing"
            )

        inventory = {
            row["source_file"]: row
            for row in reader
        }

    return {
        "manifest": manifest,
        "manifest_sha256": sha256_file(manifest_path),
        "implementations": implementations,
        "inventory": inventory,
    }


def verify_implementation(root, name, prior):
    if name not in prior:
        raise ValueError(
            "Implementation absent from previous audit: "
            + name
        )

    path = require_file(root / name)
    current_hash = sha256_file(path)
    previous_hash = prior[name].get("sha256")

    if current_hash != previous_hash:
        raise ValueError(
            "Implementation changed since V003-19: "
            + name
        )

    source = path.read_text(encoding="utf-8-sig")
    tree = ast.parse(source, filename=name)

    functions = sorted({
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (ast.FunctionDef, ast.AsyncFunctionDef),
        )
    })

    return [
        name,
        current_hash,
        len(functions),
        "PASS",
        "NOT_MATHEMATICALLY_VERIFIED",
    ]


def verify_ohlcv(root, timeframe, interval, inventory):
    rel = (
        Path("data/ohlcv")
        / timeframe
        / (PRIMARY_MARKET + ".csv")
    )

    key = rel.as_posix()

    if key not in inventory:
        raise ValueError(
            "Primary OHLCV absent from previous inventory: "
            + key
        )

    path = require_file(root / rel)

    rows = 0
    duplicates = 0
    out_of_order = 0
    gap_count = 0
    missing_intervals = 0
    pre_cutoff = 0
    post_cutoff = 0

    first_time = None
    last_time = None
    previous_time = None

    cutoff = datetime.fromisoformat(CUTOFF)

    with path.open(
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError(
                "Missing OHLCV header: " + key
            )

        missing = [
            field
            for field in REQUIRED_CSV_COLUMNS
            if field not in reader.fieldnames
        ]

        if missing:
            raise ValueError(
                "Missing OHLCV columns: "
                + key
                + " "
                + ",".join(missing)
            )

        for line, record in enumerate(reader, start=2):
            rows += 1

            if record["market"].strip() != PRIMARY_MARKET:
                raise ValueError(
                    f"{key}:{line}: market mismatch"
                )

            timestamp = parse_timestamp(
                record["timestamp"]
            )

            opening = finite_number(
                record["open"], "open", line
            )
            high = finite_number(
                record["high"], "high", line
            )
            low = finite_number(
                record["low"], "low", line
            )
            closing = finite_number(
                record["close"], "close", line
            )
            volume = finite_number(
                record["volume"], "volume", line
            )
            trade_value = finite_number(
                record["trade_value"],
                "trade_value",
                line,
            )

            if (
                low <= 0
                or opening <= 0
                or closing <= 0
                or high <= 0
                or volume < 0
                or trade_value < 0
            ):
                raise ValueError(
                    f"{key}:{line}: invalid candle values"
                )

            if (
                high < max(opening, closing, low)
                or low > min(opening, closing, high)
            ):
                raise ValueError(
                    f"{key}:{line}: OHLC relationship invalid"
                )

            if timestamp < cutoff:
                pre_cutoff += 1
            else:
                post_cutoff += 1

            if first_time is None:
                first_time = timestamp

            if previous_time is not None:
                delta = (
                    timestamp - previous_time
                ).total_seconds()

                if delta == 0:
                    duplicates += 1

                elif delta < 0:
                    out_of_order += 1

                elif delta > interval:
                    gap_count += 1

                    if delta % interval == 0:
                        missing_intervals += (
                            int(delta // interval) - 1
                        )

                elif delta != interval:
                    raise ValueError(
                        f"{key}:{line}: unexpected interval"
                    )

            previous_time = timestamp
            last_time = timestamp

    if rows == 0:
        raise ValueError(
            "Empty OHLCV source: " + key
        )

    if duplicates or out_of_order:
        raise ValueError(
            "Duplicate or unsorted candles: " + key
        )

    return [
        key,
        sha256_file(path),
        timeframe,
        rows,
        first_time.isoformat(),
        last_time.isoformat(),
        pre_cutoff,
        post_cutoff,
        gap_count,
        missing_intervals,
        "PASS_WITH_GAPS" if gap_count else "PASS",
    ]


def verify_existing(output, source_hashes):
    paths = {
        name: output / name
        for name in OUTPUT_FILES
    }

    if not all(path.is_file() for path in paths.values()):
        return False

    try:
        result = load_json(
            paths["signal_integrity_result.json"]
        )

        manifest = load_json(
            paths["signal_integrity_manifest.json"]
        )

        if (
            result.get("version") != VERSION
            or manifest.get("version") != VERSION
            or result.get("execution_status") != "PASS"
            or result.get("overall_decision") != "HOLD"
            or result.get("approved_for_live_trading") is not False
            or result.get("signal_generation_implemented") is not False
            or manifest.get("input_sha256") != source_hashes
        ):
            return False

        for name in OUTPUT_FILES:
            if name.endswith("manifest.json"):
                continue

            if (
                manifest.get("output_sha256", {}).get(name)
                != sha256_file(paths[name])
            ):
                return False

        return True

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()
    output = (root / OUTPUT_REL).resolve()

    audit = verify_previous_audit(root)

    implementation_rows = []

    for name in REQUIRED_IMPLEMENTATIONS:
        implementation_rows.append(
            verify_implementation(
                root,
                name,
                audit["implementations"],
            )
        )

    print(
        "[PASS] 3 implementation file hashes",
        flush=True,
    )

    ohlcv_rows = []

    for timeframe, interval in TIMEFRAMES.items():
        record = verify_ohlcv(
            root,
            timeframe,
            interval,
            audit["inventory"],
        )

        ohlcv_rows.append(record)

        print(
            "[CHECK] "
            + timeframe
            + " rows="
            + str(record[3])
            + " gaps="
            + str(record[8]),
            flush=True,
        )

    source_hashes = {
        "previous_audit_manifest":
            audit["manifest_sha256"],
        "implementation_files": {
            row[0]: row[1]
            for row in implementation_rows
        },
        "ohlcv_files": {
            row[0]: row[1]
            for row in ohlcv_rows
        },
    }

    if verify_existing(output, source_hashes):
        print("[SKIP] Verified prior gate output")
        print("[DECISION] HOLD")
        return 0

    if output.exists() and any(output.iterdir()):
        raise ValueError(
            "Existing output failed integrity check; "
            "manual review required"
        )

    total_gaps = sum(
        row[8] for row in ohlcv_rows
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "overall_decision": "HOLD",
        "checked_at_utc": datetime.now(
            timezone.utc
        ).isoformat(timespec="seconds"),
        "freeze_cutoff_utc": CUTOFF,
        "primary_market": PRIMARY_MARKET,
        "timeframes_checked": list(TIMEFRAMES),
        "implementation_files_verified":
            len(implementation_rows),
        "ohlcv_files_verified": len(ohlcv_rows),
        "ohlcv_total_rows": sum(
            row[3] for row in ohlcv_rows
        ),
        "ohlcv_gap_count": total_gaps,
        "ohlcv_structural_integrity_verified": True,
        "ohlcv_continuity_verified": total_gaps == 0,
        "implementation_source_hashes_verified": True,
        "signal_definition_mathematically_verified": False,
        "signal_generation_implemented": False,
        "prospective_source_verified": False,
        "new_prospective_events_created": 0,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "next_required_validation": (
            "Audit the frozen 256 calculation against "
            "build_features.py and detect_256.py; "
            "verify candle availability at signal time "
            "and exclude incomplete or gapped windows"
        ),
    }

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output / "signal_integrity_result.json"
    )

    ohlcv_path = (
        output / "ohlcv_integrity_details.csv"
    )

    implementation_path = (
        output / "signal_implementation_integrity.csv"
    )

    manifest_path = (
        output / "signal_integrity_manifest.json"
    )

    atomic_write(
        ohlcv_path,
        csv_text(
            (
                "source_file",
                "sha256",
                "timeframe",
                "rows",
                "first_timestamp",
                "last_timestamp",
                "pre_cutoff_rows",
                "post_cutoff_rows",
                "gap_count",
                "missing_intervals",
                "status",
            ),
            ohlcv_rows,
        ),
    )

    atomic_write(
        implementation_path,
        csv_text(
            (
                "python_file",
                "sha256",
                "function_count",
                "source_integrity",
                "mathematical_audit",
            ),
            implementation_rows,
        ),
    )

    atomic_write(
        result_path,
        json_text(result),
    )

    manifest = {
        "version": VERSION,
        "input_sha256": source_hashes,
        "output_sha256": {
            name: sha256_file(output / name)
            for name in OUTPUT_FILES
            if name != "signal_integrity_manifest.json"
        },
    }

    atomic_write(
        manifest_path,
        json_text(manifest),
    )

    if not verify_existing(output, source_hashes):
        raise ValueError(
            "Final output integrity check failed"
        )

    print("[PASS] " + VERSION)
    print("[PASS] OHLCV structural integrity")
    print("[PASS] Source SHA256")
    print("[INFO] Total candle gaps: " + str(total_gaps))
    print("[DECISION] HOLD")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
