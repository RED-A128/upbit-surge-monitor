
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upbit Surge Monitor
Clean V003-19

Audit frozen prospective definitions, signal implementation
candidates and real OHLCV source files.

READ ONLY for all source files.
NO signal generation.
NO retrospective event registration.
NO trading.
"""

import argparse
import ast
import csv
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V003-19"
CUTOFF = "2026-10-24T00:00:00+00:00"

FREEZE_REL = Path(
    "data/reports/pattern_256_prospective_freeze"
)
REGISTRY_REL = Path(
    "data/reports/pattern_256_prospective_registry"
)
READINESS_REL = Path(
    "data/reports/pattern_256_prospective_source_readiness"
)
OUTPUT_REL = Path(
    "data/reports/pattern_256_signal_source_audit"
)

OUTPUT_NAMES = (
    "signal_source_audit_result.json",
    "signal_implementation_candidates.csv",
    "ohlcv_source_inventory.csv",
    "signal_source_audit_manifest.json",
)

EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    "actions-runner",
}

SOURCE_KEYWORDS = (
    "pattern_256",
    "accumulation",
    "signal",
    "scanner",
    "feature",
)

TIMEFRAMES = ("h1", "h4", "d1")


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def load_json(path):
    return json.loads(
        path.read_text(encoding="utf-8-sig")
    )


def json_text(data):
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"


def write_atomic(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, name = tempfile.mkstemp(
        prefix=".audit_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temp = Path(name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temp, path)

    finally:
        if temp.exists():
            temp.unlink()


def csv_text(headers, rows):
    import io

    stream = io.StringIO(newline="")
    writer = csv.writer(
        stream,
        lineterminator="\n",
    )
    writer.writerow(headers)
    writer.writerows(rows)
    return stream.getvalue()


def safe_relative(path, root):
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def require_file(path):
    if not path.is_file():
        raise FileNotFoundError(str(path))
    return path


def verify_inputs(root):
    freeze = root / FREEZE_REL
    registry = root / REGISTRY_REL
    readiness = root / READINESS_REL

    freeze_manifest = require_file(
        freeze / "freeze_manifest.json"
    )
    registry_manifest = require_file(
        registry / "registry_manifest.json"
    )
    readiness_manifest = require_file(
        readiness / "source_readiness_manifest.json"
    )

    freeze_data = load_json(freeze_manifest)
    registry_data = load_json(registry_manifest)
    readiness_data = load_json(readiness_manifest)

    if freeze_data.get("version") != "Clean V003-16":
        raise ValueError("Freeze version mismatch")

    if registry_data.get("version") != "Clean V003-17":
        raise ValueError("Registry version mismatch")

    if readiness_data.get("version") != "Clean V003-18":
        raise ValueError("Readiness version mismatch")

    freeze_result_path = require_file(
        freeze / "freeze_result.json"
    )
    registry_result_path = require_file(
        registry / "registry_result.json"
    )
    readiness_result_path = require_file(
        readiness / "source_readiness_result.json"
    )

    freeze_result = load_json(freeze_result_path)
    registry_result = load_json(registry_result_path)
    readiness_result = load_json(readiness_result_path)

    for result in (
        freeze_result,
        registry_result,
        readiness_result,
    ):
        if result.get("execution_status") != "PASS":
            raise ValueError("Input execution status invalid")

        if result.get("overall_decision") != "HOLD":
            raise ValueError("Input decision invalid")

        if result.get("approved_for_live_trading") is not False:
            raise ValueError("Unsafe approval flag")

        if result.get("freeze_cutoff_utc") != CUTOFF:
            raise ValueError("Cutoff mismatch")

    if (
        registry_data.get("freeze_manifest_sha256")
        != digest(freeze_manifest)
    ):
        raise ValueError("Registry freeze hash mismatch")

    if (
        readiness_data.get("result_sha256")
        != digest(readiness_result_path)
    ):
        raise ValueError("Readiness result hash mismatch")

    if (
        registry_data.get("result_sha256")
        != digest(registry_result_path)
    ):
        raise ValueError("Registry result hash mismatch")

    for group, folder in (
        ("freeze", freeze),
        ("registry", registry),
    ):
        expected = readiness_data.get(
            "input_sha256", {}
        ).get(group)

        if not isinstance(expected, dict):
            raise ValueError(
                "Missing readiness input hashes: " + group
            )

        for name, expected_hash in expected.items():
            path = require_file(folder / name)

            if digest(path) != expected_hash:
                raise ValueError(
                    "Input hash mismatch: " + name
                )

    if (
        readiness_result.get("frozen_market_count") != 209
        or readiness_result.get("primary_market") != "KRW-DKA"
        or readiness_result.get("registered_event_count") != 0
    ):
        raise ValueError("Readiness state mismatch")

    if (
        readiness_result.get("signal_generation_implemented")
        is not False
        or readiness_result.get("prospective_source_verified")
        is not False
    ):
        raise ValueError("Unexpected readiness state")

    candidate_path = require_file(
        freeze / "frozen_candidates.csv"
    )

    with candidate_path.open(
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        if not reader.fieldnames:
            raise ValueError("Candidate header missing")

        rows = list(reader)

    if len(rows) != 209:
        raise ValueError("Frozen market count mismatch")

    markets = {
        row["market"] for row in rows
    }

    if len(markets) != 209:
        raise ValueError("Frozen market duplicates")

    protocol_path = require_file(
        freeze / "prospective_protocol.json"
    )

    protocol = load_json(protocol_path)

    if (
        protocol.get("version") != "Clean V003-16"
        or protocol.get("freeze_cutoff_utc") != CUTOFF
    ):
        raise ValueError("Frozen protocol mismatch")

    input_paths = [
        freeze_manifest,
        registry_manifest,
        readiness_manifest,
        freeze_result_path,
        registry_result_path,
        readiness_result_path,
        candidate_path,
        protocol_path,
        registry / "prospective_events.csv",
        readiness / "prospective_event_input_template.csv",
    ]

    hashes = {
        safe_relative(path, root): digest(path)
        for path in input_paths
    }

    return markets, hashes


def discover_python(root):
    candidates = []

    for directory, dirs, files in os.walk(root):
        dirs[:] = [
            name for name in dirs
            if name not in EXCLUDE_DIRS
            and not name.startswith(".")
        ]

        base = Path(directory)

        if (
            OUTPUT_REL.as_posix()
            in base.as_posix()
        ):
            dirs[:] = []
            continue

        for filename in files:
            if not filename.endswith(".py"):
                continue

            path = base / filename
            relative = safe_relative(path, root)

            if relative == Path(__file__).name:
                continue

            if path.stat().st_size > 2_000_000:
                continue

            try:
                source = path.read_text(
                    encoding="utf-8-sig"
                )
                tree = ast.parse(source)
            except (
                UnicodeError,
                SyntaxError,
                OSError,
            ):
                continue

            functions = []
            classes = []

            for node in ast.walk(tree):
                if isinstance(
                    node,
                    (ast.FunctionDef, ast.AsyncFunctionDef),
                ):
                    functions.append(node.name)

                elif isinstance(node, ast.ClassDef):
                    classes.append(node.name)

            searchable = (
                relative.lower()
                + " "
                + " ".join(functions).lower()
                + " "
                + " ".join(classes).lower()
            )

            matched = [
                word for word in SOURCE_KEYWORDS
                if word in searchable
            ]

            if not matched:
                continue

            candidates.append([
                relative,
                digest(path),
                ",".join(matched),
                ",".join(sorted(set(functions))[:40]),
                ",".join(sorted(set(classes))[:20]),
                "NOT_AUDITED",
            ])

    candidates.sort(key=lambda row: row[0])

    return candidates


def looks_like_ohlcv(path):
    name = path.name.lower()
    parent = path.parent.as_posix().lower()

    if path.suffix.lower() != ".csv":
        return False

    if any(
        token in parent
        for token in (
            "/reports/",
            "/.git/",
            "/__pycache__/",
        )
    ):
        return False

    return any(
        token in name or token in parent
        for token in (
            "ohlcv",
            "candles",
            "history",
            "backfill",
        )
    )


def discover_ohlcv(root, markets):
    inventory = []

    for directory, dirs, files in os.walk(root / "data"):
        dirs[:] = [
            name for name in dirs
            if name not in EXCLUDE_DIRS
            and name != "reports"
        ]

        base = Path(directory)

        for filename in files:
            path = base / filename

            if not looks_like_ohlcv(path):
                continue

            try:
                with path.open(
                    encoding="utf-8-sig",
                    newline="",
                ) as stream:
                    reader = csv.reader(stream)
                    header = next(reader, [])

            except (
                OSError,
                UnicodeError,
                csv.Error,
            ):
                header = []

            lower_header = {
                name.strip().lower()
                for name in header
            }

            has_timestamp = bool(
                lower_header.intersection({
                    "timestamp",
                    "datetime",
                    "time",
                    "candle_date_time_utc",
                })
            )

            has_ohlc = all(
                name in lower_header
                for name in (
                    "open",
                    "high",
                    "low",
                    "close",
                )
            )

            name_upper = filename.upper()

            matched_markets = [
                market for market in markets
                if market in name_upper
                or market.replace("-", "_") in name_upper
            ]

            timeframe = "UNKNOWN"

            for tf in TIMEFRAMES:
                tokens = (
                    "_" + tf + ".",
                    "_" + tf + "_",
                    "/" + tf + "/",
                    "-" + tf + ".",
                )

                if any(
                    token in (
                        "/" + path.as_posix().lower()
                    )
                    for token in tokens
                ):
                    timeframe = tf
                    break

            inventory.append([
                safe_relative(path, root),
                path.stat().st_size,
                ",".join(header),
                "YES" if has_timestamp else "NO",
                "YES" if has_ohlc else "NO",
                timeframe,
                ",".join(sorted(matched_markets)),
                "NOT_VERIFIED",
            ])

    inventory.sort(key=lambda row: row[0])

    return inventory


def verify_output(directory, inputs):
    result_path = (
        directory / "signal_source_audit_result.json"
    )
    implementation_path = (
        directory / "signal_implementation_candidates.csv"
    )
    inventory_path = (
        directory / "ohlcv_source_inventory.csv"
    )
    manifest_path = (
        directory / "signal_source_audit_manifest.json"
    )

    if not all(
        path.is_file()
        for path in (
            result_path,
            implementation_path,
            inventory_path,
            manifest_path,
        )
    ):
        return False

    try:
        result = load_json(result_path)
        manifest = load_json(manifest_path)

        return (
            result.get("version") == VERSION
            and manifest.get("version") == VERSION
            and result.get("execution_status") == "PASS"
            and result.get("overall_decision") == "HOLD"
            and result.get("approved_for_live_trading") is False
            and result.get("signal_generation_implemented") is False
            and manifest.get("input_sha256") == inputs
            and manifest.get("result_sha256") == digest(result_path)
            and manifest.get("implementation_sha256")
            == digest(implementation_path)
            and manifest.get("inventory_sha256")
            == digest(inventory_path)
        )

    except (OSError, ValueError, TypeError):
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

    markets, input_hashes = verify_inputs(root)

    if verify_output(output, input_hashes):
        print("[SKIP] Verified previous output")
        print("[DECISION] HOLD")
        return 0

    if output.exists() and any(output.iterdir()):
        raise ValueError(
            "Existing output failed integrity validation; "
            "manual audit required"
        )

    print(
        "[PASS] Frozen input chain",
        flush=True,
    )

    implementations = discover_python(root)

    print(
        "[INFO] Python candidates: "
        + str(len(implementations)),
        flush=True,
    )

    inventory = discover_ohlcv(root, markets)

    print(
        "[INFO] OHLCV inventory candidates: "
        + str(len(inventory)),
        flush=True,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "overall_decision": "HOLD",
        "checked_at_utc": datetime.now(
            timezone.utc
        ).isoformat(timespec="seconds"),
        "freeze_cutoff_utc": CUTOFF,
        "frozen_market_count": len(markets),
        "primary_market": "KRW-DKA",
        "implementation_candidate_count": len(
            implementations
        ),
        "ohlcv_file_candidate_count": len(inventory),
        "signal_definition_mathematically_verified": False,
        "ohlcv_candle_integrity_verified": False,
        "signal_generation_implemented": False,
        "prospective_source_verified": False,
        "new_prospective_events_created": 0,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "next_required_validation": (
            "Review discovered implementation candidates; "
            "verify frozen signal formulas and OHLCV timestamps "
            "before generating any post-cutoff signals"
        ),
    }

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output / "signal_source_audit_result.json"
    )
    implementation_path = (
        output / "signal_implementation_candidates.csv"
    )
    inventory_path = (
        output / "ohlcv_source_inventory.csv"
    )
    manifest_path = (
        output / "signal_source_audit_manifest.json"
    )

    write_atomic(
        implementation_path,
        csv_text(
            (
                "python_file",
                "sha256",
                "matched_keywords",
                "functions",
                "classes",
                "audit_status",
            ),
            implementations,
        ),
    )

    write_atomic(
        inventory_path,
        csv_text(
            (
                "source_file",
                "file_size_bytes",
                "header",
                "has_timestamp_column",
                "has_ohlc_columns",
                "detected_timeframe",
                "matched_frozen_markets",
                "integrity_status",
            ),
            inventory,
        ),
    )

    write_atomic(
        result_path,
        json_text(result),
    )

    manifest = {
        "version": VERSION,
        "input_sha256": input_hashes,
        "result_sha256": digest(result_path),
        "implementation_sha256": digest(
            implementation_path
        ),
        "inventory_sha256": digest(
            inventory_path
        ),
    }

    write_atomic(
        manifest_path,
        json_text(manifest),
    )

    if not verify_output(output, input_hashes):
        raise ValueError(
            "Final output verification failed"
        )

    print("[PASS] " + VERSION)
    print("[PASS] Source inventory generated")
    print("[PASS] Output SHA256 verified")
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
