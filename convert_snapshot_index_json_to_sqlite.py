#!/usr/bin/env python3

import argparse
import json
import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS manifest_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segment_summaries (
    segment_start INTEGER NOT NULL,
    segment_end INTEGER NOT NULL,
    segment_path TEXT NOT NULL,
    checkpoint_count INTEGER NOT NULL,
    min_block_height INTEGER,
    max_block_height INTEGER,
    PRIMARY KEY (segment_start, segment_end)
);

CREATE TABLE IF NOT EXISTS checkpoint_entries (
    block_height INTEGER PRIMARY KEY,
    block_time TEXT NOT NULL,
    segment_start INTEGER NOT NULL,
    segment_end INTEGER NOT NULL,
    segment_path TEXT NOT NULL,
    offset INTEGER NOT NULL,
    len INTEGER NOT NULL,
    codec TEXT NOT NULL,
    compression_level INTEGER,
    raw_len INTEGER,
    asset_count INTEGER NOT NULL,
    include_users INTEGER NOT NULL,
    include_trigger_orders INTEGER NOT NULL,
    assets_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_checkpoint_entries_segment
ON checkpoint_entries (segment_start, segment_end, block_height);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert snapshot_index*.json into snapshot_index.sqlite")
    parser.add_argument(
        "dataset_dir",
        nargs="?",
        default=".",
        help="Directory containing snapshot_index*.json files",
    )
    parser.add_argument(
        "--output",
        default="snapshot_index.sqlite",
        help="SQLite filename to create inside dataset_dir",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace an existing SQLite output file",
    )
    return parser.parse_args()


def load_json_indexes(dataset_dir: Path) -> tuple[int, list[dict], list[dict]]:
    version = None
    summaries: dict[tuple[int, int], dict] = {}
    entries: list[dict] = []

    json_paths = sorted(dataset_dir.glob("snapshot_index*.json"))
    if not json_paths:
        raise FileNotFoundError(f"no snapshot_index*.json found under {dataset_dir}")

    for json_path in json_paths:
        payload = json.loads(json_path.read_text())
        current_version = int(payload.get("version", 0))
        version = current_version if version is None else max(version, current_version)
        for segment in payload.get("segments", []):
            segment_start = int(segment["segment_start"])
            segment_end = int(segment["segment_end"])
            key = (segment_start, segment_end)
            summaries[key] = {
                "segment_start": segment_start,
                "segment_end": segment_end,
                "segment_path": segment["segment_path"],
                "checkpoint_count": int(segment["checkpoint_count"]),
                "min_block_height": segment.get("min_block_height"),
                "max_block_height": segment.get("max_block_height"),
            }
            for entry in segment.get("entries", []):
                entries.append(
                    {
                        "block_height": int(entry["block_height"]),
                        "block_time": entry["block_time"],
                        "segment_start": segment_start,
                        "segment_end": segment_end,
                        "segment_path": segment["segment_path"],
                        "offset": int(entry["offset"]),
                        "len": int(entry["len"]),
                        "codec": entry["codec"],
                        "compression_level": entry.get("compression_level"),
                        "raw_len": entry.get("raw_len"),
                        "asset_count": int(entry["asset_count"]),
                        "include_users": 1 if entry["include_users"] else 0,
                        "include_trigger_orders": 1 if entry["include_trigger_orders"] else 0,
                        "assets_json": json.dumps(entry["assets"]) if entry.get("assets") is not None else None,
                    }
                )

    return version or 0, list(summaries.values()), entries


def write_sqlite(output_path: Path, version: int, summaries: list[dict], entries: list[dict]) -> None:
    conn = sqlite3.connect(output_path)
    try:
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.executescript(SCHEMA)
        with conn:
            conn.execute(
                "INSERT INTO manifest_meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                ("version", str(version)),
            )
            conn.executemany(
                """
                INSERT INTO segment_summaries (
                    segment_start, segment_end, segment_path, checkpoint_count, min_block_height, max_block_height
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["segment_start"],
                        row["segment_end"],
                        row["segment_path"],
                        row["checkpoint_count"],
                        row["min_block_height"],
                        row["max_block_height"],
                    )
                    for row in summaries
                ],
            )
            conn.executemany(
                """
                INSERT INTO checkpoint_entries (
                    block_height, block_time, segment_start, segment_end, segment_path,
                    offset, len, codec, compression_level, raw_len, asset_count,
                    include_users, include_trigger_orders, assets_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["block_height"],
                        row["block_time"],
                        row["segment_start"],
                        row["segment_end"],
                        row["segment_path"],
                        row["offset"],
                        row["len"],
                        row["codec"],
                        row["compression_level"],
                        row["raw_len"],
                        row["asset_count"],
                        row["include_users"],
                        row["include_trigger_orders"],
                        row["assets_json"],
                    )
                    for row in entries
                ],
            )
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    dataset_dir = Path(args.dataset_dir).expanduser().resolve()
    output_path = dataset_dir / args.output

    if output_path.exists():
        if not args.replace:
            raise SystemExit(f"{output_path} already exists; pass --replace to overwrite it")
        output_path.unlink()

    version, summaries, entries = load_json_indexes(dataset_dir)
    write_sqlite(output_path, version, summaries, entries)
    print(
        f"wrote {output_path} from {len(summaries)} segment summaries and {len(entries)} checkpoint entries"
    )


if __name__ == "__main__":
    main()
