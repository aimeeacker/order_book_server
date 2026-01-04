#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import urllib.error
import urllib.request

import orjson


DEFAULT_ENDPOINT = "http://localhost:3001/info"
DEFAULT_OUT_PATH = "/dev/shm/snapshot.json"
OUTPUTS = {
    "BTC": "/dev/shm/btc.json",
    "ETH": "/dev/shm/eth.json",
}


def fetch_snapshot_bytes(endpoint: str, out_path: str) -> bytes:
    payload = {
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": True,
            "includeTriggerOrders": False,
        },
        "outPath": out_path,
        "includeHeightInOutput": True,
    }
    body = orjson.dumps(payload)
    request = urllib.request.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_bytes = response.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to call {endpoint}: {exc}") from exc

    stripped = response_bytes.lstrip()
    if stripped.startswith(b"["):
        return response_bytes

    if not os.path.exists(out_path):
        raise RuntimeError(f"Snapshot output file missing: {out_path}")
    with open(out_path, "rb") as handle:
        return handle.read()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch snapshot and extract BTC/ETH.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--out-path", default=DEFAULT_OUT_PATH)
    args = parser.parse_args()

    snapshot_bytes = fetch_snapshot_bytes(args.endpoint, args.out_path)
    payload = orjson.loads(snapshot_bytes)
    if not isinstance(payload, list) or len(payload) != 2:
        raise RuntimeError("Unexpected snapshot payload format")

    height, snapshots = payload
    if not isinstance(snapshots, list):
        raise RuntimeError("Unexpected snapshot data format")

    selected = {}
    for entry in snapshots:
        if not isinstance(entry, list) or len(entry) != 2:
            continue
        coin, levels = entry
        if coin in OUTPUTS:
            selected[coin] = {"height": height, "coin": coin, "levels": levels}

    for coin, path in OUTPUTS.items():
        if coin not in selected:
            continue
        with open(path, "wb") as handle:
            handle.write(orjson.dumps(selected[coin]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
