# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
  `n_levels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.
- Additional local endpoints:
  - `l4Lite`: per-block level-2 style updates derived from L4 state.
  - `l4Anal`: analysis rollups emitted every 10 blocks.

The `l4book` subscription first sends a snapshot of the entire book and then forwards order diffs by block. The subscription format is:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "<coin_symbol>"
  }
}
```

`l4Lite`/`l4Anal` use stream-style subscriptions (example):

```json
{
  "method": "subscribe",
  "streams": ["BTC@l4Lite", "BTC@l4Anal"],
  "req_id": 1
}
```

### `l4Anal` payload notes

- `l4Anal` frames are emitted when `block_height % 10 == 0`.
- The rollup represents the latest 10-block window only (not long-horizon rolling accumulation).
- `window_bids/window_asks[].vals` keep 4 values:
  `[fill_sz, fill_notional, change_sz, change_notional]`.
- `window_sum_bid/window_sum_ask` contain 8 positional values:
  `[fill_sz, fill_notional, change_sz, change_notional, add_sz, add_notional, remove_sz, remove_notional]`.

## Setup

1. Run a non-validating node (from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)). Requires batching by block. Requires recording fills, order statuses, and raw book diffs.
   - The node should write newline-delimited JSON batches into FIFOs at `/home/aimee/hl_runtime/hl_book/node_fifo`:
     - `fills`
     - `order`
     - `diffs`
   - Snapshot requests are written to `/home/aimee/hl_runtime/hl_book/snapshot.json`.

2. Start the FIFO merge utility (exposes the merged UDS stream at `/home/aimee/hl_runtime/hl_book/fifo_listener.sock`):

```bash
cargo run -p fifo_listener
```

3. Then run this local server:

```bash
cargo run --release --bin websocket_server
```

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (currently set to 5 seconds).
In addition, the local server periodically fetches order book snapshots from the node, and compares to its own internal state. If a difference is detected, it will exit.

If you want logging, prepend the command with `RUST_LOG=info`.
To override the default bind address/port, pass `--address` and `--port` explicitly.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## FIFO Utilities

- `cargo run -p fifo_listener`: parse and filter FIFO streams, align on block height, and emit timing metrics.
- Python bindings: build `fifo_listener` as a `cdylib` and see `fifo_listener/python_example.py`.

## Caveats

- This server does **not** show untriggered trigger orders.
- It currently **does not** support spot order books.
- The current implementation batches node outputs by block, making the order book a few milliseconds slower than a streaming implementation.
