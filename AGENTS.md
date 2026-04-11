# Repository Guidelines

This repository hosts a Rust workspace for a local WebSocket server that mirrors Hyperliquid order book subscriptions. It includes library code in `server/`, runnable binaries in `binaries/`, and a standalone FIFO utility in `fifo_listener/` (the server consumes the merged UDS stream).

## Project Structure & Module Organization

- `server/src/`: core library crate (listeners, order book logic, servers, types).
- `binaries/src/bin/`: runnable entry points like `websocket_server.rs` and `example_client.rs`.
- `fifo_listener/`: FIFO listener that merges the three streams and exposes them over UDS.
- `Cargo.toml`: workspace config and lint settings.
- `rustfmt.toml`: formatting rules (120-char width, crate-level import grouping).

## Runtime Data Flow

- Node writes three FIFO streams: `fills`, `order`, `diffs`.
- `fifo_listener` aligns batches by `block_number`, serves merged stream over UDS, and can optionally archive to Parquet.
- `server` consumes merged UDS payloads, updates L4 state, validates snapshots, and derives L4Lite/L2 projections.
- `websocket_server` broadcasts per-subscription updates and snapshot responses.

## Archive Model

- Archive runs as session-based workers, not a single global writer. `start_archive(...)` returns an `ArchiveHandle`;
  `ArchiveHandle.stop_archive(...)` stops one session, while `listener.stop_archive(...)` stops all sessions.
- A session can have its own `mode`, `symbols`, `output_dir`, alignment flags, handoff config, and optional auto-stop
  settings.
- `archive_height` is a span, not an absolute height:
  - actual archive start height = first block that really starts writing after alignment/recovery
  - effective stop height = `actual_start + archive_height - 1`
- `stop_height` is an absolute stop height.
- `archive_height` and `stop_height` are mutually exclusive.
- If `stop_height` is already below the first seen block height, the session is ignored and exits without starting
  archive writers.

## Archive Stop Semantics

- Normal Python-driven `stop_archive()` uses normal close/handoff semantics.
- `SIGINT`/`SIGTERM` are treated differently from explicit stop:
  - upstream FIFO/aggregator input is cut first
  - archive close runs on the signal-stop path
  - `blocks/fill` may be kept locally for recovery when `recover_blocks_fill_locally=true`
- Signal-stop close is intentionally more aggressive and parallelized to reduce shutdown time.
- Signal-stop now closes in parallel across:
  - `blocks`
  - `diff`
  - one task per coin for `status + fill`
- Handoff is concurrent as well, with up to `15` in-flight file transfers.

## Archive Recovery And Handoff

- Only `blocks` and `fill` support local recovery.
- Local recovery files use suffix `.parquet.0`.
- On restart, startup preflight scans local `blocks/fill` recovery files before actual archive writing begins.
- Continuity for `fill` is based on logical archive height, not only the last fill event row height:
  - a file can still be resumable even if the logical end block has no fill rows
- For `blocks/fill`, if continuity is broken, old local recovery files are staged to `.handoff` and sent to NAS/OSS
  using the session handoff config.
- Small-file discard rule applies before local recovery retention:
  - finalized archive span `< 5000` blocks => drop locally, do not hand off, do not retain for recovery

## Archive Handoff Configuration

- Handoff destination is session-scoped and should come from `start_archive(...)` parameters:
  - `move_to_nas`
  - `nas_output_dir`
  - `upload_to_oss`
  - `oss_*`
- If `nas_output_dir` is omitted, the default NAS finalize root is `/mnt`.
- Do not assume `/mnt` is always correct; if tests or ops require another path such as `/mnt/test`, pass it explicitly
  into `start_archive(...)`.
- Stop-time handoff must use the session handoff config, not a global fallback.

## Build, Test, and Development Commands

- `cargo build --workspace`: build all crates.
- `cargo b`: shorthand rebuild used in this repo workflow.
- `cargo run --release --bin websocket_server`: run the WebSocket server (defaults to `0.0.0.0:8443`).
- `RUST_LOG=info cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000`: override address/port and enable logging.
- `cargo run -p fifo_listener`: run the FIFO listener with alignment metrics.
- `cargo run --bin example_client`: run the sample client.
- `cargo fmt --all`: format code using `rustfmt.toml`.
- `cargo clippy --workspace`: run workspace lint checks.
- `cargo test --workspace`: run unit tests embedded in modules.

## Coding Style & Naming Conventions

- Rust standard style: 4-space indentation, `snake_case` for modules/functions, `UpperCamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep lines at or below 120 characters per `rustfmt.toml`.
- Prefer explicit error handling; clippy lints are enabled at warn level in the workspace.

## Testing Guidelines

- Tests are inline `#[cfg(test)]` modules under `server/src/`.
- Run all tests with `cargo test --workspace`.
- Add unit tests alongside the module they cover; name test functions descriptively.

## Commit & Pull Request Guidelines

- Recent history uses short, lowercase, imperative messages (e.g., "fix known issues").
- PRs should describe behavior changes, include repro steps, and note any config changes.
- If you touch order book logic or listeners, include test updates or explain why tests are unnecessary.
- Keep commits scoped: avoid mixing protocol/schema changes with refactors in one commit.
- For dirty worktrees, stage only intended files (`git add <path>`) and confirm with `git status --short`.

## Security & Configuration Tips

- The node writes FIFOs at `/home/aimee/hl_runtime/hl_book/runtime_fifo/{fills,order,diffs}`; `fifo_listener` must be running to expose `/home/aimee/hl_runtime/hl_book/fifo_listener.sock`.
- Snapshot requests are written to `/home/aimee/hl_runtime/hl_book/snapshot.json`.
- The process exits if inputs stop arriving or snapshots diverge; treat this as a consistency check, not a crash.

## Operational Guardrails

- Do not silently change websocket response field names; treat them as client-facing protocol.
- Prefer returning recoverable errors over panics in streaming/state-sync paths.
- Keep snapshot validation and replay paths deterministic; avoid introducing order-dependent hash behavior.
- When enabling archive writes, monitor disk growth and rotation behavior under restarts.
- Treat archive row-group policy as stream-specific:
  - `status`: coin-specific row groups
    - `BTC`: `1000`
    - `ETH`: `2000`
    - `SOL/HYPE`: `5000`
    - default fallback: `10000`
  - `diff`: `50000` blocks per row group
  - `fill`: `250000` blocks per row group
  - `blocks`: `250000` blocks per row group
- Treat archive file rotation as stream-specific:
  - `status`/`diff`: use `rotation_blocks`
  - `blocks`/`fill`: fixed `1000000`-block windows
- Respect archive alignment flags:
  - `align_start_to_10k_boundary=true` means archive can wait until the next `...00001` boundary before starting
  - `align_output_to_1000_boundary=true` means finalized filenames/end heights are snapped to the most recent `1000`
    boundary
- Keep the small-file discard rule aligned with ops expectations: finalized archive files spanning fewer than `5000`
  blocks are dropped instead of handed off to NAS/OSS.
- Treat `l4Anal` payload shape as protocol: `window_sum_bid/window_sum_ask` are positional arrays with 8 values in order:
  `[fill_sz, fill_notional, change_sz, change_notional, add_sz, add_notional, remove_sz, remove_notional]`.
- `l4Anal` rollup cadence/semantics: emitted on heights divisible by 10, representing only the most recent 10-block
  window (not a long rolling accumulation).
- If you need to change `l4Anal` array length/order/semantics, coordinate downstream consumers first and document the
  migration in `README.md`.

## Pre-Commit Checklist

- `cargo fmt --all`
- `cargo b` (or `cargo build --workspace`)
- `cargo check --workspace`
- `cargo test --workspace` (or document why it is skipped/failing)
