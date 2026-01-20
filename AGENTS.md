# Repository Guidelines

This repository hosts a Rust workspace for a local WebSocket server that mirrors Hyperliquid order book subscriptions. It includes library code in `server/`, runnable binaries in `binaries/`, and a standalone FIFO utility in `fifo_listener/` (the server consumes the merged UDS stream).

## Project Structure & Module Organization

- `server/src/`: core library crate (listeners, order book logic, servers, types).
- `binaries/src/bin/`: runnable entry points like `websocket_server.rs` and `example_client.rs`.
- `fifo_listener/`: FIFO listener that merges the three streams and exposes them over UDS.
- `Cargo.toml`: workspace config and lint settings.
- `rustfmt.toml`: formatting rules (120-char width, crate-level import grouping).

## Build, Test, and Development Commands

- `cargo build --workspace`: build all crates.
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

## Security & Configuration Tips

- The node writes FIFOs at `/home/aimee/hl_runtime/hl_book/{fills,order,diffs}`; `fifo_listener` must be running to expose `/home/aimee/hl_runtime/hl_book/fifo_listener.sock`.
- Snapshot requests are written to `/home/aimee/hl_runtime/hl_book/snapshot.json`.
- The process exits if inputs stop arriving or snapshots diverge; treat this as a consistency check, not a crash.
