#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod listeners;
mod order_book;
mod prelude;
mod servers;
mod types;

pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

pub const HL_NODE: &str = "hl-node";

pub async fn parse_snapshot_for_bench(path: &std::path::Path, allowed_coins: &[&str]) -> Result<()> {
    let _unused = order_book::multi_book::load_snapshots_from_json_filtered::<
        types::inner::InnerL4Order,
        (alloy::primitives::Address, types::SnapshotOrder),
    >(path, allowed_coins)
    .await?;
    Ok(())
}
