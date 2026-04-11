use clap as _;
use std::path::PathBuf;

use compute_l4::{ComputeOptions, append_l4_checkpoint};
use libc as _;
use pyo3 as _;
use rmp_serde as _;
use rusqlite as _;
use serde as _;
use serde_json as _;
use simd_json as _;
use zstd as _;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let input = PathBuf::from(args.next().ok_or("missing input path")?);
    let output_dir_arg = args.next().ok_or("missing output_dir argument")?;
    let output_dir = (!output_dir_arg.is_empty()).then_some(PathBuf::from(output_dir_arg));
    let include_users = args.next().ok_or("missing include_users flag")? == "1";
    let include_trigger_orders = args.next().ok_or("missing include_trigger_orders flag")? == "1";
    let assets: Vec<String> = args.collect();
    let options =
        ComputeOptions { include_users, include_trigger_orders, assets: (!assets.is_empty()).then_some(assets) };
    let result = append_l4_checkpoint(input, output_dir, &options)?;
    let json = serde_json::json!({
        "block_height": result.block_height,
        "block_time": result.block_time,
        "segment_path": result.segment_path.to_string_lossy(),
        "segment_start": result.segment_start,
        "segment_end": result.segment_end,
        "snapshot_index_path": result.snapshot_index_path.to_string_lossy(),
        "checkpoint_count": result.checkpoint_count,
        "asset_count": result.asset_count,
        "codec": result.codec,
        "compression_level": result.compression_level,
        "raw_len": result.raw_len,
        "stored_len": result.stored_len,
    });
    println!("{}", serde_json::to_string(&json)?);
    Ok(())
}
