use std::path::PathBuf;

use clap::Parser;
use compute_l4::{AppError, ComputeOptions, compute_l4_to_file};
use pyo3 as _;
use rmp_serde as _;
use serde as _;
use serde_json as _;
use simd_json as _;
use zstd as _;

#[derive(Parser, Debug)]
#[command(name = "compute_l4")]
#[command(about = "Prototype decoder for hl-node compute-l4-snapshots output")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    include_users: bool,
    #[arg(long)]
    include_trigger_orders: bool,
    #[arg(long = "asset")]
    assets: Vec<String>,
}

fn main() -> Result<(), AppError> {
    let args = Args::parse();
    let options = ComputeOptions {
        include_users: args.include_users,
        include_trigger_orders: args.include_trigger_orders,
        assets: (!args.assets.is_empty()).then_some(args.assets),
    };
    compute_l4_to_file(args.input, args.output, &options)
}
