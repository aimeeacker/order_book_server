#![allow(unused_crate_dependencies)]

use std::{path::PathBuf, time::Instant};

const MAIN_COINS: [&str; 2] = ["BTC", "ETH"];
const SNAPSHOT_PATH: &str = "/home/aimee/hl_runtime/hl_book/snapshot.json";
const RUNS: usize = 5;

#[tokio::main]
async fn main() -> server::Result<()> {
    let path = PathBuf::from(SNAPSHOT_PATH);
    let mut samples = Vec::with_capacity(RUNS);

    for idx in 0..RUNS {
        let start = Instant::now();
        server::parse_snapshot_for_bench(&path, &MAIN_COINS).await?;
        let parse_ms = start.elapsed().as_secs_f64() * 1000.0;
        println!("Snapshot parse completed in {:.3} ms (run {}/{})", parse_ms, idx + 1, RUNS);
        samples.push(parse_ms);
    }

    let avg = samples.iter().sum::<f64>() / samples.len() as f64;
    let min = samples.iter().copied().fold(f64::INFINITY, f64::min);
    let max = samples.iter().copied().fold(f64::NEG_INFINITY, f64::max);

    println!("Snapshot parse average over {} runs: {:.3} ms (min {:.3}, max {:.3})", RUNS, avg, min, max);

    Ok(())
}
