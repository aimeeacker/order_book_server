use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use arrow_array as _;
use arrow_schema as _;
use binance_book::{
    MySqlConfig, RecorderConfig, default_symbol_wall_notional_usd, parse_symbol_wall_notional_usd, run_recorder,
};
use chrono as _;
use clap::Parser;
use futures_util as _;
use log as _;
use mysql as _;
use parquet as _;
use pyo3 as _;
use rustls as _;
use serde as _;
use serde_json as _;
use tokio_tungstenite as _;

#[derive(Debug, Parser)]
#[command(about = "Record Binance USDT-M Futures bookTicker and minute-close depth analytics to Parquet")]
struct Args {
    #[arg(long, default_value = "/home/aimee/hl_runtime/binance_book")]
    output_dir: PathBuf,

    #[arg(long, value_delimiter = ',', default_value = "BTCUSDT,ETHUSDT")]
    symbols: Vec<String>,

    #[arg(long, default_value_t = 100)]
    depth_interval_ms: u64,

    #[arg(long, default_value_t = 8)]
    decimal_scale: i8,

    #[arg(long, default_value_t = 250_000)]
    bookticker_row_group_rows: usize,

    #[arg(long, default_value_t = 10_000)]
    depth_row_group_rows: usize,

    #[arg(long, default_value_t = 10_000_000.0)]
    wall_notional_usd: f64,

    #[arg(long, value_delimiter = ',', default_value = "BTCUSDT=10000000,ETHUSDT=5000000")]
    symbol_wall_notional_usd: Vec<String>,

    #[arg(long, default_value_t = 50.0)]
    search_bps: f64,

    #[arg(long, default_value_t = 1_000)]
    reconnect_delay_ms: u64,

    #[arg(long, default_value_t = true)]
    mysql_enabled: bool,

    #[arg(long, default_value = "172.22.0.198")]
    mysql_host: String,

    #[arg(long, default_value_t = 3306)]
    mysql_port: u16,

    #[arg(long, default_value = "aimee")]
    mysql_user: String,

    #[arg(long, default_value = "02011")]
    mysql_password: String,

    #[arg(long, default_value = "eventContract")]
    mysql_db: String,

    #[arg(long, default_value = "binance_depth_minute")]
    mysql_table: String,

    #[arg(long)]
    duration_seconds: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_shutdown.store(true, Ordering::SeqCst);
        }
    });

    if let Some(duration_seconds) = args.duration_seconds {
        let timer_shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(duration_seconds)).await;
            timer_shutdown.store(true, Ordering::SeqCst);
        });
    }

    let config = RecorderConfig {
        output_dir: args.output_dir,
        symbols: args.symbols,
        depth_interval_ms: args.depth_interval_ms,
        decimal_scale: args.decimal_scale,
        bookticker_row_group_rows: args.bookticker_row_group_rows,
        depth_row_group_rows: args.depth_row_group_rows,
        wall_notional_usd: args.wall_notional_usd,
        symbol_wall_notional_usd: if args.symbol_wall_notional_usd.is_empty() {
            default_symbol_wall_notional_usd()
        } else {
            parse_symbol_wall_notional_usd(&args.symbol_wall_notional_usd)?
        },
        search_bps: args.search_bps,
        reconnect_delay_ms: args.reconnect_delay_ms,
        mysql: args.mysql_enabled.then_some(MySqlConfig {
            host: args.mysql_host,
            port: args.mysql_port,
            user: args.mysql_user,
            password: args.mysql_password,
            database: args.mysql_db,
            table: args.mysql_table,
        }),
    };
    run_recorder(config, shutdown).await
}
