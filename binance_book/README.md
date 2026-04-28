# Binance Book Recorder

Records Binance USDT-M Futures public book streams for BTCUSDT/ETHUSDT by default.

## Streams

- `bookTicker`: every best bid/ask update is written to one Parquet file per symbol.
- `depth@100ms`: the process maintains an in-memory book from received diff updates only. No REST snapshot is loaded.
  Each symbol keeps the last analysis row inside the current minute. When all configured symbols have closed the same
  minute, one wide depth row is written with symbol-specific fields.

The recorder uses Binance combined public streams:

```text
wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms/ethusdt@bookTicker/ethusdt@depth@100ms
```

## Row Groups

- `bookTicker`: `250000` rows per row group. This keeps high-frequency scans efficient without holding large batches in
  memory.
- `depth_minute`: `10000` rows per row group. Minute rows are sparse, so larger row groups improve compression and scan
  locality.

Both files use ZSTD level 3. Price columns use `decimal(8,2)`, size columns use `decimal(8,3)`, notional columns use
`decimal(18,2)`, and bookTicker imbalance uses `decimal(4,3)`.

## Rotation

Parquet output rotates every 8 hours on UTC boundaries: `00:00`, `08:00`, and `16:00`. File names include the UTC window
start and inclusive end timestamps, for example:

```text
binance_usdm_btcusdt_bookticker_20260428T080000Z_20260428T155959Z.parquet
binance_usdm_ethusdt_bookticker_20260428T080000Z_20260428T155959Z.parquet
binance_usdm_depth_minute_20260428T080000Z_20260428T155959Z.parquet
```

## Schemas

`binance_usdm_<symbol>_bookticker_*.parquet`:

- `transaction_time`
- `symbol`
- `bid_px`, `bid_sz`, `ask_px`, `ask_sz`
- `bid_notional`, `ask_notional`, `imbalance`

`binance_usdm_depth_minute_*.parquet`:

- common minute field: `minute_close_time`
- one field set per configured symbol, using suffixes derived from symbols (`BTCUSDT -> _btc`, `ETHUSDT -> _eth`)
- per-symbol best quotes: `best_bid_px_btc`, `best_bid_sz_btc`, `best_ask_px_btc`, `best_ask_sz_btc`
- per-symbol support/resistance wall: `support_px_btc`, `support_sz_btc`, `support_notional_btc`,
  `resistance_px_btc`, `resistance_sz_btc`, `resistance_notional_btc`

Support/resistance is selected by scanning outward from the latest bookTicker mid price inside +/- `50` bps (`0.5%`) and
choosing the nearest observed level whose notional reaches the symbol threshold. Defaults are BTCUSDT `10_000_000` USD
and ETHUSDT `5_000_000` USD, with `wall_notional_usd` as the fallback for other symbols. If no level qualifies, the wall
columns are null.

Because snapshot initialization is intentionally disabled, the observed book starts empty and warms up from live deltas.
Early rows can miss resting levels that did not update after process start.

## CLI

```bash
cargo run --release -p binance_book -- \
  --output-dir /home/aimee/hl_runtime/binance_book \
  --symbols BTCUSDT,ETHUSDT \
  --depth-interval-ms 100 \
  --wall-notional-usd 10000000 \
  --symbol-wall-notional-usd BTCUSDT=10000000,ETHUSDT=5000000 \
  --search-bps 50 \
  --mysql-table binance_depth_minute
```

## Python

Build the `binance_book` Python extension with the same PyO3/maturin workflow used by the existing Python-bound crates,
then start and stop it from Python:

```python
import binance_book

recorder = binance_book.start_recorder(
    output_dir="/home/aimee/hl_runtime/binance_book",
    symbols=["BTCUSDT", "ETHUSDT"],
    symbol_wall_notional_usd={"BTCUSDT": 10_000_000.0, "ETHUSDT": 5_000_000.0},
    mysql_enabled=True,
    mysql_host="172.22.0.198",
    mysql_port=3306,
    mysql_user="aimee",
    mysql_password="02011",
    mysql_db="eventContract",
    mysql_table="binance_depth_minute",
)

# later
recorder.stop()
```

## MySQL

Depth minute rows are also written to MySQL on minute close. The recorder expects `binance_depth_minute` to already
exist and upserts by `ts` using UTC `DATETIME(3)`. It does not create or migrate MySQL tables. MySQL write failures are
logged and do not stop Parquet capture. MySQL column names are intentionally compact:

- common: `ts`, `c_at`, `u_at`
- per symbol: `bb_px_*`, `bb_sz_*`, `ba_px_*`, `ba_sz_*`
- support/resistance: `sup_px_*`, `sup_sz_*`, `sup_not_*`, `res_px_*`, `res_sz_*`, `res_not_*`
