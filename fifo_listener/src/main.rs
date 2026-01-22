#![allow(unused_crate_dependencies)]

fn main() {
    fifo_listener::init_cli_logging();
    fifo_listener::run_forever();
}
