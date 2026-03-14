# Example usage:
# 1) Build the module:
#    cargo build -p fifo_listener --release
# 2) Make it importable (pick one):
#    - export PYTHONPATH=target/release
#    - OR: ln -sf ~/order_book_server/target/release/libfifo_listener.so ~/hl_runtime/fifo_listener.so
# 3) Run:
#    python fifo_listener/python_example.py
#echo "/home/aimee/trading_packages" \
#  > ~/hl_runtime/lib/python3.12/site-packages/custom.pth


import asyncio

import fifo_listener


async def main():
    listener = fifo_listener.FifoListener()

    async def on_height(height: int) -> None:
        print(f"height={height}")

    loop = asyncio.get_running_loop()
    listener.start(on_height, event_loop=loop)

    print("fifo_listener running for 5 seconds with FULL archive...")
    listener.start_write_dataset("FULL")
    await asyncio.sleep(5)

    listener.stop()
    print("fifo_listener stopped")


if __name__ == "__main__":
    asyncio.run(main())
