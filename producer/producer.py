import asyncio
import json

import websockets

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]  # List of symbols to subscribe to


STREAM_NAMES = "/".join(f"{s}@trade" for s in SYMBOLS)
URL = f"wss://stream.binance.com:9443/stream?streams={STREAM_NAMES}"


async def consume():
    while True:
        try:
            async with websockets.connect(URL, ping_interval=None) as ws:
                print(f"Connected. Subscribed to {SYMBOLS}")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    #Combined stream wraps the actual trade under "data"
                    trade = message["data"]

                    symbol = trade["s"]
                    price = trade["p"]
                    quantity = trade["q"]
                    trade_time_ms = trade["T"]
                    is_buyer_maker = trade["m"]

                    print(
                        f"{symbol:8s} price={price:>12s} qty={quantity:>10s} "
                        f"time={trade_time_ms} buyer_maker={is_buyer_maker}"
                    )
        except websockets.ConnectionClosed as e:
            print(f"Connection closed ({e}). Reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(consume())
            