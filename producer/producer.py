import asyncio
import json

import websockets
from kafka import KafkaProducer

SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]  # List of symbols to subscribe to


STREAM_NAMES = "/".join(f"{s}@trade" for s in SYMBOLS)
BINANCE_URL = f"wss://stream.binance.com:9443/stream?streams={STREAM_NAMES}"

KAFKA_BOOSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-trades-raw"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v:json.dumps(v).encode("utf-8"),
)

def to_kafka_message(trade: dict) -> dict:
    """ Map Binance's cryptic single-letter fields to explicit names.
    Keeping the trae ID('t') is important: multiple trades can share 
    the same price and millisecond timestamp (partial fills against 
    different resting orders), so the trade ID is the only reliable
    dedup key downstream.
    """

    return {
        "symbol": trade["s"],
        "trade_id": trade["t"],
        "price": trade["p"],
        "quantity": trade["q"],
        "trade_time_ms": trade["T"],
        "buyer_is_maker": trade["m"]
    }

async def consume():
    while True:
        try:
            async with websockets.connect(BINANCE_URL, ping_interval=None) as ws:
                print(f"Connected. Publishing to '{KAFKA_TOPIC}' for: {SYMBOLS}")
                async for raw_message in ws:
                    message = json.loads(raw_message)
                    #Combined stream wraps the actual trade under "data"
                    trade = message["data"]
                    kafka_message = to_kafka_message(trade)

                    producer.send(
                        KAFKA_TOPIC,
                        key=kafka_message['symbol'],
                        value=kafka_message,
                    )
                    print(f"Published: {kafka_message}")

        except websockets.ConnectionClosed as e:
            print(f"Connection closed ({e}). Reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(consume())
            