import json
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
from kafka import KafkaConsumer

import os
from dotenv import load_dotenv


load_dotenv()


KAFKA_BOOSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "crypto-trades-raw"

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = "raw"

BATCH_SIZE = 500            #flush after this many messages
BATCH_MAX_SECONDS = 30      # or after this many seconds, whichever comes first

s3 = boto3.client("s3")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="s3_lake_writer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def write_batch(batch: list[dict]) -> None:
    if not batch:
        return

    now = datetime.now(timezone.utc)
    #Partition by date  matches the "partition by trade date" decision
    # made for the gold layer later, keeping it consistent from the lake up.
    key = (
        f"{S3_PREFIX}/dt={now.strftime('%Y-%m-%d')}/"
        f"batch_{now.strftime('%H%M%S')}_{now.microsecond}.json"
    )

    body = "\n".join(json.dumps(record) for record in batch).encode("utf-8")
    s3.upload_fileobj(BytesIO(body), S3_BUCKET, key)
    print(f"Wrote {len(batch)} records to s3://{S3_BUCKET}/{key}")

def run():
    batch = []
    last_flush = time.time()

    print(f"Listening on '{KAFKA_TOPIC}', writing to s3://{S3_BUCKET}/{S3_PREFIX}/")

    for message in consumer:
        batch.append(message.value)

        should_flush = (
            len(batch) >= BATCH_SIZE
            or (time.time() - last_flush) >= BATCH_MAX_SECONDS
        )

        if should_flush:
            write_batch(batch)
            batch = []
            last_flush = time.time()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Stopped.")