# Databricks notebook: Auto Loader ingestion into bronze
#
# Reads newly-arrived JSON files from the S3 lake landing zone and
# incrementally loads them into crypto_streaming.bronze.trades.
# Bronze grain: one row per trade per symbol, exactly as it arrived,
# no cleaning or dedup here (that's the silver layer's job in dbt).

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType
)

# Explicit schema, not inferred. Binance sends price/quantity as strings,
# keep them as strings in bronze (raw = raw), cast to decimal in silver.
trade_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("trade_id", LongType(), False),
    StructField("price", StringType(), False),
    StructField("quantity", StringType(), False),
    StructField("trade_time_ms", LongType(), False),
    StructField("buyer_is_maker", BooleanType(), True),
])

RAW_PATH = "s3://crypto-stream-lakehouse/raw/"
CHECKPOINT_PATH = "s3://crypto-stream-lakehouse/checkpoints/bronze_trades/"
SCHEMA_LOCATION = "s3://crypto-stream-lakehouse/schemas/bronze_trades/"

TARGET_TABLE = "crypto_streaming.bronze.trades"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .schema(trade_schema)
    .load(RAW_PATH)
)

# trigger="availableNow" processes everything currently sitting in the
# lake, then stops. Run this on a Databricks Workflow schedule (e.g.
# every 5 minutes) rather than leaving it running continuously — cheaper,
# and Auto Loader tracks what it's already ingested via the checkpoint,
# so nothing gets missed or double-counted between runs.
(
    df.writeStream
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)