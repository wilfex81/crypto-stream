# crypto-stream

Real-time crypto trade data pipeline: Binance WebSocket → Kafka → S3 (lake) → Databricks Unity Catalog (lakehouse transforms) → Snowflake (warehouse) → dashboard.

## Why this exists

A learning project built to go deep on streaming ingestion, medallion architecture, grain and cardinality design, and a real lake/lakehouse/warehouse separation.

## Architecture

```
Binance WebSocket (trade stream)
        │
        ▼
   Kafka producer ──► Kafka topic (crypto-trades-raw)
                              │
                              ▼
                       Kafka consumer
                              │
                              ▼
                    S3 landing (raw JSON)  ◄── data lake
                              │
                     Databricks Auto Loader
                              │
                              ▼
              Unity Catalog: bronze (raw, trade grain)
                              │
                            dbt
                              ▼
              Unity Catalog: silver (deduped, conformed)
                              │
                            dbt
                              ▼
      Unity Catalog: gold (dim_symbol, fact_trade, OHLC windows)
                              │
                     load job (scheduled)
                              ▼
                    Snowflake  ◄── data warehouse
                              │
                              ▼
                   Dashboard (candle chart + volume ranking)
```

## Data grain

- **Bronze**: one row per trade per symbol, as emitted by Binance.
- **Silver**: same grain, deduplicated and type-conformed.
- **Gold**: `dim_symbol` (one row per trading pair), `fact_trade` (trade grain, partitioned by trade date), plus rolling 1-minute OHLC/VWAP windows per symbol.

## Stack

- **Ingestion**: Python, `websockets`, Kafka
- **Lake**: AWS S3
- **Lakehouse / transforms**: Databricks, Unity Catalog, Delta Lake, dbt
- **Warehouse**: Snowflake
- **Orchestration**: Databricks Workflows
- **IaC**: Terraform
- **CI/CD**: GitHub Actions, Databricks Asset Bundles

## Repo structure

```
producer/        Binance WebSocket → Kafka
consumer/         Kafka → S3 landing
infra/            Terraform: S3, IAM, Databricks workspace
dbt/              dbt project: staging (silver), marts (gold)
warehouse/        Snowflake load scripts
orchestration/    Databricks Workflow definitions
.github/workflows/ CI/CD pipelines
docker-compose.yml  Local Kafka for development
```

## Status

Early build. See project checklist for current phase.

## Note on data

Live trade data is pulled from Binance's public market data WebSocket streams. No API key or authentication required for this data; used here for personal, non-commercial, educational purposes.
