"""
Reads gold and silver tables from Databricks Unity Catalog and 
loads them into Snowflake

"""

import os
import sys
from datetime import datetime, timezone

import pandas as pd
import snowflake.connector
from databricks import sql
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

 
SNOWFLAKE_DATABASE = "CRYPTO_STREAMING"
SNOWFLAKE_SCHEMA = "GOLD"

CREDENTIAL_ORDER = [
    "snowflake_account",
    "snowflake_user",
    "snowflake_password",
    "snowflake_role",
    "databricks_host",
    "databricks_http_path",
    "databricks_token",
]

def get_credentials() -> dict:
    """
    Positional CLI args (serverless job) take priority; falls back
    to env vars so this still runs locally without any job wrapper.
    """
    if len(sys.argv) - 1 >= len(CREDENTIAL_ORDER):
        return dict(zip(CREDENTIAL_ORDER, sys.argv[1:]))
 
    return {
        "snowflake_account": os.environ["SNOWFLAKE_ACCOUNT"],
        "snowflake_user": os.environ["SNOWFLAKE_USER"],
        "snowflake_password": os.environ["SNOWFLAKE_PASSWORD"],
        "snowflake_role": os.environ["SNOWFLAKE_ROLE"],
        "databricks_host": os.environ["DATABRICKS_HOST"],
        "databricks_http_path": os.environ["DATABRICKS_HTTP_PATH"],
        "databricks_token": os.environ["DATABRICKS_TOKEN"],
    }
 
# Per-table load config. dim_symbol is small and static: full refresh
# every run. The fact tables are incremental, same idea as the dbt
# incremental models: only pull rows newer than what's already loaded.
TABLES = {
    "DIM_SYMBOL": {
        "source": "crypto_streaming.silver.dim_symbol",
        "mode": "full_refresh",
    },
    "FCT_TRADE": {
        "source": "crypto_streaming.gold.fct_trade",
        "mode": "incremental",
        "watermark_column": "trade_timestamp",
    },
    "FCT_OHLC_1M": {
        "source": "crypto_streaming.gold.fct_ohlc_1m",
        "mode": "incremental",
        "watermark_column": "window_start",
    },
}
 
 
def get_databricks_connection(creds: dict):
    return sql.connect(
        server_hostname=creds["databricks_host"],
        http_path=creds["databricks_http_path"],
        access_token=creds["databricks_token"],
    )
 
 
def get_snowflake_connection(creds: dict):
    return snowflake.connector.connect(
        account=creds["snowflake_account"],
        user=creds["snowflake_user"],
        password=creds["snowflake_password"],
        role=creds["snowflake_role"],
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
 
 
def watermark_to_iso(value) -> str:
    """
    Snowflake's connector sometimes returns TIMESTAMP columns as a
    raw nanosecond-epoch integer instead of a datetime object (an
    Arrow-fetch quirk), depending on session/environment. Normalize
    either shape to an ISO string.
    """
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1e9, tz=timezone.utc).isoformat()
    return value.isoformat()
 
 
def get_watermark(sf_conn, target_table: str, watermark_column: str):
    """
    Max value already loaded into Snowflake for this table, or None
    if the table doesn't exist yet (first run).
    """
    cur = sf_conn.cursor()
    try:
        cur.execute(f"SELECT MAX({watermark_column}) FROM {target_table}")
        row = cur.fetchone()
        return row[0] if row else None
    except snowflake.connector.errors.ProgrammingError:
        return None  # table doesn't exist yet
    finally:
        cur.close()
 
 
def fetch_dataframe(db_conn, query: str, params: dict | None = None) -> pd.DataFrame:
    cur = db_conn.cursor()
    if params:
        cur.execute(query, params)
    else:
        cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=columns)
 
 
def load_table(db_conn, sf_conn, target_table: str, config: dict):
    source = config["source"]
 
    if config["mode"] == "full_refresh":
        query = f"SELECT * FROM {source}"
        df = fetch_dataframe(db_conn, query)
    else:
        watermark_col = config["watermark_column"]
        watermark = get_watermark(sf_conn, target_table, watermark_col)
        if watermark is None:
            query = f"SELECT * FROM {source}"
            df = fetch_dataframe(db_conn, query)
        else:
            # Bind as a string and cast explicitly in SQL- the driver's
            # automatic parameter typing turns a raw Python datetime into
            # a BIGINT epoch, which then can't compare against a real
            # TIMESTAMP column.
            query = (
                f"SELECT * FROM {source} "
                f"WHERE {watermark_col} > CAST(%(watermark)s AS TIMESTAMP)"
            )
            df = fetch_dataframe(db_conn, query, {"watermark": watermark_to_iso(watermark)})
    df.columns = [c.upper() for c in df.columns]  # Snowflake convention
 
    if df.empty:
        print(f"{target_table}: no new rows.")
        return
 
    success, _, nrows, _ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name=target_table,
        auto_create_table=True,
        overwrite=(config["mode"] == "full_refresh"),
        use_logical_type=True,
    )
    print(f"{target_table}: loaded {nrows} rows (success={success})")
 
 
def main():
    creds = get_credentials()
    db_conn = get_databricks_connection(creds)
    sf_conn = get_snowflake_connection(creds)
 
    try:
        for target_table, config in TABLES.items():
            load_table(db_conn, sf_conn, target_table, config)
    finally:
        db_conn.close()
        sf_conn.close()
 
 
if __name__ == "__main__":
    main()