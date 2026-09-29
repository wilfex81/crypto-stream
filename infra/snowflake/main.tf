terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.94"
    }
  }
}

# Configure via environment variables:
#   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE
provider "snowflake" {}

resource "snowflake_database" "crypto_streaming" {
  name    = "CRYPTO_STREAMING"
  comment = "Warehouse layer for the crypto streaming lakehouse project"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.crypto_streaming.name
  name     = "GOLD"
  comment  = "Loaded from Databricks Unity Catalog gold tables"
}
