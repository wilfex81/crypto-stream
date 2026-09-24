-- Gold grain: one row per trade per symbol (same grain as silver, 
-- gold just adds the date partition and joins the dimensions).
-- Incremental + partitioned by trade_date: each run only processes
-- new trades since the last run, matching how the pipeline actually
-- flows (new files land in the lake, Auto Loader picks them ip,
-- dbt picks up whatever's new in silver). 


{{
    config(
        materliazed='incremental',
        unique_key=['symbol', 'trade_id'],
        partition_by=['trade_date'],
        file_format='delta'
    )
}}


select 
    t.symbol,
    t.trade_id,
    t.price,
    t.quantity,
    t.trade_timestamp,
    date(t.trade_timestamp) as trade_date,
    t.buyer_is_maker

from {{ ref('stg_trades') }} t

{% if is_incremental() %}
where t.trade_timestamp > (select max(trade_timestamp) from {{ this }})
{% endif %}