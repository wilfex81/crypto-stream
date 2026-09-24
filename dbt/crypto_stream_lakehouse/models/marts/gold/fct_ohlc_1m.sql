-- Gold grain: one row per symbol per 1-minute window.
-- This is a genuinely different grain from fct_trade (trade-level) —
-- documenting that distinction explicitly is the point of this model,
-- not just a formality.

{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'window_start'],
        partition_by=['trade_date'],
        file_format='delta'
    )
}}

with bucketed as (

    select
        symbol,
        date_trunc('minute', trade_timestamp) as window_start,
        date(trade_timestamp)                 as trade_date,
        price,
        quantity,
        trade_timestamp

    from {{ ref('fct_trade') }}

    {% if is_incremental() %}
    where trade_timestamp > (select coalesce(max(window_start), '1970-01-01') from {{ this }})
    {% endif %}

),

ordered as (

    select
        *,
        first_value(price) over (
            partition by symbol, window_start
            order by trade_timestamp
            rows between unbounded preceding and unbounded following
        ) as open_price,
        last_value(price) over (
            partition by symbol, window_start
            order by trade_timestamp
            rows between unbounded preceding and unbounded following
        ) as close_price

    from bucketed

)

select
    symbol,
    window_start,
    trade_date,
    max(open_price)                          as open,
    max(price)                               as high,
    min(price)                               as low,
    max(close_price)                         as close,
    sum(quantity)                            as volume,
    sum(price * quantity) / sum(quantity)    as vwap,
    count(*)                                 as trade_count

from ordered
group by symbol, window_start, trade_date