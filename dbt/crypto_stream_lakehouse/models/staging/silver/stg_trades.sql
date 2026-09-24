-- Silver grain: same as bronze (one row per trade per symbol), but
-- deduplicated on trade_id and type-conformed. Dedup matters here
-- specifically because parial files against different resting orders
-- can share identical price + milliseconds timestamp - trade_id is the
-- only reliable uniqueness key (see producer.py comments for why)

with deduped as (

    select 
        *,
        row_number() over (
            partition by symbol, trade_id
            order by trade_time_ms
        ) as rn

    from {{ source('bronze', 'trades')}}

)


select 
    symbol,
    trade_id,
    cast(price as decimal(20, 8))       as price,
    cast(quantity as decimal(20, 8))    as quantity,
    timestamp_millis(trade_time_ms)     as trade_timestamp,
    buyer_is_maker

from deduped
where rn = 1