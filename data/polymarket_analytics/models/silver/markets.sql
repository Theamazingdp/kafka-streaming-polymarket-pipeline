-- models/silver/markets.sql

{{ config(
    materialized='table'
) }}

SELECT
    (payload->>'market_id')::varchar as market_id,
    (payload->>'condition_id')::varchar as condition_id,
    (payload->>'question')::varchar as question,
    (payload->>'slug')::varchar as slug,
    (payload->>'start_time')::timestamp as start_time,
    (payload->>'end_time')::timestamp as end_time,
    (payload->>'yes_price')::decimal as yes_price_at_discovery,
    (payload->>'no_price')::decimal as no_price_at_discovery,
    (payload->>'liquidity')::decimal as liquidity,
    (payload->>'volume')::decimal as volume,
    (payload->>'active')::boolean as active,
    ingested_at as discovered_at
FROM {{ source('bronze', 'market_updates')}}