-- models/silver/btc_prices.sql

{{ config(
    materialized='table'
) }}

SELECT
    (payload->>'symbol')::varchar as ticker,
    (payload->>'price')::decimal as price,
    (payload->>'volume')::decimal as volume_24h,
    (payload->>'timestamp')::timestamp as price_timestamp,
    ingested_at as discovered_at
FROM {{ source('bronze', 'btc_prices')}}