{{ config(materialized='view') }}
SELECT
    TRIP_ID,
    pickup_at,
    pickup_area,
    dropoff_area,
    -- time features
    HOUR(pickup_at)                                     AS pickup_hour,
    DAYOFWEEK(pickup_at)                                AS pickup_dow,
    MONTH(pickup_at)                                    AS pickup_month,
    CASE WHEN DAYOFWEEK(pickup_at) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN HOUR(pickup_at) IN (22,23,0,1,2,3) THEN 1 ELSE 0 END AS is_late_night,
    -- trip features
    trip_miles,
    trip_seconds / 60.0                                 AS trip_minutes,
    CASE WHEN trip_miles > 0 THEN fare / trip_miles ELSE NULL END AS fare_per_mile,
    CASE WHEN trip_seconds > 0 THEN fare / (trip_seconds/60.0) ELSE NULL END AS fare_per_minute,
    extras,
    fare,
    -- geographic: O'Hare=76, Midway=56
    CASE WHEN pickup_area  IN ('76','56') THEN 1 ELSE 0 END AS pickup_is_airport,
    CASE WHEN dropoff_area IN ('76','56') THEN 1 ELSE 0 END AS dropoff_is_airport,
    -- target
    CASE WHEN tips > 0 THEN 1 ELSE 0 END                AS tipped
FROM {{ ref('stg_chicago_trips') }}
WHERE fare_per_mile IS NOT NULL AND fare_per_mile BETWEEN 0.5 AND 50
  AND trip_seconds / 60.0 > 0.5
