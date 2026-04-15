{{ config(materialized='table', schema='MARTS') }}
SELECT
    TRIP_ID,
    pickup_hour, pickup_dow, pickup_month,
    is_weekend, is_late_night,
    trip_miles, trip_minutes,
    fare_per_mile, fare_per_minute,
    extras, fare,
    pickup_is_airport, dropoff_is_airport,
    tipped
FROM {{ ref('int_trip_features') }}
WHERE fare_per_mile IS NOT NULL
  AND fare_per_minute IS NOT NULL
  AND trip_miles IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIP_ID ORDER BY pickup_hour DESC) = 1

