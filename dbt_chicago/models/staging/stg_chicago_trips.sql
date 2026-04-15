{{ config(materialized='view') }}
SELECT
    TRIP_ID,
    TRIP_START_TIMESTAMP            AS pickup_at,
    TRIP_END_TIMESTAMP              AS dropoff_at,
    TRIP_SECONDS                    AS trip_seconds,
    TRIP_MILES                      AS trip_miles,
    PICKUP_COMMUNITY_AREA           AS pickup_area,
    DROPOFF_COMMUNITY_AREA          AS dropoff_area,
    FARE, TIPS, EXTRAS, TRIP_TOTAL,
    PAYMENT_TYPE,
    PICKUP_CENTROID_LATITUDE        AS pickup_lat,
    PICKUP_CENTROID_LONGITUDE       AS pickup_lng,
    DROPOFF_CENTROID_LATITUDE       AS dropoff_lat,
    DROPOFF_CENTROID_LONGITUDE      AS dropoff_lng
FROM {{ source('raw','chicago_trips') }}
WHERE FARE > 0
  AND TRIP_SECONDS > 0
  AND TRIP_MILES   >= 0
  AND TIPS         >= 0
  AND TRIP_SECONDS < 7200
  AND PAYMENT_TYPE = 'Credit Card'
