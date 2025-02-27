WITH fhv_trips AS (
    SELECT
        pickup_location_id,
        dropoff_location_id,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    FROM {{ ref('dim_fhv_trips') }}
    WHERE pickup_location_id IS NOT NULL AND dropoff_location_id IS NOT NULL
),

trip_p90 AS (
    SELECT
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        PERCENTILE_CONT(trip_duration, 0.90) OVER (
            PARTITION BY year, month, pickup_location_id, dropoff_location_id
        ) AS p90_trip_duration
    FROM fhv_trips
)

SELECT
    t.year,
    t.month,
    z1.zone AS pickup_zone,
    z2.zone AS dropoff_zone,
    t.p90_trip_duration,
    DENSE_RANK() OVER (PARTITION BY t.year, t.month, z1.zone ORDER BY t.p90_trip_duration DESC) AS rank
FROM trip_p90 t
JOIN {{ ref('dim_zones') }} z1 ON t.pickup_location_id = z1.locationid
JOIN {{ ref('dim_zones') }} z2 ON t.dropoff_location_id = z2.locationid
WHERE t.year = 2019 AND t.month = 11
    AND z1.zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
QUALIFY DENSE_RANK() OVER (
    PARTITION BY t.year, t.month, z1.zone ORDER BY t.p90_trip_duration DESC
) = 2

