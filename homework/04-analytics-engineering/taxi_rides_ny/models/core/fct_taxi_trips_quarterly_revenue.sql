WITH trips AS (
  SELECT 
    service_type,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
    CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
    SUM(total_amount) AS quarterly_revenue
  FROM {{ ref('fact_trips') }}
  WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
  GROUP BY 1, 2, 3, 4
),

revenue_change AS (
SELECT 
    t1.service_type,
    t1.year_quarter,
    t1.quarterly_revenue,
    t2.quarterly_revenue AS last_year_revenue,
    ROUND(
        100 * (t1.quarterly_revenue - t2.quarterly_revenue) / NULLIF(t2.quarterly_revenue, 0), 2
    ) AS yoy_growth_percentage
FROM trips t1
LEFT JOIN trips t2
    ON t1.service_type = t2.service_type
    AND t1.year = t2.year + 1
    AND t1.quarter = t2.quarter
)

SELECT * FROM revenue_change
WHERE year_quarter LIKE "2020%"
ORDER BY service_type, yoy_growth_percentage DESC