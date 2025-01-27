
## Question 1
```bash
docker pull python:3.12.8
```
```bash
docker run -it --entrypoint bash python:3.12.8
```
```bash
pip --version
```
#### Answer: pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)


## Question 2
#### Answer:
#### Hostname: db; Port: 5432

## Data Ingestion
#### [upload_data.py](upload_data.py)
#### green taxi trips
```bash
python upload_data.py \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5433 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```
#### zones
```bash
python upload_data.py \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5433 \
    --db=ny_taxi \
    --table_name=taxi_zones \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

## Question 3
```bash
SELECT 
    CASE
        WHEN trip_distance <= 1 THEN 'Up to 1 mile'
        WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1 to 3 miles'
        WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3 to 7 miles'
        WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7 to 10 miles'
        ELSE 'Over 10 miles'
    END AS trip_segment,
    COUNT(*) AS trip_count
FROM green_taxi_trips
WHERE lpep_dropoff_datetime >= '2019-10-01'
  AND lpep_dropoff_datetime < '2019-11-01'
GROUP BY trip_segment;
```
#### Answer: 
![alt text](<Question3.png>)

## Question 4
```bash
SELECT 
    DATE(lpep_pickup_datetime) AS trip_date,
    MAX(trip_distance) AS max_distance
FROM green_taxi_trips
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
```
#### Answer: 2019-10-31 with 515.89 distance

## Question 5
```bash
SELECT 
	tz."Zone" AS pickup_zone,
    SUM(total_amount) AS total_amount_sum
FROM green_taxi_trips gtt
JOIN taxi_zones tz
	ON gtt."PULocationID" = tz."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY tz."Zone"
HAVING SUM(total_amount) > 13000
ORDER BY total_amount_sum DESC
LIMIT 3;
```
#### Answer: 
![alt text](<Question5.png>)

## Question 6
```bash
SELECT 
    tz_drop."Zone" AS dropoff_zone,
    MAX(gtt.tip_amount) AS largest_tip
FROM green_taxi_trips gtt
JOIN taxi_zones tz_pick
    ON gtt."PULocationID" = tz_pick."LocationID"
JOIN taxi_zones tz_drop
    ON gtt."DOLocationID" = tz_drop."LocationID"
WHERE tz_pick."Zone" = 'East Harlem North'
  AND DATE(gtt.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
GROUP BY tz_drop."Zone"
ORDER BY largest_tip DESC
LIMIT 1;
```

#### Answer: JFK Airport with the largest tip: 87.3
